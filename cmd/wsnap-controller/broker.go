package main

import (
	"context"
	"crypto/tls"
	"errors"
	"net/http"
	"time"

	"golang.org/x/time/rate"
	"k8s.io/client-go/kubernetes"
	toolscache "k8s.io/client-go/tools/cache"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	wsnapv1 "github.com/chrisyifanjin/wsnap/api/v1alpha1"
	"github.com/chrisyifanjin/wsnap/internal/broker"
)

func newBroker(mgr manager.Manager, o options) *broker.Server {
	clientset, err := kubernetes.NewForConfig(mgr.GetConfig())
	must(err)
	return &broker.Server{
		Client: mgr.GetClient(), Reader: mgr.GetAPIReader(), Audience: o.audience,
		Auth: &broker.CachingAuthenticator{
			Next: broker.TokenReviewAuthenticator{Client: clientset, Audience: o.audience},
			TTL:  time.Minute,
			// A flood of new tokens must not overload the API server.
			Reviews: rate.NewLimiter(50, 100),
		},
		Log:                    ctrl.Log.WithName("broker"),
		AllowAutomountedTokens: o.allowAPIToken,
		Limiter:                &broker.PodLimiter{Rate: rate.Limit(o.podRate), Burst: o.podBurst},
		Hub:                    &broker.Hub{},
		Gateway:                broker.KubeGatewayAuth{Client: clientset, Reviews: rate.NewLimiter(20, 50)},
	}
}

// wakeLongPolls wakes the broker's long polls from informer events: a new
// request, or a Workspace whose writer changed. Every replica serves
// polls, so this runs without the leader lock.
func wakeLongPolls(mgr manager.Manager, hub *broker.Hub) allReplicas {
	return func(ctx context.Context) error {
		notify := func(obj any) {
			switch o := obj.(type) {
			case wsnapv1.Request:
				if co, ok := obj.(client.Object); ok {
					hub.Notify(broker.HubKey(co.GetNamespace(), o.GetWorkspaceName()))
				}
			case *wsnapv1.Workspace:
				hub.Notify(broker.HubKey(o.Namespace, o.Name))
			}
		}
		handlers := toolscache.ResourceEventHandlerFuncs{
			AddFunc:    notify,
			UpdateFunc: func(_, n any) { notify(n) },
		}
		for _, obj := range []client.Object{&wsnapv1.WorkspaceSnapshot{}, &wsnapv1.WorkspaceRestore{}, &wsnapv1.Workspace{}} {
			inf, err := mgr.GetCache().GetInformer(ctx, obj)
			if err != nil {
				return err
			}
			if _, err := inf.AddEventHandler(handlers); err != nil {
				return err
			}
		}
		<-ctx.Done()
		return nil
	}
}

// allReplicas runs fn on every replica, not only the leader.
type allReplicas func(context.Context) error

func (f allReplicas) Start(ctx context.Context) error { return f(ctx) }
func (allReplicas) NeedLeaderElection() bool          { return false }

// brokerRunnable serves the broker's HTTP API. It runs on every replica:
// agents and gateway clients reach any of them through the Service.
type brokerRunnable struct {
	addr    string
	handler http.Handler
	tls     *tls.Config
}

var _ manager.LeaderElectionRunnable = brokerRunnable{}

func (b brokerRunnable) NeedLeaderElection() bool { return false }

func (b brokerRunnable) Start(ctx context.Context) error {
	srv := &http.Server{Addr: b.addr, Handler: b.handler, TLSConfig: b.tls,
		ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 30 * time.Second, IdleTimeout: 2 * time.Minute,
		// A credential request includes an STS round trip; reports are tiny.
		WriteTimeout: time.Minute, MaxHeaderBytes: 64 << 10}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdown)
	}()
	var err error
	if b.tls != nil {
		err = srv.ListenAndServeTLS("", "")
	} else {
		err = srv.ListenAndServe()
	}
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
