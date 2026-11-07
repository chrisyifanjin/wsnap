// Command wsnap-controller runs the wsnap broker and reconcilers.
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"github.com/chrisyifanjin/wsnap/internal/version"
	"net"
	"net/http"
	"os"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	"time"

	"golang.org/x/time/rate"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	toolscache "k8s.io/client-go/tools/cache"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	ctrlwebhook "sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	wsnapv1 "github.com/chrisyifanjin/wsnap/api/v1alpha1"
	"github.com/chrisyifanjin/wsnap/internal/broker"
	"github.com/chrisyifanjin/wsnap/internal/controller"
	"github.com/chrisyifanjin/wsnap/internal/webhook"
	"github.com/chrisyifanjin/wsnap/internal/workspace"
)

func main() {
	var (
		brokerAddr  = flag.String("broker-addr", ":8080", "broker listen address")
		brokerURL   = flag.String("broker-url", "http://wsnap-broker.wsnap-system.svc:8080", "broker URL that agents use")
		audience    = flag.String("audience", "wsnap", "audience of agent service account tokens")
		agentImage  = flag.String("agent-image", "ghcr.io/chrisyifanjin/wsnap-agent:dev", "default agent image for Jobs")
		metricsAddr = flag.String("metrics-addr", ":8081", "metrics listen address")
		probeAddr   = flag.String("health-addr", ":8082", "health probe listen address")
		leaderElect = flag.Bool("leader-elect", false, "enable leader election for the reconcilers")
		tlsCert     = flag.String("tls-cert-file", "", "serve the broker over TLS with this certificate (reloaded on change)")
		tlsKey      = flag.String("tls-key-file", "", "private key for --tls-cert-file")
		allowAPITok = flag.Bool("allow-automounted-tokens", false, "accept workload Pods that mount the default API token")
		podRate     = flag.Float64("pod-rate", 1, "broker requests per second allowed per Pod")
		podBurst    = flag.Int("pod-burst", 20, "broker request burst allowed per Pod")
		agentCA     = flag.String("agent-ca-configmap", "", "ConfigMap (key ca.crt) in workload namespaces that Jobs mount to trust a TLS broker")
		webhookPort = flag.Int("webhook-port", 9443, "admission webhook port")
		webhookDir  = flag.String("webhook-cert-dir", "/tmp/wsnap-webhook", "directory with the webhook's tls.crt and tls.key")
		manageCerts = flag.Bool("webhook-manage-certs", true, "generate the webhook certificate into a shared Secret and patch the caBundle (disable to use cert-manager)")
		namespace   = flag.String("namespace", "wsnap-system", "namespace the controller runs in")
		release     = flag.Bool("release-volumes", false, "restore the reclaim policy of every protected PV and exit (run before uninstalling)")
		force       = flag.Bool("force", false, "with --release-volumes, also release PVs whose data is in no snapshot")
		showVersion = flag.Bool("version", false, "print the version and exit")
	)
	opts := zap.Options{}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()
	if *showVersion {
		fmt.Println(version.Version)
		return
	}
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	log := ctrl.Log.WithName("setup")
	log.Info("starting", "version", version.Version)

	scheme := runtime.NewScheme()
	must(clientgoscheme.AddToScheme(scheme))
	must(wsnapv1.AddToScheme(scheme))

	hasLabel := func(key string) labels.Selector {
		req, err := labels.NewRequirement(key, selection.Exists, nil)
		must(err)
		return labels.NewSelector().Add(*req)
	}
	if *release {
		c, err := client.New(ctrl.GetConfigOrDie(), client.Options{Scheme: scheme})
		must(err)
		res, err := controller.ReleaseProtectedVolumes(context.Background(), c, *force)
		must(err)
		for _, n := range res.Released {
			fmt.Println("released", n)
		}
		for n, why := range res.Skipped {
			fmt.Printf("SKIPPED %s: %s (rerun with --force to release anyway)\n", n, why)
		}
		if len(res.Skipped) > 0 {
			os.Exit(2)
		}
		return
	}
	if *manageCerts {
		// Before the manager starts: the webhook server needs its
		// certificate on disk when it begins listening.
		c, err := client.New(ctrl.GetConfigOrDie(), client.Options{Scheme: scheme})
		must(err)
		must(webhook.CertSetup{Client: c, Namespace: *namespace, SecretName: "wsnap-webhook-cert",
			ServiceName: "wsnap-webhook", WebhookName: "wsnap", CertDir: *webhookDir}.Ensure(context.Background()))
	}
	managed := labels.SelectorFromSet(labels.Set{wsnapv1.LabelManagedBy: wsnapv1.ManagedByValue})
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		WebhookServer:          ctrlwebhook.NewServer(ctrlwebhook.Options{Port: *webhookPort, CertDir: *webhookDir}),
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: *metricsAddr},
		HealthProbeBindAddress: *probeAddr,
		LeaderElection:         *leaderElect,
		LeaderElectionID:       "wsnap-controller.wsnap.dev",
		// Cache only what wsnap owns or labels. Secrets are never cached;
		// the broker reads them live.
		Cache: cache.Options{ByObject: map[client.Object]cache.ByObject{
			&corev1.Pod{}:                   {Label: hasLabel(wsnapv1.LabelWorkspace)},
			&corev1.PersistentVolume{}:      {Label: hasLabel(wsnapv1.LabelProtected)},
			&corev1.PersistentVolumeClaim{}: {Label: managed},
			&batchv1.Job{}:                  {Label: managed},
		}},
		Client: client.Options{Cache: &client.CacheOptions{DisableFor: []client.Object{&corev1.Secret{}}}},
	})
	must(err)

	jobs := controller.JobConfig{AgentImage: *agentImage, BrokerURL: *brokerURL, Audience: *audience, CAConfigMap: *agentCA}
	mgr.GetWebhookServer().Register("/mutate-pods", &admission.Webhook{Handler: &webhook.Handler{
		Config:  webhook.Config{AgentImage: *agentImage, BrokerURL: *brokerURL, Audience: *audience, CAConfigMap: *agentCA, AllowAPIToken: *allowAPITok},
		Decoder: admission.NewDecoder(scheme),
	}})
	reader := mgr.GetAPIReader()
	must((&controller.PodReconciler{Client: mgr.GetClient(), Reader: reader}).SetupWithManager(mgr))
	must((&controller.PVReconciler{Client: mgr.GetClient(), Reader: reader, Jobs: jobs,
		Recorder: mgr.GetEventRecorderFor("wsnap")}).SetupWithManager(mgr))
	must((&controller.WorkspaceReconciler{Client: mgr.GetClient(), Reader: reader, Jobs: jobs}).SetupWithManager(mgr))
	must((&controller.RepositoryReconciler{Client: mgr.GetClient(), Reader: reader}).SetupWithManager(mgr))
	must((&controller.RequestReconciler{Client: mgr.GetClient(), Reader: reader, Jobs: jobs}).SetupWithManager(mgr))
	must((&controller.RequestReconciler{Client: mgr.GetClient(), Reader: reader, Jobs: jobs, Restore: true}).SetupWithManager(mgr))
	must(workspace.RegisterIndexes(context.Background(), mgr.GetFieldIndexer()))

	clientset, err := kubernetes.NewForConfig(mgr.GetConfig())
	must(err)
	srv := &broker.Server{
		Client: mgr.GetClient(), Reader: reader, Audience: *audience,
		Auth: &broker.CachingAuthenticator{
			Next: broker.TokenReviewAuthenticator{Client: clientset, Audience: *audience},
			TTL:  time.Minute,
		},
		Log:                    ctrl.Log.WithName("broker"),
		AllowAutomountedTokens: *allowAPITok,
		Limiter:                &broker.PodLimiter{Rate: rate.Limit(*podRate), Burst: *podBurst},
		Hub:                    &broker.Hub{},
		Gateway:                broker.KubeGatewayAuth{Client: clientset},
	}
	// Wake long polls from informer events: a new request, or a Workspace
	// whose writer changed. Every replica serves polls, so this runs
	// without the leader lock.
	must(mgr.Add(allReplicas(func(ctx context.Context) error {
		notify := func(obj any) {
			switch o := obj.(type) {
			case wsnapv1.Request:
				if co, ok := obj.(client.Object); ok {
					srv.Hub.Notify(broker.HubKey(co.GetNamespace(), o.GetWorkspaceName()))
				}
			case *wsnapv1.Workspace:
				srv.Hub.Notify(broker.HubKey(o.Namespace, o.Name))
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
	})))
	var tlsConfig *tls.Config
	if *tlsCert != "" {
		reloader := &broker.CertReloader{CertFile: *tlsCert, KeyFile: *tlsKey}
		if _, err := reloader.GetCertificate(nil); err != nil {
			must(err)
		}
		tlsConfig = &tls.Config{MinVersion: tls.VersionTLS12, GetCertificate: reloader.GetCertificate}
	} else {
		log.Info("broker serves plain HTTP; credentials cross the Pod network unencrypted (set --tls-cert-file)")
	}
	// The broker serves on every replica; it does not need the leader lock.
	// Health gauges for alerts (config/prometheus), read from the cache.
	ctrlmetrics.Registry.MustRegister(controller.StateCollector{Reader: mgr.GetCache()})
	must(mgr.Add(brokerRunnable{addr: *brokerAddr, handler: srv.Handler(), tls: tlsConfig}))
	if *manageCerts {
		// Uncached: the RBAC allows get/update of this one webhook
		// configuration by name, not list/watch, so a cached read would
		// wait forever for an informer that never syncs.
		direct, err := client.New(mgr.GetConfig(), client.Options{Scheme: scheme})
		must(err)
		certs := webhook.CertSetup{Client: direct, Namespace: *namespace, SecretName: "wsnap-webhook-cert",
			ServiceName: "wsnap-webhook", WebhookName: "wsnap", CertDir: *webhookDir}
		// Renew while running, on every replica (see CertSetup.Ensure).
		must(mgr.Add(allReplicas(func(ctx context.Context) error {
			t := time.NewTicker(10 * time.Minute)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return nil
				case <-t.C:
					if err := certs.Ensure(ctx); err != nil {
						log.Error(err, "webhook certificate check failed; retrying in 10m")
					}
				}
			}
		})))
	}
	must(mgr.AddHealthzCheck("healthz", func(*http.Request) error { return nil }))
	// Ready only when the webhook serves: with failurePolicy Fail, an
	// endpoint whose webhook is not listening refuses labeled Pods.
	must(mgr.AddReadyzCheck("webhook", mgr.GetWebhookServer().StartedChecker()))
	must(mgr.AddReadyzCheck("broker", func(*http.Request) error {
		conn, err := net.DialTimeout("tcp", "127.0.0.1"+*brokerAddr, time.Second)
		if err != nil {
			return err
		}
		return conn.Close()
	}))

	log.Info("starting", "broker", *brokerAddr, "audience", *audience)
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		log.Error(err, "manager stopped")
		os.Exit(1)
	}
}

// allReplicas runs fn on every replica, not only the leader.
type allReplicas func(context.Context) error

func (f allReplicas) Start(ctx context.Context) error { return f(ctx) }
func (allReplicas) NeedLeaderElection() bool          { return false }

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

func must(err error) {
	if err != nil {
		ctrl.Log.Error(err, "setup failed")
		os.Exit(1)
	}
}
