package main

import (
	"context"
	"net"
	"net/http"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/selection"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	ctrlwebhook "sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	wsnapv1 "github.com/chrisyifanjin/wsnap/api/v1alpha1"
	"github.com/chrisyifanjin/wsnap/internal/controller"
	"github.com/chrisyifanjin/wsnap/internal/webhook"
	"github.com/chrisyifanjin/wsnap/internal/workspace"
)

func newScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	must(clientgoscheme.AddToScheme(scheme))
	must(wsnapv1.AddToScheme(scheme))
	return scheme
}

func newManager(scheme *runtime.Scheme, o options) manager.Manager {
	managed := labels.SelectorFromSet(labels.Set{wsnapv1.LabelManagedBy: wsnapv1.ManagedByValue})
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		WebhookServer:          ctrlwebhook.NewServer(ctrlwebhook.Options{Port: o.webhookPort, CertDir: o.webhookDir}),
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: o.metricsAddr},
		HealthProbeBindAddress: o.probeAddr,
		LeaderElection:         o.leaderElect,
		LeaderElectionID:       "wsnap-controller.wsnap.dev",
		// Hand the lease over on shutdown instead of letting it expire: a
		// rollout that changes flags (such as the broker URL) must not
		// leave the old leader creating Jobs with the old configuration
		// while the new Pods wait. The process exits when the manager
		// stops, so no leader work runs after the release.
		LeaderElectionReleaseOnCancel: true,
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
	return mgr
}

func hasLabel(key string) labels.Selector {
	req, err := labels.NewRequirement(key, selection.Exists, nil)
	must(err)
	return labels.NewSelector().Add(*req)
}

func registerAdmission(mgr manager.Manager, scheme *runtime.Scheme, o options, caBundle func() []byte) {
	mgr.GetWebhookServer().Register("/mutate-pods", &admission.Webhook{Handler: &webhook.Handler{
		Config: webhook.Config{AgentImage: o.agentImage, BrokerURL: o.brokerURL, Audience: o.audience,
			CAConfigMap: o.agentCA, CABundle: caBundle, AllowAPIToken: o.allowAPIToken},
		Reader:  mgr.GetAPIReader(),
		Decoder: admission.NewDecoder(scheme),
	}})
}

func registerReconcilers(mgr manager.Manager, jobs controller.JobConfig) {
	c, reader := mgr.GetClient(), mgr.GetAPIReader()
	must((&controller.PodReconciler{Client: c, Reader: reader}).SetupWithManager(mgr))
	must((&controller.WorkerReconciler{Client: c, Reader: reader, Jobs: jobs}).SetupWithManager(mgr))
	must((&controller.PVReconciler{Client: c, Reader: reader, Jobs: jobs,
		Recorder: mgr.GetEventRecorder("wsnap")}).SetupWithManager(mgr))
	must((&controller.WorkspaceReconciler{Client: c, Reader: reader, Jobs: jobs}).SetupWithManager(mgr))
	must((&controller.RepositoryReconciler{Client: c, Reader: reader}).SetupWithManager(mgr))
	must((&controller.RequestReconciler{Client: c, Reader: reader, Jobs: jobs}).SetupWithManager(mgr))
	must((&controller.RequestReconciler{Client: c, Reader: reader, Jobs: jobs, Restore: true}).SetupWithManager(mgr))
	must(workspace.RegisterIndexes(context.Background(), mgr.GetFieldIndexer()))
}

func addHealthChecks(mgr manager.Manager, brokerAddr string) {
	must(mgr.AddHealthzCheck("healthz", func(*http.Request) error { return nil }))
	// Ready only when the webhook serves: with failurePolicy Fail, an
	// endpoint whose webhook is not listening refuses labeled Pods.
	must(mgr.AddReadyzCheck("webhook", mgr.GetWebhookServer().StartedChecker()))
	must(mgr.AddReadyzCheck("broker", func(*http.Request) error {
		conn, err := net.DialTimeout("tcp", "127.0.0.1"+brokerAddr, time.Second)
		if err != nil {
			return err
		}
		return conn.Close()
	}))
}
