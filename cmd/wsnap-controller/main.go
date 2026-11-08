// Command wsnap-controller runs the wsnap broker, admission webhook and
// reconcilers in one process.
//
// Lifecycle: parse flags; prepare certificates (they must exist before
// anything listens); build the manager; register admission and the
// reconcilers (leader only); add the broker and background loops (every
// replica); start, and stop on SIGTERM. The files group these steps:
// flags.go, certs.go, manager.go and broker.go.
package main

import (
	"context"
	"fmt"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/chrisyifanjin/wsnap/internal/controller"
	"github.com/chrisyifanjin/wsnap/internal/version"
)

func main() {
	o := parseFlags()
	if o.showVersion {
		fmt.Println(version.Version)
		return
	}
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&o.zap)))
	log := ctrl.Log.WithName("setup")
	log.Info("starting", "version", version.Version)
	scheme := newScheme()
	if o.release {
		os.Exit(releaseVolumes(scheme, o.force))
	}

	brokerCerts := ensureBrokerCerts(scheme, o)
	ensureWebhookCerts(scheme, o)

	mgr := newManager(scheme, o)
	var caBundle func() []byte
	if brokerCerts != nil {
		caBundle = brokerCerts.CABundle
	}
	registerAdmission(mgr, scheme, o, caBundle)
	registerReconcilers(mgr, controller.JobConfig{AgentImage: o.agentImage, BrokerURL: o.brokerURL,
		Audience: o.audience, CAConfigMap: o.agentCA, CABundle: caBundle})

	srv := newBroker(mgr, o)
	must(mgr.Add(wakeLongPolls(mgr, srv.Hub)))
	tlsConfig := brokerTLSConfig(o, brokerCerts, log)
	if o.plainAddr != "" {
		log.Info("broker also serves plain HTTP for agents started before TLS; remove --broker-plain-addr when they are gone", "addr", o.plainAddr)
		must(mgr.Add(brokerRunnable{addr: o.plainAddr, handler: srv.Handler()}))
	}
	// Health gauges for alerts (config/prometheus), read from the cache.
	ctrlmetrics.Registry.MustRegister(controller.StateCollector{Reader: mgr.GetCache()})
	must(mgr.Add(brokerRunnable{addr: o.brokerAddr, handler: srv.Handler(), tls: tlsConfig}))
	addCertRenewal(mgr, scheme, o, brokerCerts, log)
	addHealthChecks(mgr, o.brokerAddr)

	log.Info("starting", "broker", o.brokerAddr, "audience", o.audience)
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		log.Error(err, "manager stopped")
		os.Exit(1)
	}
}

// releaseVolumes restores the reclaim policy of every protected PV (run
// before uninstalling) and returns the exit code: 2 if any was skipped.
func releaseVolumes(scheme *runtime.Scheme, force bool) int {
	c, err := client.New(ctrl.GetConfigOrDie(), client.Options{Scheme: scheme})
	must(err)
	res, err := controller.ReleaseProtectedVolumes(context.Background(), c, force)
	must(err)
	for _, n := range res.Released {
		fmt.Println("released", n)
	}
	for n, why := range res.Skipped {
		fmt.Printf("SKIPPED %s: %s (rerun with --force to release anyway)\n", n, why)
	}
	if len(res.Skipped) > 0 {
		return 2
	}
	return 0
}

func must(err error) {
	if err != nil {
		ctrl.Log.Error(err, "setup failed")
		os.Exit(1)
	}
}
