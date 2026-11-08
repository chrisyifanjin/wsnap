package main

import (
	"flag"

	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

// options are the controller's command-line flags.
type options struct {
	brokerAddr, plainAddr, brokerURL string
	brokerCerts                      bool
	audience, agentImage             string
	metricsAddr, probeAddr           string
	leaderElect                      bool
	tlsCert, tlsKey                  string
	allowAPIToken                    bool
	podRate                          float64
	podBurst                         int
	agentCA                          string
	webhookPort                      int
	webhookDir                       string
	webhookCerts                     bool
	namespace                        string
	release, force, showVersion      bool
	zap                              zap.Options
}

func parseFlags() options {
	var o options
	flag.StringVar(&o.brokerAddr, "broker-addr", ":8443", "broker listen address (TLS unless --broker-manage-certs=false and no --tls-cert-file)")
	flag.StringVar(&o.plainAddr, "broker-plain-addr", "", "also serve plain HTTP here, for agents started before an upgrade to TLS (for example :8080)")
	flag.StringVar(&o.brokerURL, "broker-url", "https://wsnap-broker.wsnap-system.svc:8443", "broker URL that agents use")
	flag.BoolVar(&o.brokerCerts, "broker-manage-certs", true, "without --tls-cert-file, generate the broker certificate into a shared Secret and give agents its CA")
	flag.StringVar(&o.audience, "audience", "wsnap", "audience of agent service account tokens")
	flag.StringVar(&o.agentImage, "agent-image", "ghcr.io/chrisyifanjin/wsnap-agent:dev", "default agent image for Jobs")
	flag.StringVar(&o.metricsAddr, "metrics-addr", ":8081", "metrics listen address")
	flag.StringVar(&o.probeAddr, "health-addr", ":8082", "health probe listen address")
	flag.BoolVar(&o.leaderElect, "leader-elect", false, "enable leader election for the reconcilers")
	flag.StringVar(&o.tlsCert, "tls-cert-file", "", "serve the broker over TLS with this certificate (reloaded on change)")
	flag.StringVar(&o.tlsKey, "tls-key-file", "", "private key for --tls-cert-file")
	flag.BoolVar(&o.allowAPIToken, "allow-automounted-tokens", false, "accept workload Pods that mount the default API token")
	flag.Float64Var(&o.podRate, "pod-rate", 1, "broker requests per second allowed per Pod")
	flag.IntVar(&o.podBurst, "pod-burst", 20, "broker request burst allowed per Pod")
	flag.StringVar(&o.agentCA, "agent-ca-configmap", "", "ConfigMap (key ca.crt) in workload namespaces that Jobs mount to trust a TLS broker")
	flag.IntVar(&o.webhookPort, "webhook-port", 9443, "admission webhook port")
	flag.StringVar(&o.webhookDir, "webhook-cert-dir", "/tmp/wsnap-webhook", "directory with the webhook's tls.crt and tls.key")
	flag.BoolVar(&o.webhookCerts, "webhook-manage-certs", true, "generate the webhook certificate into a shared Secret and patch the caBundle (disable to use cert-manager)")
	flag.StringVar(&o.namespace, "namespace", "wsnap-system", "namespace the controller runs in")
	flag.BoolVar(&o.release, "release-volumes", false, "restore the reclaim policy of every protected PV and exit (run before uninstalling)")
	flag.BoolVar(&o.force, "force", false, "with --release-volumes, also release PVs whose data is in no snapshot")
	flag.BoolVar(&o.showVersion, "version", false, "print the version and exit")
	o.zap.BindFlags(flag.CommandLine)
	flag.Parse()
	return o
}
