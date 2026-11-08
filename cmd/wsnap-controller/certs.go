package main

import (
	"context"
	"crypto/tls"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/chrisyifanjin/wsnap/internal/broker"
	"github.com/chrisyifanjin/wsnap/internal/webhook"
)

// ensureBrokerCerts creates or loads the broker's own certificate, unless
// the operator brings one (--tls-cert-file) or turns TLS off. It runs
// before the manager: Jobs and sidecars need the CA from the start.
func ensureBrokerCerts(scheme *runtime.Scheme, o options) *broker.ManagedCerts {
	if o.tlsCert != "" || !o.brokerCerts {
		return nil
	}
	c, err := client.New(ctrl.GetConfigOrDie(), client.Options{Scheme: scheme})
	must(err)
	certs := &broker.ManagedCerts{Client: c, Namespace: o.namespace, SecretName: "wsnap-broker-cert",
		DNSNames: []string{"wsnap-broker." + o.namespace + ".svc", "wsnap-broker." + o.namespace + ".svc.cluster.local"}}
	must(certs.Ensure(context.Background()))
	return certs
}

func webhookCertSetup(c client.Client, o options) webhook.CertSetup {
	return webhook.CertSetup{Client: c, Namespace: o.namespace, SecretName: "wsnap-webhook-cert",
		ServiceName: "wsnap-webhook", WebhookName: "wsnap", CertDir: o.webhookDir}
}

// ensureWebhookCerts runs before the manager starts: the webhook server
// needs its certificate on disk when it begins listening.
func ensureWebhookCerts(scheme *runtime.Scheme, o options) {
	if !o.webhookCerts {
		return
	}
	c, err := client.New(ctrl.GetConfigOrDie(), client.Options{Scheme: scheme})
	must(err)
	must(webhookCertSetup(c, o).Ensure(context.Background()))
}

// brokerTLSConfig picks the broker's certificate source: the operator's
// files, the managed Secret, or none (plain HTTP).
func brokerTLSConfig(o options, managed *broker.ManagedCerts, log logr.Logger) *tls.Config {
	switch {
	case o.tlsCert != "":
		reloader := &broker.CertReloader{CertFile: o.tlsCert, KeyFile: o.tlsKey}
		if _, err := reloader.GetCertificate(nil); err != nil {
			must(err)
		}
		return &tls.Config{MinVersion: tls.VersionTLS12, GetCertificate: reloader.GetCertificate}
	case managed != nil:
		return &tls.Config{MinVersion: tls.VersionTLS12, GetCertificate: managed.GetCertificate}
	default:
		log.Info("broker serves plain HTTP; credentials cross the Pod network unencrypted (set --tls-cert-file or --broker-manage-certs)")
		return nil
	}
}

// addCertRenewal keeps both certificates fresh on every replica: the
// replica that finds one expiring renews it, the others load the result.
func addCertRenewal(mgr manager.Manager, scheme *runtime.Scheme, o options, managed *broker.ManagedCerts, log logr.Logger) {
	if managed != nil {
		must(mgr.Add(every(time.Hour, "broker certificate check", managed.Ensure, log)))
	}
	if o.webhookCerts {
		// Uncached: the RBAC allows get/update of this one webhook
		// configuration by name, not list/watch, so a cached read would
		// wait forever for an informer that never syncs.
		direct, err := client.New(mgr.GetConfig(), client.Options{Scheme: scheme})
		must(err)
		must(mgr.Add(every(10*time.Minute, "webhook certificate check", webhookCertSetup(direct, o).Ensure, log)))
	}
}

// every runs fn at each interval on every replica until shutdown, and
// logs a failure instead of stopping: the next run tries again.
func every(interval time.Duration, what string, fn func(context.Context) error, log logr.Logger) allReplicas {
	return func(ctx context.Context) error {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-t.C:
				if err := fn(ctx); err != nil {
					log.Error(err, what+" failed; retrying", "in", interval)
				}
			}
		}
	}
}
