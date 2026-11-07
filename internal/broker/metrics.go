package broker

import (
	"errors"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

// Labels are bounded enums only (role, operation, HTTP code), never Pod or
// workspace names.
var (
	requests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "wsnap_broker_requests_total",
		Help: "Broker requests by endpoint, role and HTTP status code.",
	}, []string{"endpoint", "role", "code"})
	issueSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "wsnap_credential_issue_seconds",
		Help:    "Time to issue a credential, including the provider call (e.g. STS).",
		Buckets: prometheus.ExponentialBuckets(0.005, 2, 12),
	}, []string{"role"})
	reports = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "wsnap_agent_reports_total",
		Help: "Agent reports by operation and result (ok, reused, error).",
	}, []string{"operation", "result"})
)

func init() {
	metrics.Registry.MustRegister(requests, issueSeconds, reports)
}

func statusCode(err error) string {
	if err == nil {
		return "200"
	}
	var he *httpError
	if errors.As(err, &he) {
		return strconv.Itoa(he.code)
	}
	return "500"
}
