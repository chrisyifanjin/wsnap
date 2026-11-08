// Package broker serves credentials to wsnap agents. An agent proves which
// Pod it runs in with a projected service account token. The broker maps
// the Pod to its workspace, decides which roles the Pod may have, and
// returns a credential scoped to that workspace and role.
//
// Files by responsibility:
//
//   - identity.go: who is calling (TokenReview, live Pod, Job registration)
//     and which roles it may have;
//   - credentials.go: issuing a scoped credential;
//   - writer.go: one writer per workspace, writer epochs, volume fencing;
//   - worker.go: separate worker Pods (worker mode);
//   - report.go: recording snapshot and restore results, final marks;
//   - requests.go: delivering WorkspaceSnapshot and WorkspaceRestore objects;
//   - gateway.go: the REST API for platforms, authorized by RBAC;
//   - auth.go, limits.go, certs.go, tls.go: token checks, rate limits, TLS.
package broker

import (
	"errors"
	"fmt"
	"net/http"
	"sync"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/chrisyifanjin/wsnap/internal/workspace"
)

// Server is the broker's HTTP server.
type Server struct {
	// Client writes Workspace status and PV annotations, and lists
	// protected PVs from the cache.
	Client client.Client
	// Reader reads live objects. It must not be a cache: the broker's
	// decisions depend on the current Pod status, and Secrets are not
	// cached on purpose.
	Reader   client.Reader
	Auth     Authenticator
	Audience string
	Log      logr.Logger
	// AllowAutomountedTokens accepts workload Pods that mount the default
	// API token. Off by default: untrusted code holding an API token may be
	// able to create Pods and so obtain workspace credentials itself.
	AllowAutomountedTokens bool
	// Limiter bounds requests per Pod. Nil disables limiting.
	Limiter *PodLimiter
	// ReviewsPerIP bounds TokenReviews (for tokens not in the cache) per
	// client IP. Nil: the default.
	ReviewsPerIP *PodLimiter
	reviewsOnce  sync.Once
	// Hub wakes long polls when requests appear. Nil falls back to
	// periodic checks only.
	Hub *Hub
	// Gateway enables the gateway API (gateway.go). Nil disables it.
	Gateway GatewayAuth
}

// Handler returns the broker's routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/credentials", s.handleCredentials)
	mux.HandleFunc("POST /v1/report", s.handleReport)
	mux.HandleFunc("POST /v1/wait", s.handleWait)
	mux.HandleFunc("POST /v1/worker-state", s.handleWorkerState)
	s.registerGateway(mux)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	return mux
}

type httpError struct {
	code int
	msg  string
}

func (e *httpError) Error() string { return e.msg }

func fail(code int, format string, args ...any) error {
	return &httpError{code: code, msg: fmt.Sprintf(format, args...)}
}

func volumeError(err error) error {
	if errors.Is(err, workspace.ErrBadVolume) {
		return fail(http.StatusFailedDependency, "%v", err)
	}
	return upstream(err, "resolve workspace volume")
}

// upstream maps an API server error: a missing object is a configuration
// problem (424), anything else may pass (503, which agents retry). A
// transient error must not become a 4xx, or a final snapshot is dropped.
func upstream(err error, format string, args ...any) error {
	msg := fmt.Sprintf(format, args...) + ": " + err.Error()
	if apierrors.IsNotFound(err) {
		return fail(http.StatusFailedDependency, "%s", msg)
	}
	return fail(http.StatusServiceUnavailable, "%s", msg)
}

func (s *Server) writeError(w http.ResponseWriter, err error) {
	code := http.StatusInternalServerError
	var he *httpError
	if errors.As(err, &he) {
		code = he.code
	}
	s.Log.Info("request refused", "status", code, "error", err.Error())
	http.Error(w, err.Error(), code)
}
