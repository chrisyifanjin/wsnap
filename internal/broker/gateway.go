package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/time/rate"
	"net/http"
	"strings"
	"time"

	authnv1 "k8s.io/api/authentication/v1"
	authzv1 "k8s.io/api/authorization/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"

	wsnapv1 "github.com/chrisyifanjin/wsnap/api/v1alpha1"
)

// The gateway API lets a platform API, an agent gateway or a sandbox
// router ask for snapshots and restores over HTTP without a Kubernetes
// client. The caller sends its own Kubernetes token. The broker checks with
// a SubjectAccessReview that the caller may create (or get) the request
// object, then creates it on the caller's behalf. Kubernetes RBAC stays the
// only permission model.
//
//	POST /v1/namespaces/{ns}/workspaces/{ws}/snapshots   {"wait": true, "timeoutSeconds": 120}
//	POST /v1/namespaces/{ns}/workspaces/{ws}/restores    {"snapshotID": "...", "sourceWorkspace": "...", "wait": true}
//	POST /v1/namespaces/{ns}/workspaces/{ws}/forks       {"name": "child", "repository": "..."}
//	GET  /v1/namespaces/{ns}/workspacesnapshots/{name}
//	GET  /v1/namespaces/{ns}/workspacerestores/{name}

// AnnotationRequestedBy records which user asked through the gateway.
const AnnotationRequestedBy = "wsnap.dev/requested-by"

// GatewayAuth authenticates gateway callers and checks their RBAC.
type GatewayAuth interface {
	AuthenticateUser(ctx context.Context, token string) (authnv1.UserInfo, error)
	Allowed(ctx context.Context, user authnv1.UserInfo, verb, namespace, resource string) (bool, string, error)
}

// KubeGatewayAuth uses TokenReview (the API server's own audiences, so
// ordinary service account and user tokens work) and SubjectAccessReview.
type KubeGatewayAuth struct {
	Client kubernetes.Interface
	// Reviews bounds TokenReviews for gateway callers. Nil: no bound.
	Reviews *rate.Limiter
}

func (k KubeGatewayAuth) AuthenticateUser(ctx context.Context, token string) (authnv1.UserInfo, error) {
	if k.Reviews != nil && !k.Reviews.Allow() {
		return authnv1.UserInfo{}, fmt.Errorf("%w: too many token reviews; try again", ErrAuthUnavailable)
	}
	r, err := k.Client.AuthenticationV1().TokenReviews().Create(ctx, &authnv1.TokenReview{Spec: authnv1.TokenReviewSpec{Token: token}}, metav1.CreateOptions{})
	if err != nil {
		return authnv1.UserInfo{}, fmt.Errorf("%w: token review: %w", ErrAuthUnavailable, err)
	}
	if !r.Status.Authenticated {
		return authnv1.UserInfo{}, fmt.Errorf("token rejected: %s", r.Status.Error)
	}
	return r.Status.User, nil
}

func (k KubeGatewayAuth) Allowed(ctx context.Context, u authnv1.UserInfo, verb, namespace, resource string) (bool, string, error) {
	extra := map[string]authzv1.ExtraValue{}
	for key, v := range u.Extra {
		extra[key] = authzv1.ExtraValue(v)
	}
	r, err := k.Client.AuthorizationV1().SubjectAccessReviews().Create(ctx, &authzv1.SubjectAccessReview{Spec: authzv1.SubjectAccessReviewSpec{
		User: u.Username, Groups: u.Groups, UID: u.UID, Extra: extra,
		ResourceAttributes: &authzv1.ResourceAttributes{Namespace: namespace, Verb: verb, Group: wsnapv1.GroupVersion.Group, Resource: resource},
	}}, metav1.CreateOptions{})
	if err != nil {
		return false, "", err
	}
	return r.Status.Allowed, r.Status.Reason, nil
}

type gatewayRequest struct {
	SnapshotID      string `json:"snapshotID,omitempty"`
	SourceWorkspace string `json:"sourceWorkspace,omitempty"`
	Wait            bool   `json:"wait,omitempty"`
	TimeoutSeconds  int    `json:"timeoutSeconds,omitempty"`
}

type gatewayResponse struct {
	Name       string `json:"name"`
	Phase      string `json:"phase"`
	SnapshotID string `json:"snapshotID,omitempty"`
	Executor   string `json:"executor,omitempty"`
	Message    string `json:"message,omitempty"`
}

// maxGatewayWait bounds wait=true requests below the server write timeout.
var maxGatewayWait = 50 * time.Second

func (s *Server) registerGateway(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/namespaces/{ns}/workspaces/{ws}/snapshots", s.gatewayCreate(false))
	mux.HandleFunc("POST /v1/namespaces/{ns}/workspaces/{ws}/restores", s.gatewayCreate(true))
	mux.HandleFunc("POST /v1/namespaces/{ns}/workspaces/{ws}/forks", s.gatewayFork)
	mux.HandleFunc("GET /v1/namespaces/{ns}/workspacesnapshots/{name}", s.gatewayGet(false))
	mux.HandleFunc("GET /v1/namespaces/{ns}/workspacerestores/{name}", s.gatewayGet(true))
}

func resourceName(restore bool) string {
	if restore {
		return "workspacerestores"
	}
	return "workspacesnapshots"
}

// gatewayCaller authenticates and authorizes one gateway call.
func (s *Server) gatewayCaller(r *http.Request, verb, ns, resource string) (authnv1.UserInfo, error) {
	if s.Gateway == nil {
		return authnv1.UserInfo{}, fail(http.StatusNotFound, "the gateway API is disabled")
	}
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok || token == "" {
		return authnv1.UserInfo{}, fail(http.StatusUnauthorized, "missing bearer token")
	}
	// Gateway callers are platforms, often behind one proxy IP; they share
	// the global TokenReview cap, not a per-IP budget.
	user, err := s.Gateway.AuthenticateUser(r.Context(), token)
	if errors.Is(err, ErrAuthUnavailable) {
		return user, fail(http.StatusServiceUnavailable, "%v", err)
	}
	if err != nil {
		return user, fail(http.StatusUnauthorized, "%v", err)
	}
	if s.Limiter != nil && !s.Limiter.Allow("user/"+user.Username) {
		return user, fail(http.StatusTooManyRequests, "too many requests from %s", user.Username)
	}
	allowed, reason, err := s.Gateway.Allowed(r.Context(), user, verb, ns, resource)
	if err != nil {
		return user, fail(http.StatusServiceUnavailable, "access review: %v", err)
	}
	if !allowed {
		return user, fail(http.StatusForbidden, "%s may not %s %s in namespace %s %s", user.Username, verb, resource, ns, reason)
	}
	return user, nil
}

func (s *Server) gatewayCreate(restore bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ns, wsName := r.PathValue("ns"), r.PathValue("ws")
		resp, err := s.doGatewayCreate(r, restore, ns, wsName)
		requests.WithLabelValues("gateway", resourceName(restore), statusCode(err)).Inc()
		if err != nil {
			s.writeError(w, err)
			return
		}
		writeJSON(w, resp)
	}
}

func (s *Server) doGatewayCreate(r *http.Request, restore bool, ns, wsName string) (*gatewayResponse, error) {
	ctx := r.Context()
	user, err := s.gatewayCaller(r, "create", ns, resourceName(restore))
	if err != nil {
		return nil, err
	}
	var body gatewayRequest
	if r.ContentLength != 0 {
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			return nil, fail(http.StatusBadRequest, "invalid body: %v", err)
		}
	}
	var ws wsnapv1.Workspace
	if err := s.Reader.Get(ctx, client.ObjectKey{Namespace: ns, Name: wsName}, &ws); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fail(http.StatusNotFound, "workspace %s/%s not found", ns, wsName)
		}
		return nil, err
	}
	meta := metav1.ObjectMeta{Namespace: ns, GenerateName: "gw-", Annotations: map[string]string{AnnotationRequestedBy: user.Username}}
	var obj client.Object
	if restore {
		obj = &wsnapv1.WorkspaceRestore{ObjectMeta: meta, Spec: wsnapv1.WorkspaceRestoreSpec{
			WorkspaceName: wsName, SnapshotID: body.SnapshotID, SourceWorkspace: body.SourceWorkspace}}
	} else {
		obj = &wsnapv1.WorkspaceSnapshot{ObjectMeta: meta, Spec: wsnapv1.WorkspaceSnapshotSpec{WorkspaceName: wsName}}
	}
	if err := s.Client.Create(ctx, obj); err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	s.Log.Info("gateway request created", "user", user.Username, "namespace", ns, "workspace", wsName, "request", obj.GetName(), "restore", restore)
	if !body.Wait {
		return &gatewayResponse{Name: obj.GetName(), Phase: wsnapv1.SnapshotPending}, nil
	}
	timeout := maxGatewayWait
	if t := time.Duration(body.TimeoutSeconds) * time.Second; t > 0 && t < timeout {
		timeout = t
	}
	return s.waitForRequest(ctx, ns, obj.GetName(), restore, timeout)
}

// waitForRequest polls the live object until it finishes or the timeout
// passes. Gateway waits are rare, so a 1s poll is cheaper than a watch.
func (s *Server) waitForRequest(ctx context.Context, ns, name string, restore bool, timeout time.Duration) (*gatewayResponse, error) {
	deadline := time.Now().Add(timeout)
	for {
		_, st, _, err := s.requestObject(ctx, ns, name, restore)
		if err != nil {
			return nil, err
		}
		resp := &gatewayResponse{Name: name, Phase: st.Phase, SnapshotID: st.SnapshotID, Executor: st.Executor, Message: st.Message}
		if st.Phase == wsnapv1.SnapshotSucceeded || st.Phase == wsnapv1.SnapshotFailed || time.Now().After(deadline) {
			return resp, nil
		}
		select {
		case <-ctx.Done():
			return resp, ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

func (s *Server) gatewayGet(restore bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ns, name := r.PathValue("ns"), r.PathValue("name")
		var resp *gatewayResponse
		_, err := s.gatewayCaller(r, "get", ns, resourceName(restore))
		if err == nil {
			var st *wsnapv1.WorkspaceSnapshotStatus
			_, st, _, err = s.requestObject(r.Context(), ns, name, restore)
			if apierrors.IsNotFound(err) {
				err = fail(http.StatusNotFound, "%s %s/%s not found", resourceName(restore), ns, name)
			} else if err == nil {
				resp = &gatewayResponse{Name: name, Phase: st.Phase, SnapshotID: st.SnapshotID, Executor: st.Executor, Message: st.Message}
			}
		}
		requests.WithLabelValues("gateway-get", resourceName(restore), statusCode(err)).Inc()
		if err != nil {
			s.writeError(w, err)
			return
		}
		writeJSON(w, resp)
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

type forkRequest struct {
	// Name of the new workspace (a DNS-1123 label).
	Name string `json:"name"`
	// Repository of the new workspace; empty means the parent's.
	Repository string `json:"repository,omitempty"`
}

type forkResponse struct {
	Workspace  string `json:"workspace"`
	SnapshotID string `json:"snapshotID"`
	Snapshot   string `json:"snapshot"`
}

// gatewayFork snapshots the parent now and creates the child workspace
// pinned to that snapshot. The child's first Pod restores exactly the
// parent's state at the moment of the fork, not a later one, and from then
// on the child has its own history. The caller needs RBAC to create both a
// WorkspaceSnapshot and a Workspace.
func (s *Server) gatewayFork(w http.ResponseWriter, r *http.Request) {
	resp, err := s.doGatewayFork(r)
	requests.WithLabelValues("gateway", "forks", statusCode(err)).Inc()
	if err != nil {
		s.writeError(w, err)
		return
	}
	writeJSON(w, resp)
}

func (s *Server) doGatewayFork(r *http.Request) (*forkResponse, error) {
	ctx := r.Context()
	ns, parentName := r.PathValue("ns"), r.PathValue("ws")
	user, err := s.gatewayCaller(r, "create", ns, "workspacesnapshots")
	if err != nil {
		return nil, err
	}
	if allowed, reason, err := s.Gateway.Allowed(ctx, user, "create", ns, "workspaces"); err != nil {
		return nil, fail(http.StatusServiceUnavailable, "access review: %v", err)
	} else if !allowed {
		return nil, fail(http.StatusForbidden, "%s may not create workspaces in namespace %s %s", user.Username, ns, reason)
	}
	var body forkRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		return nil, fail(http.StatusBadRequest, "invalid body: %v", err)
	}
	if errs := validation.IsDNS1123Label(body.Name); len(errs) > 0 {
		return nil, fail(http.StatusBadRequest, "name %q: %s", body.Name, strings.Join(errs, "; "))
	}
	var parent wsnapv1.Workspace
	if err := s.Reader.Get(ctx, client.ObjectKey{Namespace: ns, Name: parentName}, &parent); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fail(http.StatusNotFound, "workspace %s/%s not found", ns, parentName)
		}
		return nil, err
	}
	var existing wsnapv1.Workspace
	if err := s.Reader.Get(ctx, client.ObjectKey{Namespace: ns, Name: body.Name}, &existing); err == nil {
		return nil, fail(http.StatusConflict, "workspace %s/%s already exists", ns, body.Name)
	}
	snap := &wsnapv1.WorkspaceSnapshot{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, GenerateName: "fork-", Annotations: map[string]string{AnnotationRequestedBy: user.Username}},
		Spec:       wsnapv1.WorkspaceSnapshotSpec{WorkspaceName: parentName},
	}
	if err := s.Client.Create(ctx, snap); err != nil {
		return nil, fmt.Errorf("create snapshot request: %w", err)
	}
	res, err := s.waitForRequest(ctx, ns, snap.Name, false, maxGatewayWait)
	if err != nil {
		return nil, err
	}
	if res.Phase != wsnapv1.SnapshotSucceeded || res.SnapshotID == "" {
		return nil, fail(http.StatusConflict, "the parent snapshot did not finish (%s: %s); no fork was created", res.Phase, res.Message)
	}
	repo := body.Repository
	if repo == "" {
		repo = parent.Spec.RepositoryName
	}
	child := &wsnapv1.Workspace{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: body.Name, Annotations: map[string]string{AnnotationRequestedBy: user.Username},
			Labels: map[string]string{wsnapv1.LabelManagedBy: wsnapv1.ManagedByValue}},
		Spec: wsnapv1.WorkspaceSpec{RepositoryName: repo,
			RestoreFrom: &wsnapv1.RestoreSource{WorkspaceName: parentName, SnapshotID: res.SnapshotID}},
	}
	if err := s.Client.Create(ctx, child); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return nil, fail(http.StatusConflict, "workspace %s/%s already exists", ns, body.Name)
		}
		return nil, fmt.Errorf("create workspace: %w", err)
	}
	s.Log.Info("workspace forked", "user", user.Username, "namespace", ns, "parent", parentName, "child", body.Name, "snapshot", res.SnapshotID)
	return &forkResponse{Workspace: body.Name, SnapshotID: res.SnapshotID, Snapshot: snap.Name}, nil
}
