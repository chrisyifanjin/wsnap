package broker

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	wsnapv1 "github.com/chrisyifanjin/wsnap/api/v1alpha1"
	"github.com/chrisyifanjin/wsnap/internal/credentials"
	"github.com/chrisyifanjin/wsnap/internal/engine"
	"github.com/chrisyifanjin/wsnap/internal/protocol"
	"github.com/chrisyifanjin/wsnap/internal/workspace"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (s *Server) handleCredentials(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	resp, err := s.credentials(r)
	role := "unknown"
	if resp != nil {
		role = string(resp.Role)
		issueSeconds.WithLabelValues(role).Observe(time.Since(start).Seconds())
	}
	requests.WithLabelValues("credentials", role, statusCode(err)).Inc()
	if err != nil {
		s.writeError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *Server) credentials(r *http.Request) (*protocol.CredentialsResponse, error) {
	ctx := r.Context()
	c, err := s.resolve(r)
	if err != nil {
		return nil, err
	}
	var req protocol.CredentialsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || !req.Role.Valid() {
		return nil, fail(http.StatusBadRequest, "body must be {\"role\": restore|snapshot|mover|maintenance}")
	}
	restoreReq, err := s.authorizeRole(ctx, c, req)
	if err != nil {
		return nil, err
	}
	var repo wsnapv1.SnapshotRepository
	if err := s.Reader.Get(ctx, client.ObjectKey{Namespace: c.id.Namespace, Name: c.id.Repository}, &repo); err != nil {
		return nil, upstream(err, "snapshot repository %s/%s", c.id.Namespace, c.id.Repository)
	}
	ws, err := s.admit(ctx, c, req.Role, restoreReq)
	if err != nil {
		return nil, err
	}
	src, err := s.restoreSource(ctx, c, req.Role, restoreReq, ws, &repo)
	if err != nil {
		return nil, err
	}
	resp, err := s.issue(ctx, c, &repo, src.workspace, req.Role)
	if err != nil {
		return nil, err
	}
	resp.SnapshotID, resp.SourceWorkspace, resp.RestoreFromGeneration = src.snapshotID, src.source, src.generation
	switch req.Role {
	case protocol.RoleRestore:
		if src.snapshotID == "" {
			// The recorded newest snapshot, of the source for a fork. A
			// deposed writer can still upload snapshots, but the broker never
			// records them, so they are never restored.
			var tws wsnapv1.Workspace
			if err := s.Reader.Get(ctx, client.ObjectKey{Namespace: c.id.Namespace, Name: src.workspace}, &tws); err != nil {
				return nil, upstream(err, "workspace %s", src.workspace)
			}
			if last := tws.Status.LastSnapshot; last != nil {
				resp.LatestSnapshotID = last.ID
			}
		}
	case protocol.RoleMaintenance:
		if resp.KeepIDs, err = s.pinnedSnapshots(ctx, c.id); err != nil {
			return nil, err
		}
	}
	s.Log.Info("issued credential", "namespace", c.id.Namespace, "workspace", c.id.Name, "role", req.Role,
		"pod", c.pod.Name, "scoped", resp.Credentials.Scoped, "expires", resp.Credentials.ExpiresAt)
	return resp, nil
}

// authorizeRole checks that the caller may have the requested role. A
// claimed WorkspaceRestore authorizes the restore role in a running Pod
// and selects what to restore; it is returned for the later steps.
func (s *Server) authorizeRole(ctx context.Context, c *caller, req protocol.CredentialsRequest) (*wsnapv1.WorkspaceRestore, error) {
	var restoreReq *wsnapv1.WorkspaceRestore
	if req.Role == protocol.RoleRestore && req.Request != "" {
		var err error
		if restoreReq, err = s.claimedRestore(ctx, c, req.Request); err != nil {
			return nil, err
		}
	} else if err := c.allowed(req.Role); err != nil {
		return nil, err
	}
	if restoreReq == nil && c.job == wsnapv1.JobRestorer && req.Role == protocol.RoleRestore {
		return nil, fail(http.StatusForbidden, "a restorer job must name its WorkspaceRestore")
	}
	if c.job == wsnapv1.JobMover || c.job == wsnapv1.JobRestorer {
		if err := s.checkNoWorkloadOnVolume(ctx, c); err != nil {
			return nil, err
		}
	}
	return restoreReq, nil
}

// admit applies the writer rules for callers that serve a live workload:
// a workspace Pod's own sidecar, or a worker Job on behalf of its
// workload Pod. Other Jobs (mover, restorer, maintenance) were admitted
// when the controller created them. It returns the Workspace, or nil.
func (s *Server) admit(ctx context.Context, c *caller, role protocol.Role, restoreReq *wsnapv1.WorkspaceRestore) (*wsnapv1.Workspace, error) {
	switch c.job {
	case "":
		ws, err := s.ensureWorkspace(ctx, c.id)
		if err != nil {
			return nil, err
		}
		return ws, s.admitWriter(ctx, c, ws, role, restoreReq)
	case wsnapv1.JobWorker:
		ws, err := s.ensureWorkspace(ctx, c.id)
		if err != nil {
			return nil, err
		}
		if ws.Spec.ExecutionMode() != wsnapv1.ExecutionWorker {
			return nil, fail(http.StatusForbidden, "workspace %s is not in worker mode", c.id.Name)
		}
		if c.workloadPod == nil {
			if c.workerEpoch == 0 {
				return nil, fail(http.StatusConflict, "worker's workload Pod is gone before initialization")
			}
			return ws, nil // the workload stopped; the worker finishes its final snapshot
		}
		if err := workspace.CheckWorkerIsolation(c.workloadPod, s.Audience, s.AllowAutomountedTokens); err != nil {
			return nil, fail(http.StatusForbidden, "workload Pod %s: %v", c.workloadPod.Name, err)
		}
		if err := s.checkNoAPIToken(ctx, c.workloadPod); err != nil {
			return nil, err
		}
		// The writer is the workload Pod, not the worker.
		workload := *c
		workload.pod = c.workloadPod
		if err := s.admitWriter(ctx, &workload, ws, role, restoreReq); err != nil {
			return nil, err
		}
		return ws, s.bindWorkerEpoch(ctx, c)
	}
	return nil, nil
}

// admitWriter is the one place where sidecar and worker modes admit a
// writer: at most one live writer per workspace; no restore while an
// older volume of the workspace waits for its rescue; and the volume is
// protected before any credential is handed out, so data the Pod writes
// is never reclaimed without a final snapshot.
func (s *Server) admitWriter(ctx context.Context, writer *caller, ws *wsnapv1.Workspace, role protocol.Role, restoreReq *wsnapv1.WorkspaceRestore) error {
	if err := s.checkSingleWriter(ctx, writer, ws); err != nil {
		return err
	}
	if role == protocol.RoleRestore && restoreReq == nil {
		if err := s.checkNoPendingRescue(ctx, writer); err != nil {
			return err
		}
	}
	return s.trackVolume(ctx, writer, role == protocol.RoleSnapshot)
}

// restoreTarget is what a credential opens: normally the caller's own
// workspace, or another workspace's repository for a fork.
type restoreTarget struct {
	workspace  string
	snapshotID string // pinned snapshot; empty means the recorded newest
	source     string // set for a fork
	generation int64  // the restoreFrom generation consumed, or 0
}

// restoreSource picks what a restore reads: a WorkspaceRestore, or
// Workspace.spec.restoreFrom once per generation. For a fork it replaces
// repo with the source workspace's repository.
func (s *Server) restoreSource(ctx context.Context, c *caller, role protocol.Role, restoreReq *wsnapv1.WorkspaceRestore,
	ws *wsnapv1.Workspace, repo *wsnapv1.SnapshotRepository) (restoreTarget, error) {
	t := restoreTarget{workspace: c.id.Name}
	var rf *wsnapv1.RestoreSource
	switch {
	case restoreReq != nil:
		rf = &wsnapv1.RestoreSource{WorkspaceName: restoreReq.Spec.SourceWorkspace, SnapshotID: restoreReq.Spec.SnapshotID}
	case role == protocol.RoleRestore && ws != nil && ws.Spec.RestoreFrom != nil && ws.Status.RestoreFromGeneration != ws.Generation:
		rf, t.generation = ws.Spec.RestoreFrom, ws.Generation
	}
	if rf == nil {
		return t, nil
	}
	t.snapshotID = rf.SnapshotID
	if rf.WorkspaceName != "" && rf.WorkspaceName != c.id.Name {
		var src wsnapv1.Workspace
		if err := s.Reader.Get(ctx, client.ObjectKey{Namespace: c.id.Namespace, Name: rf.WorkspaceName}, &src); err != nil {
			return t, upstream(err, "restoreFrom workspace %s", rf.WorkspaceName)
		}
		if err := s.Reader.Get(ctx, client.ObjectKey{Namespace: c.id.Namespace, Name: src.Spec.RepositoryName}, repo); err != nil {
			return t, upstream(err, "repository of workspace %s", rf.WorkspaceName)
		}
		t.workspace, t.source = rf.WorkspaceName, rf.WorkspaceName
	}
	return t, nil
}

// issue asks the repository's provider for a credential scoped to one
// workspace's repository (target) and one role.
func (s *Server) issue(ctx context.Context, c *caller, repo *wsnapv1.SnapshotRepository, target string, role protocol.Role) (*protocol.CredentialsResponse, error) {
	var secret corev1.Secret
	if err := s.Reader.Get(ctx, client.ObjectKey{Namespace: repo.Namespace, Name: repo.Spec.Credentials.SecretName}, &secret); err != nil {
		return nil, upstream(err, "credentials secret")
	}
	backend, err := workspace.Backend(repo, c.id.Namespace, target)
	if err != nil {
		return nil, fail(http.StatusFailedDependency, "%v", err)
	}
	provider, err := workspace.Provider(repo)
	if err != nil {
		return nil, fail(http.StatusFailedDependency, "%v", err)
	}
	eng := workspace.Engine(repo)
	creds, err := provider.Issue(ctx, credentials.Request{
		Role: role, Namespace: c.id.Namespace, Workspace: target,
		Backend: backend, Secret: secret.Data,
		WriterDeletePrefixes: engine.WriterDeletePrefixes(eng),
		TTL:                  workspace.TTL(repo),
	})
	if err != nil {
		// STS, IAM or Azure may be briefly down; agents retry 5xx.
		return nil, fail(http.StatusServiceUnavailable, "issue credential: %v", err)
	}
	password, err := credentials.RepositoryPassword(secret.Data[credentials.KeyMasterKey], c.id.Namespace, target)
	if err != nil {
		return nil, fail(http.StatusFailedDependency, "%v", err)
	}
	return &protocol.CredentialsResponse{
		Namespace: c.id.Namespace, Workspace: c.id.Name, Role: role,
		Engine: eng, Backend: backend, Credentials: creds, RepositoryPassword: password,
		Interval:  protocol.Duration{Duration: workspace.Interval(repo)},
		KeepLast:  workspace.KeepLast(repo),
		KeepDaily: workspace.KeepDaily(repo),
		// Only an explicit setting: an unset field must not override the
		// agent's own --operation-timeout with the default.
		OperationTimeout: protocol.Duration{Duration: repo.Spec.OperationTimeout.Duration},
	}, nil
}

// maxSuperseded bounds Workspace.status.supersededSnapshots.
const maxSuperseded = 10

// pinnedSnapshots lists the snapshots of workspace id that maintenance
// must keep: its recorded newest snapshot, and every snapshot that a
// Workspace (fork or rollback) or an unfinished WorkspaceRestore pins.
func (s *Server) pinnedSnapshots(ctx context.Context, id workspace.Identity) ([]string, error) {
	var keep []string
	var list wsnapv1.WorkspaceList
	if err := s.Reader.List(ctx, &list, client.InNamespace(id.Namespace)); err != nil {
		return nil, upstream(err, "list workspaces")
	}
	for _, ws := range list.Items {
		if ws.Name == id.Name && ws.Status.LastSnapshot != nil {
			keep = append(keep, ws.Status.LastSnapshot.ID)
		}
		if ws.Name == id.Name {
			for _, s := range ws.Status.SupersededSnapshots {
				keep = append(keep, s.ID)
			}
		}
		if rf := ws.Spec.RestoreFrom; rf != nil && rf.SnapshotID != "" {
			src := rf.WorkspaceName
			if src == "" {
				src = ws.Name
			}
			if src == id.Name {
				keep = append(keep, rf.SnapshotID)
			}
		}
	}
	var restores wsnapv1.WorkspaceRestoreList
	if err := s.Reader.List(ctx, &restores, client.InNamespace(id.Namespace)); err != nil {
		return nil, upstream(err, "list workspace restores")
	}
	for _, r := range restores.Items {
		src := r.Spec.SourceWorkspace
		if src == "" {
			src = r.Spec.WorkspaceName
		}
		done := r.Status.Phase == wsnapv1.SnapshotSucceeded || r.Status.Phase == wsnapv1.SnapshotFailed
		if src == id.Name && r.Spec.SnapshotID != "" && !done {
			keep = append(keep, r.Spec.SnapshotID)
		}
	}
	return keep, nil
}

func (s *Server) ensureWorkspace(ctx context.Context, id workspace.Identity) (*wsnapv1.Workspace, error) {
	var ws wsnapv1.Workspace
	err := s.Reader.Get(ctx, client.ObjectKey{Namespace: id.Namespace, Name: id.Name}, &ws)
	if apierrors.IsNotFound(err) {
		ws = wsnapv1.Workspace{
			ObjectMeta: metav1.ObjectMeta{Namespace: id.Namespace, Name: id.Name,
				Labels: map[string]string{wsnapv1.LabelManagedBy: wsnapv1.ManagedByValue}},
			Spec: wsnapv1.WorkspaceSpec{RepositoryName: id.Repository},
		}
		err = s.Client.Create(ctx, &ws)
		if err == nil {
			return &ws, nil
		}
		if !apierrors.IsAlreadyExists(err) {
			return nil, upstream(err, "create workspace")
		}
		// Lost the race: the winner (a user who set restoreFrom, or another
		// replica) has the real spec and status; ours is empty.
		err = s.Reader.Get(ctx, client.ObjectKey{Namespace: id.Namespace, Name: id.Name}, &ws)
	}
	if err != nil {
		return nil, upstream(err, "workspace %s", id.Name)
	}
	if ws.Spec.RepositoryName != id.Repository {
		return nil, fail(http.StatusConflict, "workspace %s uses repository %s, but the pod asks for %s", id.Name, ws.Spec.RepositoryName, id.Repository)
	}
	return &ws, nil
}
