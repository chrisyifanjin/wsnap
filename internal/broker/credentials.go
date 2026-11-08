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
	// A claimed WorkspaceRestore authorizes the restore role in a running
	// Pod and selects what to restore.
	var restoreReq *wsnapv1.WorkspaceRestore
	if req.Role == protocol.RoleRestore && req.Request != "" {
		var err error
		if restoreReq, err = s.claimedRestore(ctx, c, req.Request); err != nil {
			return nil, err
		}
	} else if err := c.allowed(req.Role); err != nil {
		return nil, err
	}
	if restoreReq == nil && c.job == "restorer" && req.Role == protocol.RoleRestore {
		return nil, fail(http.StatusForbidden, "a restorer job must name its WorkspaceRestore")
	}
	if c.job == "mover" || c.job == "restorer" {
		if err := s.checkNoWorkloadOnVolume(ctx, c); err != nil {
			return nil, err
		}
	}
	var repo wsnapv1.SnapshotRepository
	if err := s.Reader.Get(ctx, client.ObjectKey{Namespace: c.id.Namespace, Name: c.id.Repository}, &repo); err != nil {
		return nil, upstream(err, "snapshot repository %s/%s", c.id.Namespace, c.id.Repository)
	}
	var ws *wsnapv1.Workspace
	if c.job == "" {
		var err error
		ws, err = s.ensureWorkspace(ctx, c.id)
		if err != nil {
			return nil, err
		}
		if err := s.checkSingleWriter(ctx, c, ws); err != nil {
			return nil, err
		}
		if req.Role == protocol.RoleRestore && restoreReq == nil {
			if err := s.checkNoPendingRescue(ctx, c); err != nil {
				return nil, err
			}
		}
		// Protect the volume before handing out any credential, so data
		// the Pod writes is never reclaimed without a final snapshot.
		if err := s.trackVolume(ctx, c, req.Role == protocol.RoleSnapshot); err != nil {
			return nil, err
		}
	} else if c.job == "worker" {
		ws, err = s.ensureWorkspace(ctx, c.id)
		if err != nil {
			return nil, err
		}
		if ws.Spec.ExecutionMode() != wsnapv1.ExecutionWorker {
			return nil, fail(http.StatusForbidden, "workspace %s is not in worker mode", c.id.Name)
		}
		if c.workloadPod != nil {
			if err := workspace.CheckWorkerIsolation(c.workloadPod, s.Audience, s.AllowAutomountedTokens); err != nil {
				return nil, fail(http.StatusForbidden, "workload Pod %s: %v", c.workloadPod.Name, err)
			}
			if err := s.checkNoAPIToken(ctx, c.workloadPod); err != nil {
				return nil, err
			}
			workload := *c
			workload.pod = c.workloadPod
			if err := s.checkSingleWriter(ctx, &workload, ws); err != nil {
				return nil, err
			}
			if req.Role == protocol.RoleRestore && restoreReq == nil {
				if err := s.checkNoPendingRescue(ctx, &workload); err != nil {
					return nil, err
				}
			}
			if err := s.trackVolume(ctx, &workload, req.Role == protocol.RoleSnapshot); err != nil {
				return nil, err
			}
			if err := s.bindWorkerEpoch(ctx, c); err != nil {
				return nil, err
			}
		} else if c.workerEpoch == 0 {
			return nil, fail(http.StatusConflict, "worker's workload Pod is gone before initialization")
		}
	}
	// A restore may read another workspace (fork) or a pinned snapshot,
	// chosen by a WorkspaceRestore or by Workspace.spec.restoreFrom.
	target := c.id.Name
	var snapshotID, source string
	var restoreGen int64
	var rf *wsnapv1.RestoreSource
	switch {
	case restoreReq != nil:
		rf = &wsnapv1.RestoreSource{WorkspaceName: restoreReq.Spec.SourceWorkspace, SnapshotID: restoreReq.Spec.SnapshotID}
	case req.Role == protocol.RoleRestore && ws != nil && ws.Spec.RestoreFrom != nil && ws.Status.RestoreFromGeneration != ws.Generation:
		rf, restoreGen = ws.Spec.RestoreFrom, ws.Generation
	}
	if rf != nil {
		snapshotID = rf.SnapshotID
		if rf.WorkspaceName != "" && rf.WorkspaceName != c.id.Name {
			var src wsnapv1.Workspace
			if err := s.Reader.Get(ctx, client.ObjectKey{Namespace: c.id.Namespace, Name: rf.WorkspaceName}, &src); err != nil {
				return nil, upstream(err, "restoreFrom workspace %s", rf.WorkspaceName)
			}
			if err := s.Reader.Get(ctx, client.ObjectKey{Namespace: c.id.Namespace, Name: src.Spec.RepositoryName}, &repo); err != nil {
				return nil, upstream(err, "repository of workspace %s", rf.WorkspaceName)
			}
			target, source = rf.WorkspaceName, rf.WorkspaceName
		}
	}
	var secret corev1.Secret
	if err := s.Reader.Get(ctx, client.ObjectKey{Namespace: repo.Namespace, Name: repo.Spec.Credentials.SecretName}, &secret); err != nil {
		return nil, upstream(err, "credentials secret")
	}
	backend, err := workspace.Backend(&repo, c.id.Namespace, target)
	if err != nil {
		return nil, fail(http.StatusFailedDependency, "%v", err)
	}
	provider, err := workspace.Provider(&repo)
	if err != nil {
		return nil, fail(http.StatusFailedDependency, "%v", err)
	}
	eng := workspace.Engine(&repo)
	creds, err := provider.Issue(ctx, credentials.Request{
		Role: req.Role, Namespace: c.id.Namespace, Workspace: target,
		Backend: backend, Secret: secret.Data,
		WriterDeletePrefixes: engine.WriterDeletePrefixes(eng),
		TTL:                  workspace.TTL(&repo),
	})
	if err != nil {
		// STS, IAM or Azure may be briefly down; agents retry 5xx.
		return nil, fail(http.StatusServiceUnavailable, "issue credential: %v", err)
	}
	password, err := credentials.RepositoryPassword(secret.Data[credentials.KeyMasterKey], c.id.Namespace, target)
	if err != nil {
		return nil, fail(http.StatusFailedDependency, "%v", err)
	}
	var latestID string
	var keep []string
	switch req.Role {
	case protocol.RoleRestore:
		if snapshotID == "" {
			// The recorded newest snapshot, of the source for a fork. A
			// deposed writer can still upload snapshots, but the broker never
			// records them, so they are never restored.
			var tws wsnapv1.Workspace
			if err := s.Reader.Get(ctx, client.ObjectKey{Namespace: c.id.Namespace, Name: target}, &tws); err != nil {
				return nil, upstream(err, "workspace %s", target)
			}
			if last := tws.Status.LastSnapshot; last != nil {
				latestID = last.ID
			}
		}
	case protocol.RoleMaintenance:
		if keep, err = s.pinnedSnapshots(ctx, c.id); err != nil {
			return nil, err
		}
	}
	s.Log.Info("issued credential", "namespace", c.id.Namespace, "workspace", c.id.Name, "role", req.Role,
		"pod", c.pod.Name, "scoped", creds.Scoped, "expires", creds.ExpiresAt)
	return &protocol.CredentialsResponse{
		Namespace: c.id.Namespace, Workspace: c.id.Name, Role: req.Role,
		Engine: eng, Backend: backend, Credentials: creds, RepositoryPassword: password,
		Interval:  protocol.Duration{Duration: workspace.Interval(&repo)},
		KeepLast:  workspace.KeepLast(&repo),
		KeepDaily: workspace.KeepDaily(&repo),
		// Only an explicit setting: an unset field must not override the
		// agent's own --operation-timeout with the default.
		OperationTimeout: protocol.Duration{Duration: repo.Spec.OperationTimeout.Duration},
		SnapshotID:       snapshotID, SourceWorkspace: source, RestoreFromGeneration: restoreGen,
		LatestSnapshotID: latestID, KeepIDs: keep,
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
