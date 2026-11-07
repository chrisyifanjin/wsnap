package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	authnv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	wsnapv1 "github.com/chrisyifanjin/wsnap/api/v1alpha1"
)

// fakeGateway: token "platform" may create snapshots only; "viewer" may
// only get.
type fakeGateway struct{}

func (fakeGateway) AuthenticateUser(_ context.Context, token string) (authnv1.UserInfo, error) {
	if token == "platform" || token == "viewer" || token == "forker" {
		return authnv1.UserInfo{Username: "system:serviceaccount:team-a:" + token}, nil
	}
	return authnv1.UserInfo{}, fmt.Errorf("unknown token")
}

func (fakeGateway) Allowed(_ context.Context, u authnv1.UserInfo, verb, ns, resource string) (bool, string, error) {
	if ns != "team-a" {
		return false, "wrong namespace", nil
	}
	switch u.Username {
	case "system:serviceaccount:team-a:platform":
		return resource == "workspacesnapshots", "", nil
	case "system:serviceaccount:team-a:forker":
		return verb == "create" && (resource == "workspacesnapshots" || resource == "workspaces"), "", nil
	case "system:serviceaccount:team-a:viewer":
		return verb == "get", "", nil
	}
	return false, "", nil
}

func gatewayCall(t *testing.T, e *env, method, path, token string, body any) (*httptest.ResponseRecorder, gatewayResponse) {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		_ = json.NewEncoder(&buf).Encode(body)
	}
	req := httptest.NewRequest(method, path, &buf)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	e.srv.Handler().ServeHTTP(rec, req)
	var resp gatewayResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	return rec, resp
}

func TestGatewayAuthorization(t *testing.T) {
	ws := &wsnapv1.Workspace{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "ws1"}, Spec: wsnapv1.WorkspaceSpec{RepositoryName: "repo"}}
	e := newEnv(t, ws)
	e.srv.Gateway = fakeGateway{}
	cases := []struct {
		name, method, path, token string
		want                      int
	}{
		{"platform creates a snapshot", "POST", "/v1/namespaces/team-a/workspaces/ws1/snapshots", "platform", 200},
		{"platform may not restore", "POST", "/v1/namespaces/team-a/workspaces/ws1/restores", "platform", 403},
		{"viewer may not create", "POST", "/v1/namespaces/team-a/workspaces/ws1/snapshots", "viewer", 403},
		{"other namespace", "POST", "/v1/namespaces/team-b/workspaces/ws1/snapshots", "platform", 403},
		{"no token", "POST", "/v1/namespaces/team-a/workspaces/ws1/snapshots", "", 401},
		{"bad token", "POST", "/v1/namespaces/team-a/workspaces/ws1/snapshots", "forged", 401},
		{"missing workspace", "POST", "/v1/namespaces/team-a/workspaces/ghost/snapshots", "platform", 404},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec, _ := gatewayCall(t, e, tc.method, tc.path, tc.token, nil)
			if rec.Code != tc.want {
				t.Fatalf("status %d, want %d: %s", rec.Code, tc.want, rec.Body.String())
			}
		})
	}
	// The created request names its requester for audit.
	var list wsnapv1.WorkspaceSnapshotList
	_ = e.client.List(context.Background(), &list, client.InNamespace(ns))
	if len(list.Items) != 1 || list.Items[0].Annotations[AnnotationRequestedBy] != "system:serviceaccount:team-a:platform" || list.Items[0].Spec.WorkspaceName != "ws1" {
		t.Fatalf("created requests = %+v", list.Items)
	}
	// The viewer can read it.
	rec, resp := gatewayCall(t, e, "GET", "/v1/namespaces/team-a/workspacesnapshots/"+list.Items[0].Name, "viewer", nil)
	if rec.Code != 200 || resp.Name != list.Items[0].Name {
		t.Errorf("get: %d %+v", rec.Code, resp)
	}
}

func TestGatewayWaitReturnsTheOutcome(t *testing.T) {
	ws := &wsnapv1.Workspace{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "ws1"}, Spec: wsnapv1.WorkspaceSpec{RepositoryName: "repo"}}
	e := newEnv(t, ws)
	e.srv.Gateway = fakeGateway{}
	// Play the sidecar: complete any request as soon as it appears.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 50; i++ {
			var list wsnapv1.WorkspaceSnapshotList
			_ = e.client.List(context.Background(), &list, client.InNamespace(ns))
			if len(list.Items) == 1 {
				r := list.Items[0]
				r.Status.Phase, r.Status.SnapshotID, r.Status.Executor = wsnapv1.SnapshotSucceeded, "snap-1", "sidecar"
				_ = e.client.Status().Update(context.Background(), &r)
				return
			}
			<-time.After(20 * time.Millisecond)
		}
	}()
	rec, resp := gatewayCall(t, e, "POST", "/v1/namespaces/team-a/workspaces/ws1/snapshots", "platform", map[string]any{"wait": true, "timeoutSeconds": 10})
	<-done
	if rec.Code != 200 || resp.Phase != wsnapv1.SnapshotSucceeded || resp.SnapshotID != "snap-1" {
		t.Fatalf("wait response %d %+v", rec.Code, resp)
	}
}

func TestGatewayDisabled(t *testing.T) {
	e := newEnv(t)
	if rec, _ := gatewayCall(t, e, "POST", "/v1/namespaces/team-a/workspaces/ws1/snapshots", "platform", nil); rec.Code != 404 {
		t.Errorf("disabled gateway answered %d", rec.Code)
	}
}

// completeSoon plays the sidecar: it completes the first snapshot request.
func completeSoon(e *env, id string) chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 100; i++ {
			var list wsnapv1.WorkspaceSnapshotList
			_ = e.client.List(context.Background(), &list, client.InNamespace(ns))
			if len(list.Items) == 1 {
				r := list.Items[0]
				r.Status.Phase, r.Status.SnapshotID, r.Status.Executor = wsnapv1.SnapshotSucceeded, id, "sidecar"
				_ = e.client.Status().Update(context.Background(), &r)
				return
			}
			<-time.After(20 * time.Millisecond)
		}
	}()
	return done
}

func TestGatewayFork(t *testing.T) {
	parent := &wsnapv1.Workspace{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "parent"}, Spec: wsnapv1.WorkspaceSpec{RepositoryName: "repo"}}
	e := newEnv(t, parent)
	e.srv.Gateway = fakeGateway{}
	done := completeSoon(e, "snap-at-fork")
	rec, _ := gatewayCall(t, e, "POST", "/v1/namespaces/team-a/workspaces/parent/forks", "forker", map[string]any{"name": "attempt-1"})
	<-done
	if rec.Code != 200 {
		t.Fatalf("fork: %d %s", rec.Code, rec.Body.String())
	}
	var child wsnapv1.Workspace
	if err := e.client.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: "attempt-1"}, &child); err != nil {
		t.Fatalf("child workspace not created: %v", err)
	}
	rf := child.Spec.RestoreFrom
	if rf == nil || rf.WorkspaceName != "parent" || rf.SnapshotID != "snap-at-fork" || child.Spec.RepositoryName != "repo" {
		t.Fatalf("child spec = %+v; want restoreFrom pinned to the snapshot taken at the fork", child.Spec)
	}
	// The platform may snapshot but not create workspaces: no fork.
	if rec, _ := gatewayCall(t, e, "POST", "/v1/namespaces/team-a/workspaces/parent/forks", "platform", map[string]any{"name": "attempt-2"}); rec.Code != 403 {
		t.Errorf("fork without permission to create workspaces: %d", rec.Code)
	}
	if rec, _ := gatewayCall(t, e, "POST", "/v1/namespaces/team-a/workspaces/parent/forks", "forker", map[string]any{"name": "Bad_Name"}); rec.Code != 400 {
		t.Errorf("invalid name: %d", rec.Code)
	}
	if rec, _ := gatewayCall(t, e, "POST", "/v1/namespaces/team-a/workspaces/parent/forks", "forker", map[string]any{"name": "attempt-1"}); rec.Code != 409 {
		t.Errorf("existing child name: %d", rec.Code)
	}
}

func TestGatewayForkWithoutParentSnapshotCreatesNothing(t *testing.T) {
	old := maxGatewayWait
	maxGatewayWait = 300 * time.Millisecond
	t.Cleanup(func() { maxGatewayWait = old })
	parent := &wsnapv1.Workspace{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "parent"}, Spec: wsnapv1.WorkspaceSpec{RepositoryName: "repo"}}
	e := newEnv(t, parent)
	e.srv.Gateway = fakeGateway{}
	if rec, _ := gatewayCall(t, e, "POST", "/v1/namespaces/team-a/workspaces/parent/forks", "forker", map[string]any{"name": "attempt-1"}); rec.Code != 409 {
		t.Fatalf("fork with an unfinished parent snapshot: %d", rec.Code)
	}
	if err := e.client.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: "attempt-1"}, &wsnapv1.Workspace{}); err == nil {
		t.Fatal("a child was created without a parent snapshot")
	}
}
