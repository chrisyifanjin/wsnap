package controller

import (
	"context"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	wsnapv1 "github.com/chrisyifanjin/wsnap/api/v1alpha1"
	"github.com/chrisyifanjin/wsnap/internal/workspace"
)

func requestEnv(t *testing.T, objs ...client.Object) (*RequestReconciler, client.Client) {
	t.Helper()
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = wsnapv1.AddToScheme(scheme)
	repo := &wsnapv1.SnapshotRepository{ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: "repo"}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(append(objs, repo)...).
		WithStatusSubresource(&wsnapv1.WorkspaceSnapshot{}, &wsnapv1.WorkspaceRestore{}, &batchv1.Job{}).Build()
	return &RequestReconciler{Client: c, Reader: c, Jobs: JobConfig{AgentImage: "agent:test", BrokerURL: "http://b", Audience: "wsnap"}}, c
}

func snapReq(created time.Time, phase, executor string) *wsnapv1.WorkspaceSnapshot {
	return &wsnapv1.WorkspaceSnapshot{
		ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: "req", UID: "req-uid", CreationTimestamp: metav1.NewTime(created)},
		Spec:       wsnapv1.WorkspaceSnapshotSpec{WorkspaceName: "ws1"},
		Status:     wsnapv1.WorkspaceSnapshotStatus{Phase: phase, Executor: executor},
	}
}

func wsWith(volume *wsnapv1.VolumeRef, writer *wsnapv1.PodRef) *wsnapv1.Workspace {
	return &wsnapv1.Workspace{ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: "ws1"},
		Spec:   wsnapv1.WorkspaceSpec{RepositoryName: "repo"},
		Status: wsnapv1.WorkspaceStatus{Volume: volume, WriterPod: writer}}
}

func runningPod(name, uid string, claim string) *corev1.Pod {
	p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: name, UID: types.UID(uid)},
		Status: corev1.PodStatus{Phase: corev1.PodRunning}}
	if claim != "" {
		p.Spec.Volumes = []corev1.Volume{{Name: "workspace", VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claim}}}}
	}
	return p
}

func reconcileReq(t *testing.T, r *RequestReconciler, c client.Client) *wsnapv1.WorkspaceSnapshot {
	t.Helper()
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNS, Name: "req"}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	var out wsnapv1.WorkspaceSnapshot
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: "req"}, &out)
	return &out
}

var pvcVol = &wsnapv1.VolumeRef{Kind: workspace.KindPVC, ClaimName: "data", VolumeName: "pv-1"}

func boundClaim() *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: "data"}, Spec: corev1.PersistentVolumeClaimSpec{VolumeName: "pv-1"}}
}

func TestRequestRouting(t *testing.T) {
	now := time.Now()
	cases := []struct {
		name      string
		objs      []client.Object
		req       *wsnapv1.WorkspaceSnapshot
		phase     string
		executor  string
		wantMover bool
	}{
		{"live writer: left for the sidecar", []client.Object{wsWith(pvcVol, &wsnapv1.PodRef{Name: "p", UID: "u"}), runningPod("p", "u", "data"), boundClaim()},
			snapReq(now, "", ""), wsnapv1.SnapshotPending, "", false},
		{"idle PVC: mover", []client.Object{wsWith(pvcVol, &wsnapv1.PodRef{Name: "gone", UID: "x"}), boundClaim()},
			snapReq(now, "", ""), wsnapv1.SnapshotRunning, "mover", true},
		{"emptyDir and no pod: nothing to snapshot", []client.Object{wsWith(&wsnapv1.VolumeRef{Kind: workspace.KindEmptyDir}, nil)},
			snapReq(now, "", ""), wsnapv1.SnapshotFailed, "", false},
		{"claim mounted by a pod without a sidecar", []client.Object{wsWith(pvcVol, nil), boundClaim(), runningPod("intruder", "i", "data")},
			snapReq(now, "", ""), wsnapv1.SnapshotFailed, "", false},
		{"claim deleted", []client.Object{wsWith(pvcVol, nil)},
			snapReq(now, "", ""), wsnapv1.SnapshotFailed, "", false},
		{"claimed by a sidecar whose pod stopped: routed again", []client.Object{wsWith(pvcVol, &wsnapv1.PodRef{Name: "gone", UID: "x"}), boundClaim()},
			snapReq(now, wsnapv1.SnapshotRunning, "sidecar"), wsnapv1.SnapshotPending, "", false},
		{"timed out", []client.Object{wsWith(pvcVol, &wsnapv1.PodRef{Name: "p", UID: "u"}), runningPod("p", "u", "data")},
			snapReq(now.Add(-RequestTimeout-time.Minute), wsnapv1.SnapshotPending, ""), wsnapv1.SnapshotFailed, "", false},
		{"workspace missing", nil, snapReq(now, "", ""), wsnapv1.SnapshotFailed, "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, c := requestEnv(t, append(tc.objs, tc.req)...)
			got := reconcileReq(t, r, c)
			if got.Status.Phase != tc.phase || got.Status.Executor != tc.executor {
				t.Fatalf("phase %q executor %q (%s), want %q %q", got.Status.Phase, got.Status.Executor, got.Status.Message, tc.phase, tc.executor)
			}
			var job batchv1.Job
			err := c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: requestJobName(request{tc.req})}, &job)
			if tc.wantMover != (err == nil) {
				t.Fatalf("mover job exists = %v, want %v", err == nil, tc.wantMover)
			}
			if tc.wantMover {
				if job.Annotations[wsnapv1.AnnotationRequest] != "req" || job.Annotations[wsnapv1.AnnotationVolumeName] != "pv-1" {
					t.Errorf("mover annotations = %v", job.Annotations)
				}
				args := job.Spec.Template.Spec.Containers[0].Args
				if args[0] != "mover" || args[2] != "--request=req" {
					t.Errorf("mover args = %v", args)
				}
				if v := job.Spec.Template.Spec.Volumes; v[len(v)-1].PersistentVolumeClaim == nil || v[len(v)-1].PersistentVolumeClaim.ClaimName != "data" {
					t.Errorf("mover does not mount the workspace claim: %+v", v)
				}
			}
		})
	}
}

func TestRequestMoverFailure(t *testing.T) {
	sr := snapReq(time.Now(), wsnapv1.SnapshotRunning, "mover")
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: requestJobName(request{sr})},
		Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Message: "BackoffLimitExceeded"}}}}
	r, c := requestEnv(t, sr, job)
	if got := reconcileReq(t, r, c); got.Status.Phase != wsnapv1.SnapshotFailed {
		t.Fatalf("phase %q after the mover failed", got.Status.Phase)
	}
}

func TestTerminalRequestIsLeftAlone(t *testing.T) {
	sr := snapReq(time.Now().Add(-time.Hour), wsnapv1.SnapshotSucceeded, "sidecar")
	sr.Status.SnapshotID = "s1"
	r, c := requestEnv(t, sr)
	if got := reconcileReq(t, r, c); got.Status.Phase != wsnapv1.SnapshotSucceeded || got.Status.SnapshotID != "s1" {
		t.Fatalf("terminal request changed: %+v", got.Status)
	}
}

func TestTerminalRequestReleasesFinishedJobPVC(t *testing.T) {
	for _, restore := range []bool{false, true} {
		name := "snapshot"
		var obj requestObj = snapReq(time.Now(), wsnapv1.SnapshotSucceeded, "mover")
		if restore {
			name = "restore"
			obj = &wsnapv1.WorkspaceRestore{ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: "req", UID: "req-uid", CreationTimestamp: metav1.Now()},
				Spec:   wsnapv1.WorkspaceRestoreSpec{WorkspaceName: "ws1"},
				Status: wsnapv1.WorkspaceSnapshotStatus{Phase: wsnapv1.SnapshotSucceeded, Executor: "restorer"}}
		}
		t.Run(name, func(t *testing.T) {
			kind := "mover"
			if restore {
				kind = "restorer"
			}
			job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: requestJobName(request{obj}),
				Labels:      map[string]string{wsnapv1.LabelJob: kind},
				Annotations: map[string]string{wsnapv1.AnnotationRequest: "req"}}}
			r, c := requestEnv(t, obj, job)
			r.Restore = restore
			if result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNS, Name: "req"}}); err != nil || result.RequeueAfter == 0 {
				t.Fatalf("running job must stay until its agent exits: result=%+v err=%v", result, err)
			}
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(job), &batchv1.Job{}); err != nil {
				t.Fatalf("job removed before it completed: %v", err)
			}
			job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
			if err := c.Status().Update(context.Background(), job); err != nil {
				t.Fatal(err)
			}
			if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNS, Name: "req"}}); err != nil {
				t.Fatal(err)
			}
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(job), &batchv1.Job{}); err == nil {
				t.Fatal("finished job still holds the PVC")
			}
		})
	}
}

func TestRestoreRouting(t *testing.T) {
	newReq := func() *wsnapv1.WorkspaceRestore {
		return &wsnapv1.WorkspaceRestore{ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: "req", UID: "rq", CreationTimestamp: metav1.Now()},
			Spec: wsnapv1.WorkspaceRestoreSpec{WorkspaceName: "ws1", SnapshotID: "s1"}}
	}
	// Live writer: left for the sidecar to claim.
	live := newReq()
	r, c := requestEnv(t, live, wsWith(pvcVol, &wsnapv1.PodRef{Name: "p", UID: "u"}), runningPod("p", "u", "data"), boundClaim())
	r.Restore = true
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNS, Name: "req"}}); err != nil {
		t.Fatal(err)
	}
	var got wsnapv1.WorkspaceRestore
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: "req"}, &got)
	if got.Status.Phase != wsnapv1.SnapshotPending {
		t.Fatalf("live: phase %q", got.Status.Phase)
	}
	// Idle PVC: a restorer Job.
	idle := newReq()
	r, c = requestEnv(t, idle, wsWith(pvcVol, nil), boundClaim())
	r.Restore = true
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNS, Name: "req"}}); err != nil {
		t.Fatal(err)
	}
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: "req"}, &got)
	if got.Status.Phase != wsnapv1.SnapshotRunning || got.Status.Executor != "restorer" {
		t.Fatalf("idle: %+v", got.Status)
	}
	var job batchv1.Job
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: requestJobName(request{idle})}, &job); err != nil {
		t.Fatalf("restorer job: %v", err)
	}
	if args := job.Spec.Template.Spec.Containers[0].Args; args[0] != "restorer" || args[2] != "--request=req" || job.Labels[wsnapv1.LabelJob] != "restorer" {
		t.Errorf("restorer job args %v labels %v", args, job.Labels)
	}
	if requestJobName(request{idle}) == requestJobName(request{snapReq(time.Now(), "", "")}) {
		t.Error("snapshot and restore requests with the same name share a job name")
	}
}

// TestRequestJobRecreatedAfterFailedCreate: the request was marked Running
// but the Job create failed (or the Job vanished). The next reconcile must
// create the Job, not fail the request.
func TestRequestJobRecreatedAfterFailedCreate(t *testing.T) {
	sr := snapReq(time.Now(), wsnapv1.SnapshotRunning, "mover")
	r, c := requestEnv(t, sr, wsWith(pvcVol, nil), boundClaim())
	got := reconcileReq(t, r, c)
	if got.Status.Phase != wsnapv1.SnapshotRunning {
		t.Fatalf("phase %q (%s), want Running", got.Status.Phase, got.Status.Message)
	}
	var job batchv1.Job
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: requestJobName(request{sr})}, &job); err != nil {
		t.Fatalf("job not recreated: %v", err)
	}
}

// TestRequestMarkedRunningBeforeJobExists checks the order of writes that
// lets a fast Job Pod find its request already Running.
func TestRequestMarkedRunningBeforeJobExists(t *testing.T) {
	sr := snapReq(time.Now(), "", "")
	r, c := requestEnv(t, sr, wsWith(pvcVol, nil), boundClaim())
	r.Client = &createSpy{Client: c, onCreate: func() {
		var cur wsnapv1.WorkspaceSnapshot
		_ = c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: "req"}, &cur)
		if cur.Status.Phase != wsnapv1.SnapshotRunning {
			t.Errorf("job created while the request is %q; a fast job would be refused", cur.Status.Phase)
		}
	}}
	reconcileReq(t, r, c)
}

type createSpy struct {
	client.Client
	onCreate func()
}

func (s *createSpy) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if _, ok := obj.(*batchv1.Job); ok {
		s.onCreate()
	}
	return s.Client.Create(ctx, obj, opts...)
}

// TestOwnMoverPodIsNotAForeignWriter: the request's own mover Pod mounts
// the claim; on the recreate path it must not count as a Pod without a
// sidecar.
func TestOwnMoverPodIsNotAForeignWriter(t *testing.T) {
	sr := snapReq(time.Now(), wsnapv1.SnapshotRunning, "mover")
	moverPod := runningPod("mover-pod", "m", "data")
	moverPod.Labels = map[string]string{wsnapv1.LabelJob: "mover"}
	r, c := requestEnv(t, sr, wsWith(pvcVol, nil), boundClaim(), moverPod)
	if got := reconcileReq(t, r, c); got.Status.Phase != wsnapv1.SnapshotRunning {
		t.Fatalf("phase %q (%s): the request's own mover was taken for a foreign writer", got.Status.Phase, got.Status.Message)
	}
}

// TestDeadClaimantIsRerouted: the request goes back to Pending, and the
// next reconcile hands it to a mover because no Pod runs.
func TestDeadClaimantIsRerouted(t *testing.T) {
	sr := snapReq(time.Now(), wsnapv1.SnapshotRunning, "sidecar")
	sr.Status.ClaimedBy = "x"
	r, c := requestEnv(t, sr, wsWith(pvcVol, &wsnapv1.PodRef{Name: "gone", UID: "x"}), boundClaim())
	reconcileReq(t, r, c)
	got := reconcileReq(t, r, c)
	if got.Status.Phase != wsnapv1.SnapshotRunning || got.Status.Executor != "mover" {
		t.Fatalf("after a dead claimant: %+v, want Running by a mover", got.Status)
	}
}

// TestOneJobPerVolume (finding R5): a second request waits while another
// wsnap Job works on the same volume.
func TestOneJobPerVolume(t *testing.T) {
	busy := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: "wsnap-reqr-other",
		Labels:      map[string]string{wsnapv1.LabelManagedBy: wsnapv1.ManagedByValue, wsnapv1.LabelJob: "restorer"},
		Annotations: map[string]string{wsnapv1.AnnotationVolumeName: "pv-1"}}}
	sr := snapReq(time.Now(), "", "")
	r, c := requestEnv(t, sr, wsWith(pvcVol, nil), boundClaim(), busy)
	got := reconcileReq(t, r, c)
	var job batchv1.Job
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: requestJobName(request{sr})}, &job); err == nil {
		t.Fatal("a second job started on a busy volume")
	}
	if got.Status.Phase != wsnapv1.SnapshotPending {
		t.Fatalf("phase %q, want Pending while waiting", got.Status.Phase)
	}
}

// TestTimeoutDeletesTheJob (finding R5).
func TestTimeoutDeletesTheJob(t *testing.T) {
	sr := snapReq(time.Now().Add(-RequestTimeout-time.Minute), wsnapv1.SnapshotRunning, "mover")
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: requestJobName(request{sr})}}
	r, c := requestEnv(t, sr, job)
	if got := reconcileReq(t, r, c); got.Status.Phase != wsnapv1.SnapshotFailed {
		t.Fatalf("phase %q", got.Status.Phase)
	}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: job.Name}, &batchv1.Job{}); err == nil {
		t.Fatal("the job of a timed-out request still exists and may keep writing")
	}
}
