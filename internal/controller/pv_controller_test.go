package controller

import (
	"context"
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	wsnapv1 "github.com/chrisyifanjin/wsnap/api/v1alpha1"
	"github.com/chrisyifanjin/wsnap/internal/workspace"
)

const testNS = "team-a"

func releasedPV(annotations map[string]string) *corev1.PersistentVolume {
	a := map[string]string{
		wsnapv1.AnnotationWorkspaceRef:          testNS + "/ws1",
		wsnapv1.AnnotationOriginalReclaimPolicy: "Delete",
	}
	for k, v := range annotations {
		a[k] = v
	}
	return &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-1", UID: "uid-pv-1", Labels: map[string]string{wsnapv1.LabelProtected: "true"}, Annotations: a},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
			StorageClassName:              "standard",
			AccessModes:                   []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Capacity:                      corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
			ClaimRef:                      &corev1.ObjectReference{Namespace: testNS, Name: "old-claim", UID: "old-uid"},
		},
		Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeReleased},
	}
}

func rescueClaimFixture(pv *corev1.PersistentVolume, uid types.UID) *corev1.PersistentVolumeClaim {
	class := pv.Spec.StorageClassName
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: rescueClaimName(pv.Name), UID: uid,
			Labels: map[string]string{wsnapv1.LabelManagedBy: wsnapv1.ManagedByValue}},
		Spec: corev1.PersistentVolumeClaimSpec{VolumeName: pv.Name, StorageClassName: &class,
			AccessModes: pv.Spec.AccessModes, VolumeMode: pv.Spec.VolumeMode,
			Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: pv.Spec.Capacity[corev1.ResourceStorage]}}},
	}
}

func testWorkspace(mutate ...func(*wsnapv1.Workspace)) *wsnapv1.Workspace {
	ws := &wsnapv1.Workspace{
		ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: "ws1"},
		Spec:       wsnapv1.WorkspaceSpec{RepositoryName: "repo"},
	}
	for _, m := range mutate {
		m(ws)
	}
	return ws
}

func newPVReconciler(t *testing.T, objs ...client.Object) (*PVReconciler, client.Client) {
	t.Helper()
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = wsnapv1.AddToScheme(scheme)
	repo := &wsnapv1.SnapshotRepository{ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: "repo"}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(append(objs, repo)...).
		WithStatusSubresource(&wsnapv1.Workspace{}, &corev1.PersistentVolume{}, &batchv1.Job{}).Build()
	return &PVReconciler{Client: uidPVCClient{c}, Reader: c, Recorder: record.NewFakeRecorder(10),
		Jobs: JobConfig{AgentImage: "agent:test", BrokerURL: "http://broker", Audience: "wsnap"}}, c
}

// The fake client does not assign UIDs; the API server does so on Create.
type uidPVCClient struct{ client.Client }

func (c uidPVCClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if pvc, ok := obj.(*corev1.PersistentVolumeClaim); ok && pvc.UID == "" {
		pvc.UID = types.UID("uid-" + pvc.Name)
	}
	return c.Client.Create(ctx, obj, opts...)
}

func reconcilePV(t *testing.T, r *PVReconciler) {
	t.Helper()
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "pv-1"}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
}

func getPV(t *testing.T, c client.Client) *corev1.PersistentVolume {
	t.Helper()
	var pv corev1.PersistentVolume
	if err := c.Get(context.Background(), types.NamespacedName{Name: "pv-1"}, &pv); err != nil {
		t.Fatal(err)
	}
	return &pv
}

func TestReleasedWithFinalSnapshotIsReclaimed(t *testing.T) {
	r, c := newPVReconciler(t, testWorkspace(), releasedPV(map[string]string{
		workspace.AnnotationWriterPod: "u1", workspace.AnnotationFinalPod: "u1",
	}))
	reconcilePV(t, r)
	pv := getPV(t, c)
	if pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimDelete || pv.Labels[wsnapv1.LabelProtected] != "" {
		t.Errorf("policy %s, labels %v: want Delete and unprotected", pv.Spec.PersistentVolumeReclaimPolicy, pv.Labels)
	}
	var jobs batchv1.JobList
	_ = c.List(context.Background(), &jobs)
	if len(jobs.Items) != 0 {
		t.Errorf("started %d jobs for a volume that needed none", len(jobs.Items))
	}
}

func TestOriginallyRetainedVolumeStaysRetained(t *testing.T) {
	r, c := newPVReconciler(t, testWorkspace(), releasedPV(map[string]string{
		workspace.AnnotationWriterPod: "u1", workspace.AnnotationFinalPod: "u1",
		wsnapv1.AnnotationOriginalReclaimPolicy: "Retain",
	}))
	reconcilePV(t, r)
	if p := getPV(t, c).Spec.PersistentVolumeReclaimPolicy; p != corev1.PersistentVolumeReclaimRetain {
		t.Errorf("policy %s, want the original Retain", p)
	}
	if got := getPV(t, c); got.Annotations[wsnapv1.AnnotationWorkspaceRef] != "" || got.Annotations[workspace.AnnotationWriterPod] != "" {
		t.Errorf("reusable Retain PV kept old workspace identity: %v", got.Annotations)
	}
}

func TestReleasedWithoutFinalSnapshotIsRescued(t *testing.T) {
	// The writer (u2) is newer than the last final snapshot (u1).
	r, c := newPVReconciler(t, testWorkspace(), releasedPV(map[string]string{
		workspace.AnnotationWriterPod: "u2", workspace.AnnotationFinalPod: "u1",
	}))
	reconcilePV(t, r)
	pv := getPV(t, c)
	if pv.Annotations[wsnapv1.AnnotationRescue] != RescueRunning {
		t.Fatalf("rescue annotation = %q", pv.Annotations[wsnapv1.AnnotationRescue])
	}
	if pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain {
		t.Error("PV lost its Retain policy during the rescue")
	}
	ref := pv.Spec.ClaimRef
	if ref == nil || ref.Name != rescueClaimName("pv-1") || ref.UID != types.UID("uid-"+rescueClaimName("pv-1")) {
		t.Errorf("claimRef = %+v, want the rescue claim's UID", ref)
	}
	var claim corev1.PersistentVolumeClaim
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: rescueClaimName("pv-1")}, &claim); err != nil {
		t.Fatalf("rescue claim: %v", err)
	}
	if claim.Spec.VolumeName != "pv-1" || ptr.Deref(claim.Spec.StorageClassName, "") != "standard" {
		t.Errorf("rescue claim spec = %+v", claim.Spec)
	}
	var job batchv1.Job
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: moverJobName("pv-1")}, &job); err != nil {
		t.Fatalf("mover job: %v", err)
	}
	if job.Labels[wsnapv1.LabelJob] != "mover" || job.Labels[wsnapv1.LabelJobWorkspace] != "ws1" ||
		job.Annotations[wsnapv1.AnnotationVolumeName] != "pv-1" || job.Labels[wsnapv1.LabelManagedBy] != wsnapv1.ManagedByValue {
		t.Errorf("job metadata = %v / %v", job.Labels, job.Annotations)
	}
	spec := job.Spec.Template.Spec
	if ptr.Deref(spec.AutomountServiceAccountToken, true) {
		t.Error("mover pod automounts the API token")
	}

	// The broker records the mover's final snapshot, then the Job
	// completes: the rescue is done and its objects are removed.
	markFinal(t, c, "u2")
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	if err := c.Status().Update(context.Background(), &job); err != nil {
		t.Fatal(err)
	}
	reconcilePV(t, r)
	if a := getPV(t, c).Annotations[wsnapv1.AnnotationRescue]; a != RescueDone {
		t.Errorf("rescue = %q after job completion", a)
	}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: rescueClaimName("pv-1")}, &claim); err == nil {
		t.Error("rescue claim not deleted")
	}

	// The PV is released again (rescue claim gone) and gets reclaimed.
	reconcilePV(t, r)
	if p := getPV(t, c).Spec.PersistentVolumeReclaimPolicy; p != corev1.PersistentVolumeReclaimDelete {
		t.Errorf("policy %s after rescue, want Delete", p)
	}
}

func TestRescueRefusesPreexistingSameNameClaim(t *testing.T) {
	pv := releasedPV(map[string]string{workspace.AnnotationWriterPod: "u2"})
	// A namespace user made the exact claim wsnap would make. Matching
	// labels and spec are insufficient; the PV never reserved its UID.
	claim := rescueClaimFixture(pv, "attacker-uid")
	r, c := newPVReconciler(t, testWorkspace(), pv, claim)
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: pv.Name}})
	if err == nil || !strings.Contains(err.Error(), "without matching PV claim UID") {
		t.Fatalf("reconcile error = %v, want claim collision refusal", err)
	}
	got := getPV(t, c)
	if got.Spec.ClaimRef.UID != "old-uid" || got.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain {
		t.Fatalf("collision changed protected PV: %+v", got.Spec)
	}
	var jobs batchv1.JobList
	if err := c.List(context.Background(), &jobs); err != nil || len(jobs.Items) != 0 {
		t.Fatalf("collision started rescue Job: %v, %d Jobs", err, len(jobs.Items))
	}
}

// TestOlderEpochVolumeIsRescued: a newer writer exists, but the older
// volume's unsaved data is still saved. The mover Job carries the volume's
// epoch, so the broker keeps the snapshot without making it the newest
// (reviewer #3 finding 2 replaced the history-based skip).
func TestOlderEpochVolumeIsRescued(t *testing.T) {
	newer := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: "new-pod", UID: "u9"}}
	ws := testWorkspace(func(ws *wsnapv1.Workspace) {
		ws.Status.WriterPod = &wsnapv1.PodRef{Name: "new-pod", UID: "u9"}
		ws.Status.WriterEpoch = 2
		ws.Status.Volume = &wsnapv1.VolumeRef{Kind: workspace.KindPVC, ClaimName: "new", VolumeName: "pv-2"}
		ws.Status.LastSnapshot = &wsnapv1.SnapshotRecord{ID: "b1", Executor: "sidecar", Epoch: 2, Pod: &wsnapv1.PodRef{Name: "new-pod", UID: "u9"}}
	})
	r, c := newPVReconciler(t, newer, ws, releasedPV(map[string]string{
		workspace.AnnotationWriterPod: "u2", workspace.AnnotationWriterEpoch: "1"}))
	reconcilePV(t, r)
	if a := getPV(t, c).Annotations[wsnapv1.AnnotationRescue]; a != RescueRunning {
		t.Fatalf("rescue = %q, want running", a)
	}
	var job batchv1.Job
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: moverJobName("pv-1")}, &job); err != nil {
		t.Fatal(err)
	}
	if got := job.Annotations[workspace.AnnotationWriterEpoch]; got != "1" {
		t.Fatalf("mover job epoch = %q, want the volume's epoch 1", got)
	}
}

func TestRescueRunsWhenNewerWriterPodIsGone(t *testing.T) {
	// The workspace's writer on pv-2 no longer exists, so nothing can be
	// rolled back.
	ws := testWorkspace(func(ws *wsnapv1.Workspace) {
		ws.Status.WriterPod = &wsnapv1.PodRef{Name: "gone", UID: "u9"}
		ws.Status.Volume = &wsnapv1.VolumeRef{Kind: workspace.KindPVC, VolumeName: "pv-2"}
	})
	r, c := newPVReconciler(t, ws, releasedPV(map[string]string{workspace.AnnotationWriterPod: "u2"}))
	reconcilePV(t, r)
	if a := getPV(t, c).Annotations[wsnapv1.AnnotationRescue]; a != RescueRunning {
		t.Errorf("rescue = %q, want running", a)
	}
}

// staleJobCache hides Jobs, like an informer cache that has not yet seen a
// Job the controller just created.
type staleJobCache struct{ client.Client }

func (s staleJobCache) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*batchv1.Job); ok {
		return apierrors.NewNotFound(batchv1.Resource("jobs"), key.Name)
	}
	return s.Client.Get(ctx, key, obj, opts...)
}

// TestStaleCacheDoesNotUndoRescue reproduces a bug the e2e found: right
// after creating the mover Job, the cache missed it, the reconciler reset
// the rescue, and the PV stayed bound to the rescue claim forever.
func TestStaleCacheDoesNotUndoRescue(t *testing.T) {
	r, c := newPVReconciler(t, testWorkspace(), releasedPV(map[string]string{workspace.AnnotationWriterPod: "u2"}))
	reconcilePV(t, r) // starts the rescue
	r.Client = staleJobCache{c}
	reconcilePV(t, r) // the cache misses the Job; the live Reader has it
	if a := getPV(t, c).Annotations[wsnapv1.AnnotationRescue]; a != RescueRunning {
		t.Fatalf("rescue = %q after a stale cache read; it must stay running", a)
	}
	var job batchv1.Job
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: moverJobName("pv-1")}, &job)
	markFinal(t, c, "u2")
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	_ = c.Status().Update(context.Background(), &job)
	reconcilePV(t, r)
	if a := getPV(t, c).Annotations[wsnapv1.AnnotationRescue]; a != RescueDone {
		t.Fatalf("rescue = %q after the job completed", a)
	}
}

func TestMissingMoverJobIsRecreated(t *testing.T) {
	pv := releasedPV(map[string]string{workspace.AnnotationWriterPod: "u2", wsnapv1.AnnotationRescue: RescueRunning})
	pv.Spec.ClaimRef = &corev1.ObjectReference{Namespace: testNS, Name: rescueClaimName("pv-1"), UID: "rescue-claim-uid"}
	pv.Status.Phase = corev1.VolumeBound
	r, c := newPVReconciler(t, testWorkspace(), pv, rescueClaimFixture(pv, "rescue-claim-uid"))
	reconcilePV(t, r)
	var job batchv1.Job
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: moverJobName("pv-1")}, &job); err != nil {
		t.Fatalf("mover job not recreated: %v", err)
	}
	got := getPV(t, c)
	if got.Annotations[wsnapv1.AnnotationRescue] != RescueRunning || got.Spec.ClaimRef.UID != "rescue-claim-uid" {
		t.Errorf("rescue %q, claimRef %+v; want running with the bound claim untouched", got.Annotations[wsnapv1.AnnotationRescue], got.Spec.ClaimRef)
	}
}

// TestLostAnnotationHeals: a PV bound to its rescue claim but with an empty
// rescue phase (the stranded state of the old bug) still finishes.
func TestLostAnnotationHeals(t *testing.T) {
	pv := releasedPV(map[string]string{workspace.AnnotationWriterPod: "u2", wsnapv1.AnnotationRescue: "", workspace.AnnotationFinalPod: "mover"})
	pv.Spec.ClaimRef = &corev1.ObjectReference{Namespace: testNS, Name: rescueClaimName("pv-1"), UID: "c"}
	pv.Status.Phase = corev1.VolumeBound
	done := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: moverJobName("pv-1")},
		Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}}}
	r, c := newPVReconciler(t, testWorkspace(), pv, done)
	reconcilePV(t, r)
	if a := getPV(t, c).Annotations[wsnapv1.AnnotationRescue]; a != RescueDone {
		t.Fatalf("stranded PV not healed: rescue = %q", a)
	}
}

func TestReleaseProtectedVolumes(t *testing.T) {
	inUse := releasedPV(map[string]string{workspace.AnnotationWriterPod: "u1"})
	inUse.Name, inUse.Status.Phase = "in-use", corev1.VolumeBound
	safe := releasedPV(map[string]string{workspace.AnnotationWriterPod: "u1", workspace.AnnotationFinalPod: "u1"})
	safe.Name = "safe"
	unsafe := releasedPV(map[string]string{workspace.AnnotationWriterPod: "u2"})
	unsafe.Name = "unsafe"
	keepRetain := releasedPV(map[string]string{workspace.AnnotationWriterPod: "u1", workspace.AnnotationFinalPod: "u1",
		wsnapv1.AnnotationOriginalReclaimPolicy: "Retain"})
	keepRetain.Name = "was-retain"
	_, c := newPVReconciler(t, inUse, safe, unsafe, keepRetain)

	res, err := ReleaseProtectedVolumes(context.Background(), c, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Released) != 2 || res.Skipped["unsafe"] == "" || res.Skipped["in-use"] == "" {
		t.Fatalf("released %v, skipped %v; want only the PVs whose data is in a snapshot", res.Released, res.Skipped)
	}
	policy := func(name string) corev1.PersistentVolumeReclaimPolicy {
		var pv corev1.PersistentVolume
		_ = c.Get(context.Background(), types.NamespacedName{Name: name}, &pv)
		return pv.Spec.PersistentVolumeReclaimPolicy
	}
	if policy("safe") != corev1.PersistentVolumeReclaimDelete || policy("was-retain") != corev1.PersistentVolumeReclaimRetain {
		t.Errorf("original policies not restored: safe %s, was-retain %s", policy("safe"), policy("was-retain"))
	}
	if policy("in-use") != corev1.PersistentVolumeReclaimRetain {
		t.Error("an in-use PV with unsnapshotted writes was released")
	}
	if policy("unsafe") != corev1.PersistentVolumeReclaimRetain {
		t.Error("the unsnapshotted PV was released")
	}
	if res, _ := ReleaseProtectedVolumes(context.Background(), c, true); len(res.Released) != 2 {
		t.Errorf("force release = %+v", res)
	}
}

func TestJobsTrustTheBrokerCA(t *testing.T) {
	jc := JobConfig{AgentImage: "a", BrokerURL: "https://b", Audience: "wsnap", CAConfigMap: "wsnap-ca"}
	job := jc.newAgentJob("mover", "j", testWorkspace(), &wsnapv1.SnapshotRepository{}, []string{"mover"}, nil, nil)
	spec := job.Spec.Template.Spec
	found := false
	for _, v := range spec.Volumes {
		if v.ConfigMap != nil && v.ConfigMap.Name == "wsnap-ca" {
			found = true
		}
	}
	args := strings.Join(spec.Containers[0].Args, " ")
	if !found || !strings.Contains(args, "--ca-file=/var/run/wsnap-ca/ca.crt") || !strings.Contains(args, "--broker-url=https://b") {
		t.Errorf("job does not trust the broker CA: volumes %+v args %s", spec.Volumes, args)
	}
}

// TestFailedRescueIsNotRestarted (finding R8): after the failed Job's TTL
// removes it, the PV is still bound to the rescue claim; it must stay
// failed for an operator.
func TestFailedRescueIsNotRestarted(t *testing.T) {
	pv := releasedPV(map[string]string{workspace.AnnotationWriterPod: "u2", wsnapv1.AnnotationRescue: RescueFailed})
	pv.Spec.ClaimRef = &corev1.ObjectReference{Namespace: testNS, Name: rescueClaimName("pv-1"), UID: "c"}
	pv.Status.Phase = corev1.VolumeBound
	r, c := newPVReconciler(t, testWorkspace(), pv)
	reconcilePV(t, r)
	if a := getPV(t, c).Annotations[wsnapv1.AnnotationRescue]; a != RescueFailed {
		t.Fatalf("rescue = %q; a failed rescue restarted on its own", a)
	}
	var job batchv1.Job
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: moverJobName("pv-1")}, &job); err == nil {
		t.Fatal("a new mover job was created for a failed rescue")
	}
}

// staleWorkspaceCache hides Workspaces, like a cache that has not seen a
// just-created one.
type staleWorkspaceCache struct{ client.Client }

func (s staleWorkspaceCache) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*wsnapv1.Workspace); ok {
		return apierrors.NewNotFound(wsnapv1.GroupVersion.WithResource("workspaces").GroupResource(), key.Name)
	}
	return s.Client.Get(ctx, key, obj, opts...)
}

// TestStaleWorkspaceCacheDoesNotReclaim (suspicion S2).
func TestStaleWorkspaceCacheDoesNotReclaim(t *testing.T) {
	r, c := newPVReconciler(t, testWorkspace(), releasedPV(map[string]string{workspace.AnnotationWriterPod: "u2"}))
	r.Client = uidPVCClient{staleWorkspaceCache{c}}
	reconcilePV(t, r)
	if p := getPV(t, c).Spec.PersistentVolumeReclaimPolicy; p != corev1.PersistentVolumeReclaimRetain {
		t.Fatalf("PV reclaim policy %s after a stale cache read; the data would be deleted", p)
	}
}

// markFinal does what the broker does when it records a mover's snapshot.
func markFinal(t *testing.T, c client.Client, uid string) {
	t.Helper()
	pv := getPV(t, c)
	pv.Annotations[workspace.AnnotationFinalPod] = uid
	if err := c.Update(context.Background(), pv); err != nil {
		t.Fatal(err)
	}
}

// TestCompleteJobWithoutRecordedSnapshotKeepsTheVolume (reviewer #4
// finding 1): the mover's report was lost, so the Job completed but the
// broker never recorded the snapshot. The volume must be kept.
func TestCompleteJobWithoutRecordedSnapshotKeepsTheVolume(t *testing.T) {
	r, c := newPVReconciler(t, testWorkspace(), releasedPV(map[string]string{workspace.AnnotationWriterPod: "u2"}))
	reconcilePV(t, r)
	var job batchv1.Job
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: moverJobName("pv-1")}, &job)
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	_ = c.Status().Update(context.Background(), &job)
	reconcilePV(t, r)
	pv := getPV(t, c)
	if a := pv.Annotations[wsnapv1.AnnotationRescue]; a != RescueFailed || pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain {
		t.Fatalf("rescue %q, policy %s; an unrecorded rescue must keep the volume", a, pv.Spec.PersistentVolumeReclaimPolicy)
	}
}

// TestRemovingFailedAnnotationRetriesTheRescue (reviewer #6 finding 7):
// the documented retry must start a new mover, not re-read the old Job.
func TestRemovingFailedAnnotationRetriesTheRescue(t *testing.T) {
	pv := releasedPV(map[string]string{workspace.AnnotationWriterPod: "u2", wsnapv1.AnnotationRescue: ""}) // the operator removed "failed"
	pv.Spec.ClaimRef = &corev1.ObjectReference{Namespace: testNS, Name: rescueClaimName("pv-1"), UID: "c"}
	pv.Status.Phase = corev1.VolumeBound
	failed := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: moverJobName("pv-1"), UID: "old-job"},
		Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}}}}
	r, c := newPVReconciler(t, testWorkspace(), pv, rescueClaimFixture(pv, "c"), failed)
	reconcilePV(t, r) // deletes the failed Job
	reconcilePV(t, r) // creates a new one
	if a := getPV(t, c).Annotations[wsnapv1.AnnotationRescue]; a != RescueRunning {
		t.Fatalf("rescue = %q after the retry, want running", a)
	}
	var job batchv1.Job
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: moverJobName("pv-1")}, &job); err != nil || job.UID == "old-job" {
		t.Fatalf("no new mover job (err %v, uid %s)", err, job.UID)
	}
}
