package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	wsnapv1 "github.com/chrisyifanjin/wsnap/api/v1alpha1"
	"github.com/chrisyifanjin/wsnap/internal/workspace"
)

// Handler is the Pod mutating webhook.
type Handler struct {
	Config  Config
	Reader  client.Reader
	Decoder admission.Decoder
}

func (h *Handler) Handle(ctx context.Context, req admission.Request) admission.Response {
	var pod corev1.Pod
	if err := h.Decoder.Decode(req, &pod); err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}
	if pod.Namespace == "" {
		pod.Namespace = req.Namespace // generateName Pods have no namespace yet
	}
	cfg := h.Config
	if name := pod.Labels[wsnapv1.LabelWorkspace]; name != "" && h.Reader != nil {
		var ws wsnapv1.Workspace
		if err := h.Reader.Get(ctx, client.ObjectKey{Namespace: pod.Namespace, Name: name}, &ws); err == nil {
			cfg.ExecutionMode = ws.Spec.ExecutionMode()
		} else if !apierrors.IsNotFound(err) {
			return admission.Errored(http.StatusServiceUnavailable, err)
		}
	}
	if req.SubResource == "ephemeralcontainers" {
		var err error
		if cfg.ExecutionMode == wsnapv1.ExecutionWorker {
			err = workspace.CheckWorkerIsolation(&pod, cfg.Audience, cfg.AllowAPIToken)
		} else {
			err = ValidateEphemeral(&pod, cfg.Audience, cfg.AllowAPIToken)
		}
		if err != nil {
			return admission.Denied("wsnap: " + err.Error())
		}
		return admission.Allowed("")
	}
	if cfg.ExecutionMode == wsnapv1.ExecutionWorker && h.Reader != nil {
		reason, err := h.unsharableClaim(ctx, &pod)
		if err != nil {
			return admission.Errored(http.StatusServiceUnavailable, err)
		}
		if reason != "" {
			return admission.Denied("wsnap: " + reason)
		}
	}
	if err := Mutate(&pod, cfg); err != nil {
		return admission.Denied("wsnap: " + err.Error())
	}
	raw, err := json.Marshal(&pod)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}
	return admission.PatchResponseFromRaw(req.Object.Raw, raw)
}

// unsharableClaim explains why a worker-mode Pod's standalone PVC cannot
// be shared: a ReadWriteOncePod claim cannot be mounted by the worker, so
// the workload would wait at its gate forever. A claim that does not exist
// yet is checked by the controller when the Pod is scheduled.
func (h *Handler) unsharableClaim(ctx context.Context, pod *corev1.Pod) (string, error) {
	volName := pod.Annotations[wsnapv1.AnnotationVolume]
	if volName == "" {
		volName = wsnapv1.DefaultVolumeName
	}
	for _, v := range pod.Spec.Volumes {
		if v.Name != volName || v.PersistentVolumeClaim == nil {
			continue
		}
		var claim corev1.PersistentVolumeClaim
		err := h.Reader.Get(ctx, client.ObjectKey{Namespace: pod.Namespace, Name: v.PersistentVolumeClaim.ClaimName}, &claim)
		if apierrors.IsNotFound(err) {
			return "", nil
		}
		if err != nil {
			return "", err
		}
		if slices.Contains(claim.Spec.AccessModes, corev1.ReadWriteOncePod) {
			return fmt.Sprintf("worker mode cannot share ReadWriteOncePod claim %s with a worker Pod", claim.Name), nil
		}
	}
	return "", nil
}
