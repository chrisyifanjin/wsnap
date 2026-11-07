package webhook

import (
	"context"
	"encoding/json"
	"net/http"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// Handler is the Pod mutating webhook.
type Handler struct {
	Config  Config
	Decoder admission.Decoder
}

func (h *Handler) Handle(_ context.Context, req admission.Request) admission.Response {
	var pod corev1.Pod
	if err := h.Decoder.Decode(req, &pod); err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}
	if pod.Namespace == "" {
		pod.Namespace = req.Namespace // generateName Pods have no namespace yet
	}
	if req.SubResource == "ephemeralcontainers" {
		if err := ValidateEphemeral(&pod, h.Config.Audience, h.Config.AllowAPIToken); err != nil {
			return admission.Denied("wsnap: " + err.Error())
		}
		return admission.Allowed("")
	}
	if err := Mutate(&pod, h.Config); err != nil {
		return admission.Denied("wsnap: " + err.Error())
	}
	raw, err := json.Marshal(&pod)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}
	return admission.PatchResponseFromRaw(req.Object.Raw, raw)
}
