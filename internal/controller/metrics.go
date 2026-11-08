package controller

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	wsnapv1 "github.com/chrisyifanjin/wsnap/api/v1alpha1"
	"github.com/chrisyifanjin/wsnap/internal/version"
)

// StateCollector exports cluster-wide health gauges, computed from the
// informer cache at scrape time. Labels stay bounded: no workspace or Pod
// names, so the series count does not grow with the number of workspaces.
// Every replica serves them; alert on max() across replicas.
type StateCollector struct {
	Reader client.Reader
}

var (
	failingDesc = prometheus.NewDesc("wsnap_workspaces_failing",
		"Workspaces whose last snapshot, restore or maintenance failed (status.lastError is set).", nil, nil)
	rescueDesc = prometheus.NewDesc("wsnap_volumes_rescue",
		"Protected PVs by rescue phase: running (a mover Job saves it) or failed (kept for an operator).", []string{"phase"}, nil)
	buildDesc                = prometheus.NewDesc("wsnap_build_info", "Always 1; the label is the build version.", []string{"version"}, nil)
	rescueRequestsFailedDesc = prometheus.NewDesc("wsnap_rescue_requests_failed",
		"Failed snapshot requests for a stopped writer's volume (wsnap-unsaved-*): its data may be in no snapshot. Stays until the request is deleted.", nil, nil)
	supersededDesc = prometheus.NewDesc("wsnap_workspaces_with_superseded_snapshots",
		"Workspaces whose status.supersededSnapshots is not empty: an older writer's data was saved after newer history and is not what restores use.", nil, nil)
)

func (c StateCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- failingDesc
	ch <- rescueDesc
	ch <- buildDesc
	ch <- rescueRequestsFailedDesc
	ch <- supersededDesc
}

func (c StateCollector) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(buildDesc, prometheus.GaugeValue, 1, version.Version)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var wss wsnapv1.WorkspaceList
	if err := c.Reader.List(ctx, &wss); err == nil {
		failing, superseded := 0, 0
		for _, ws := range wss.Items {
			if ws.Status.LastError != "" {
				failing++
			}
			if len(ws.Status.SupersededSnapshots) > 0 {
				superseded++
			}
		}
		ch <- prometheus.MustNewConstMetric(failingDesc, prometheus.GaugeValue, float64(failing))
		ch <- prometheus.MustNewConstMetric(supersededDesc, prometheus.GaugeValue, float64(superseded))
	}
	var reqs wsnapv1.WorkspaceSnapshotList
	if err := c.Reader.List(ctx, &reqs, client.MatchingLabels{wsnapv1.LabelManagedBy: wsnapv1.ManagedByValue}); err == nil {
		failed := 0
		for _, r := range reqs.Items {
			if r.Annotations[wsnapv1.AnnotationVolumeName] != "" && r.Status.Phase == wsnapv1.SnapshotFailed {
				failed++
			}
		}
		ch <- prometheus.MustNewConstMetric(rescueRequestsFailedDesc, prometheus.GaugeValue, float64(failed))
	}
	var pvs corev1.PersistentVolumeList
	if err := c.Reader.List(ctx, &pvs, client.MatchingLabels{wsnapv1.LabelProtected: "true"}); err == nil {
		n := map[string]int{RescueRunning: 0, RescueFailed: 0}
		for _, pv := range pvs.Items {
			if p := pv.Annotations[wsnapv1.AnnotationRescue]; p == RescueRunning || p == RescueFailed {
				n[p]++
			}
		}
		for phase, v := range n {
			ch <- prometheus.MustNewConstMetric(rescueDesc, prometheus.GaugeValue, float64(v), phase)
		}
	}
}
