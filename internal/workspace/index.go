package workspace

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	wsnapv1 "github.com/chrisyifanjin/wsnap/api/v1alpha1"
)

// Cache field indexes. Lookups by workspace then cost O(matches) instead of
// a scan of every request or every protected PV.
const (
	IndexWorkspaceName = "spec.workspaceName"
	IndexWorkspaceRef  = "metadata.annotations.workspaceRef"
)

// Indexers lists every index with its extractor, for the manager's field
// indexer and for fake clients in tests.
func Indexers() []struct {
	Obj     client.Object
	Field   string
	Extract client.IndexerFunc
} {
	return []struct {
		Obj     client.Object
		Field   string
		Extract client.IndexerFunc
	}{
		{&wsnapv1.WorkspaceSnapshot{}, IndexWorkspaceName, func(o client.Object) []string {
			return []string{o.(*wsnapv1.WorkspaceSnapshot).Spec.WorkspaceName}
		}},
		{&wsnapv1.WorkspaceRestore{}, IndexWorkspaceName, func(o client.Object) []string {
			return []string{o.(*wsnapv1.WorkspaceRestore).Spec.WorkspaceName}
		}},
		{&corev1.PersistentVolume{}, IndexWorkspaceRef, func(o client.Object) []string {
			if ref := o.GetAnnotations()[wsnapv1.AnnotationWorkspaceRef]; ref != "" {
				return []string{ref}
			}
			return nil
		}},
	}
}

// RegisterIndexes adds the indexes to a manager's field indexer.
func RegisterIndexes(ctx context.Context, fi client.FieldIndexer) error {
	for _, ix := range Indexers() {
		if err := fi.IndexField(ctx, ix.Obj, ix.Field, ix.Extract); err != nil {
			return err
		}
	}
	return nil
}
