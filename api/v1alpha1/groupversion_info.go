// Package v1alpha1 contains the wsnap API.
// +kubebuilder:object:generate=true
// +groupName=wsnap.dev
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	// GroupVersion is the API group and version of wsnap objects.
	GroupVersion = schema.GroupVersion{Group: "wsnap.dev", Version: "v1alpha1"}

	// SchemeBuilder registers the wsnap types with a scheme. It uses only
	// apimachinery, so importing the API pulls in no controller runtime.
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)

	// AddToScheme adds the wsnap types to a scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)

func addKnownTypes(s *runtime.Scheme) error {
	s.AddKnownTypes(GroupVersion,
		&SnapshotRepository{}, &SnapshotRepositoryList{},
		&Workspace{}, &WorkspaceList{},
		&WorkspaceSnapshot{}, &WorkspaceSnapshotList{},
		&WorkspaceRestore{}, &WorkspaceRestoreList{})
	metav1.AddToGroupVersion(s, GroupVersion)
	return nil
}
