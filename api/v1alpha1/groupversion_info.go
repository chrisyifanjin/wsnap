// Package v1alpha1 contains the wsnap API.
// +kubebuilder:object:generate=true
// +groupName=wsnap.dev
package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

var (
	// GroupVersion is the API group and version of wsnap objects.
	GroupVersion = schema.GroupVersion{Group: "wsnap.dev", Version: "v1alpha1"}

	// SchemeBuilder registers the wsnap types with a scheme.
	SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}

	// AddToScheme adds the wsnap types to a scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)
