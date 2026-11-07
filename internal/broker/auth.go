package broker

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	authnv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// Identity is the Pod a bound service account token belongs to.
type Identity struct {
	Namespace      string
	ServiceAccount string
	PodName        string
	PodUID         string
}

// ErrAuthUnavailable marks an authentication that could not run, as
// opposed to a token that was rejected. Callers answer it with 503, which
// agents retry, and never cache it.
var ErrAuthUnavailable = errors.New("authentication unavailable")

// Authenticator validates agent tokens.
type Authenticator interface {
	Authenticate(ctx context.Context, token string) (Identity, error)
}

// Keys the API server puts in TokenReview status.user.extra for tokens
// bound to a Pod. See
// https://kubernetes.io/docs/reference/access-authn-authz/service-accounts-admin/.
const (
	extraPodName = "authentication.kubernetes.io/pod-name"
	extraPodUID  = "authentication.kubernetes.io/pod-uid"
)

// TokenReviewAuthenticator asks the API server to validate a projected
// service account token for one audience. Tokens for other audiences, such
// as the default token Kubernetes mounts for the API server, fail the
// review, so only containers that mount a wsnap-audience token can reach
// the broker.
type TokenReviewAuthenticator struct {
	Client   kubernetes.Interface
	Audience string
}

func (a TokenReviewAuthenticator) Authenticate(ctx context.Context, token string) (Identity, error) {
	review, err := a.Client.AuthenticationV1().TokenReviews().Create(ctx, &authnv1.TokenReview{
		Spec: authnv1.TokenReviewSpec{Token: token, Audiences: []string{a.Audience}},
	}, metav1.CreateOptions{})
	if err != nil {
		// The API server did not answer; this says nothing about the token.
		return Identity{}, fmt.Errorf("%w: token review: %w", ErrAuthUnavailable, err)
	}
	if !review.Status.Authenticated {
		return Identity{}, fmt.Errorf("token rejected: %s", review.Status.Error)
	}
	// The API server returns the requested audiences the token is valid
	// for; an authenticator that ignores audiences returns none.
	if !slices.Contains(review.Status.Audiences, a.Audience) {
		return Identity{}, fmt.Errorf("token is not valid for audience %q", a.Audience)
	}
	return identityFromUser(review.Status.User)
}

func identityFromUser(u authnv1.UserInfo) (Identity, error) {
	// Service account usernames look like system:serviceaccount:<ns>:<name>.
	parts := strings.Split(u.Username, ":")
	if len(parts) != 4 || parts[0] != "system" || parts[1] != "serviceaccount" {
		return Identity{}, fmt.Errorf("token does not belong to a service account")
	}
	id := Identity{Namespace: parts[2], ServiceAccount: parts[3]}
	if v := u.Extra[extraPodName]; len(v) == 1 {
		id.PodName = v[0]
	}
	if v := u.Extra[extraPodUID]; len(v) == 1 {
		id.PodUID = v[0]
	}
	if id.PodName == "" || id.PodUID == "" {
		return Identity{}, fmt.Errorf("token is not bound to a pod; mount a projected service account token")
	}
	return id, nil
}
