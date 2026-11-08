#!/usr/bin/env bash
# Sets up Google Cloud for .github/workflows/cloud-conformance.yml. Run it
# in Cloud Shell (or anywhere gcloud is logged in as a project owner):
#
#   REPO=chrisyifanjin/wsnap PROJECT=my-project LOCATION=us-central1 bash gcp.sh
#
# It creates, with least privilege:
#   - a GCS bucket whose conformance/ objects are deleted after one day;
#   - a service account with objectAdmin on that bucket only (it plays the
#     wsnap broker; the gcs-downscoped provider narrows it per workspace);
#   - a workload identity pool and GitHub provider that accept only this
#     repository, allowed to act as that service account.
# It prints the repository variables to set. No key is created or stored.
set -euo pipefail

REPO=${REPO:?set REPO=owner/name}
PROJECT=${PROJECT:-$(gcloud config get-value project 2>/dev/null)}
[[ -n $PROJECT ]] || { echo "set PROJECT" >&2; exit 1; }
LOCATION=${LOCATION:-us-central1}
NUMBER=$(gcloud projects describe "$PROJECT" --format='value(projectNumber)')
BUCKET=${BUCKET:-wsnap-conformance-$PROJECT}
SA=wsnap-conformance@$PROJECT.iam.gserviceaccount.com
POOL=wsnap-github
PROVIDER=github

echo "project $PROJECT ($NUMBER), bucket $BUCKET, repository $REPO"

gcloud services enable storage.googleapis.com iam.googleapis.com iamcredentials.googleapis.com \
	sts.googleapis.com --project "$PROJECT"

if ! gcloud storage buckets describe "gs://$BUCKET" --project "$PROJECT" >/dev/null 2>&1; then
	gcloud storage buckets create "gs://$BUCKET" --project "$PROJECT" --location "$LOCATION" \
		--uniform-bucket-level-access --public-access-prevention
fi
lifecycle=$(mktemp)
echo '{"rule":[{"action":{"type":"Delete"},"condition":{"age":1,"matchesPrefix":["conformance/"]}}]}' > "$lifecycle"
gcloud storage buckets update "gs://$BUCKET" --lifecycle-file "$lifecycle"
rm -f "$lifecycle"

if ! gcloud iam service-accounts describe "$SA" --project "$PROJECT" >/dev/null 2>&1; then
	gcloud iam service-accounts create wsnap-conformance --project "$PROJECT" \
		--display-name "wsnap cloud conformance (GitHub Actions)"
fi
gcloud storage buckets add-iam-policy-binding "gs://$BUCKET" \
	--member "serviceAccount:$SA" --role roles/storage.objectAdmin >/dev/null

if ! gcloud iam workload-identity-pools describe "$POOL" --project "$PROJECT" --location global >/dev/null 2>&1; then
	gcloud iam workload-identity-pools create "$POOL" --project "$PROJECT" --location global \
		--display-name "wsnap GitHub Actions"
fi
if ! gcloud iam workload-identity-pools providers describe "$PROVIDER" --project "$PROJECT" \
	--location global --workload-identity-pool "$POOL" >/dev/null 2>&1; then
	gcloud iam workload-identity-pools providers create-oidc "$PROVIDER" --project "$PROJECT" \
		--location global --workload-identity-pool "$POOL" \
		--issuer-uri https://token.actions.githubusercontent.com \
		--attribute-mapping 'google.subject=assertion.sub,attribute.repository=assertion.repository' \
		--attribute-condition "assertion.repository == '$REPO'"
fi
gcloud iam service-accounts add-iam-policy-binding "$SA" --project "$PROJECT" \
	--role roles/iam.workloadIdentityUser \
	--member "principalSet://iam.googleapis.com/projects/$NUMBER/locations/global/workloadIdentityPools/$POOL/attribute.repository/$REPO" >/dev/null

cat <<EOF

Done. Set the repository variables (from a machine with gh logged in):

gh variable set WSNAP_GCP_WORKLOAD_IDENTITY_PROVIDER --repo $REPO --body projects/$NUMBER/locations/global/workloadIdentityPools/$POOL/providers/$PROVIDER
gh variable set WSNAP_GCP_SERVICE_ACCOUNT           --repo $REPO --body $SA
gh variable set WSNAP_GCP_BUCKET                    --repo $REPO --body $BUCKET
EOF
