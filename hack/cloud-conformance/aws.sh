#!/usr/bin/env bash
# Sets up AWS for .github/workflows/cloud-conformance.yml. Run it in AWS
# CloudShell (or anywhere the aws CLI is logged in as an IAM admin):
#
#   REPO=chrisyifanjin/wsnap REGION=us-east-1 bash aws.sh
#
# It creates, with least privilege:
#   - an S3 bucket whose conformance/ objects expire after one day;
#   - the GitHub OIDC identity provider (if missing);
#   - a CI role that only this repository's workflows can assume (it plays
#     the wsnap broker);
#   - a workspace role that the CI role assumes with a session policy, as
#     the s3-sts provider does, allowed to use only that bucket.
# It prints the repository variables to set. No key is created or stored.
set -euo pipefail

REPO=${REPO:?set REPO=owner/name}
REGION=${REGION:-us-east-1}
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
BUCKET=${BUCKET:-wsnap-conformance-$ACCOUNT-$REGION}
CI_ROLE=wsnap-conformance-ci
WS_ROLE=wsnap-conformance-workspace
OIDC=token.actions.githubusercontent.com

echo "account $ACCOUNT, region $REGION, bucket $BUCKET, repository $REPO"

if ! aws s3api head-bucket --bucket "$BUCKET" 2>/dev/null; then
	if [[ $REGION == us-east-1 ]]; then
		aws s3api create-bucket --bucket "$BUCKET" --region "$REGION" >/dev/null
	else
		aws s3api create-bucket --bucket "$BUCKET" --region "$REGION" \
			--create-bucket-configuration "LocationConstraint=$REGION" >/dev/null
	fi
fi
aws s3api put-public-access-block --bucket "$BUCKET" --public-access-block-configuration \
	BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
aws s3api put-bucket-lifecycle-configuration --bucket "$BUCKET" --lifecycle-configuration \
	'{"Rules":[{"ID":"expire-conformance","Status":"Enabled","Filter":{"Prefix":"conformance/"},"Expiration":{"Days":1}}]}'

if ! aws iam get-open-id-connect-provider \
	--open-id-connect-provider-arn "arn:aws:iam::$ACCOUNT:oidc-provider/$OIDC" >/dev/null 2>&1; then
	aws iam create-open-id-connect-provider --url "https://$OIDC" --client-id-list sts.amazonaws.com >/dev/null
fi

ci_trust=$(cat <<EOF
{"Version":"2012-10-17","Statement":[{"Effect":"Allow",
 "Principal":{"Federated":"arn:aws:iam::$ACCOUNT:oidc-provider/$OIDC"},
 "Action":"sts:AssumeRoleWithWebIdentity",
 "Condition":{"StringEquals":{"$OIDC:aud":"sts.amazonaws.com"},
              "StringLike":{"$OIDC:sub":"repo:$REPO:*"}}}]}
EOF
)
ws_trust=$(cat <<EOF
{"Version":"2012-10-17","Statement":[{"Effect":"Allow",
 "Principal":{"AWS":"arn:aws:iam::$ACCOUNT:role/$CI_ROLE"},
 "Action":"sts:AssumeRole"}]}
EOF
)
ws_policy=$(cat <<EOF
{"Version":"2012-10-17","Statement":[
 {"Effect":"Allow","Action":["s3:GetObject","s3:PutObject","s3:DeleteObject"],"Resource":"arn:aws:s3:::$BUCKET/*"},
 {"Effect":"Allow","Action":["s3:ListBucket","s3:GetBucketLocation"],"Resource":"arn:aws:s3:::$BUCKET"}]}
EOF
)
ci_policy=$(cat <<EOF
{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"sts:AssumeRole",
 "Resource":"arn:aws:iam::$ACCOUNT:role/$WS_ROLE"}]}
EOF
)

ensure_role() { # name trust-policy
	if aws iam get-role --role-name "$1" >/dev/null 2>&1; then
		aws iam update-assume-role-policy --role-name "$1" --policy-document "$2"
	else
		aws iam create-role --role-name "$1" --assume-role-policy-document "$2" >/dev/null
	fi
}
# The CI role must exist before the workspace role's trust policy names it.
ensure_role "$CI_ROLE" "$ci_trust"
ensure_role "$WS_ROLE" "$ws_trust"
aws iam put-role-policy --role-name "$CI_ROLE" --policy-name assume-workspace-role --policy-document "$ci_policy"
aws iam put-role-policy --role-name "$WS_ROLE" --policy-name conformance-bucket --policy-document "$ws_policy"

cat <<EOF

Done. Set the repository variables (from a machine with gh logged in):

gh variable set WSNAP_AWS_CI_ROLE_ARN        --repo $REPO --body arn:aws:iam::$ACCOUNT:role/$CI_ROLE
gh variable set WSNAP_AWS_WORKSPACE_ROLE_ARN --repo $REPO --body arn:aws:iam::$ACCOUNT:role/$WS_ROLE
gh variable set WSNAP_AWS_REGION             --repo $REPO --body $REGION
gh variable set WSNAP_AWS_BUCKET             --repo $REPO --body $BUCKET
EOF
