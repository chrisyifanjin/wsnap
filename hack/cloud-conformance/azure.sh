#!/usr/bin/env bash
# Sets up Azure for .github/workflows/cloud-conformance.yml. Run it in
# Azure Cloud Shell (bash), or anywhere az is logged in with rights to
# create app registrations and role assignments:
#
#   REPO=chrisyifanjin/wsnap LOCATION=eastus bash azure.sh
#
# It creates, with least privilege:
#   - a resource group and a storage account (no public blob access) whose
#     blobs are deleted one day after their last change;
#   - an app registration whose only credential is a GitHub federated
#     credential for this repository's main branch (scheduled and manual
#     runs of the workflow);
#   - the role "Storage Account Key Operator Service Role" on that storage
#     account only: the workflow reads the account key, as the broker's
#     AZURE_ACCOUNT_KEY, and the azure-sas provider signs SAS tokens with it.
# It prints the repository variables to set. No secret is created or stored.
set -euo pipefail

REPO=${REPO:?set REPO=owner/name}
LOCATION=${LOCATION:-eastus}
GROUP=${GROUP:-wsnap-conformance}
SUBSCRIPTION=$(az account show --query id -o tsv)
TENANT=$(az account show --query tenantId -o tsv)
# Storage account names: 3-24 lowercase letters and digits, globally unique.
ACCOUNT=${ACCOUNT:-wsnapconf$(echo -n "$SUBSCRIPTION" | sha256sum | cut -c1-12)}

echo "subscription $SUBSCRIPTION, storage account $ACCOUNT, repository $REPO"

az group create --name "$GROUP" --location "$LOCATION" -o none
if ! az storage account show --name "$ACCOUNT" --resource-group "$GROUP" -o none 2>/dev/null; then
	az storage account create --name "$ACCOUNT" --resource-group "$GROUP" --location "$LOCATION" \
		--sku Standard_LRS --kind StorageV2 --allow-blob-public-access false --min-tls-version TLS1_2 -o none
fi
SCOPE=$(az storage account show --name "$ACCOUNT" --resource-group "$GROUP" --query id -o tsv)
az storage account management-policy create --account-name "$ACCOUNT" --resource-group "$GROUP" -o none --policy '{
  "rules": [{"enabled": true, "name": "expire-conformance", "type": "Lifecycle",
    "definition": {"actions": {"baseBlob": {"delete": {"daysAfterModificationGreaterThan": 1}}},
                   "filters": {"blobTypes": ["blockBlob"]}}}]}'

APP=$(az ad app list --display-name wsnap-conformance --query '[0].appId' -o tsv)
if [[ -z $APP ]]; then
	APP=$(az ad app create --display-name wsnap-conformance --query appId -o tsv)
fi
az ad sp show --id "$APP" -o none 2>/dev/null || az ad sp create --id "$APP" -o none
if ! az ad app federated-credential list --id "$APP" --query "[?name=='wsnap-main']" -o tsv | grep -q .; then
	az ad app federated-credential create --id "$APP" --parameters "{
	  \"name\": \"wsnap-main\",
	  \"issuer\": \"https://token.actions.githubusercontent.com\",
	  \"subject\": \"repo:$REPO:ref:refs/heads/main\",
	  \"audiences\": [\"api://AzureADTokenExchange\"]}" -o none
fi
az role assignment create --assignee "$APP" --role "Storage Account Key Operator Service Role" --scope "$SCOPE" -o none
# azure/login selects the subscription; a role in this resource group makes
# it visible to the app without granting anything outside the group.
az role assignment create --assignee "$APP" --role Reader \
	--scope "$(az group show --name "$GROUP" --query id -o tsv)" -o none

cat <<EOF

Done. Set the repository variables (from a machine with gh logged in):

gh variable set WSNAP_AZURE_CLIENT_ID       --repo $REPO --body $APP
gh variable set WSNAP_AZURE_TENANT_ID       --repo $REPO --body $TENANT
gh variable set WSNAP_AZURE_SUBSCRIPTION_ID --repo $REPO --body $SUBSCRIPTION
gh variable set WSNAP_AZURE_ACCOUNT         --repo $REPO --body $ACCOUNT
EOF
