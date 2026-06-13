# Key Vault Setup — T0-5

Azure Key Vault stores all runtime secrets. The Fabric workspace managed identity
is granted **Key Vault Secrets User** — no credentials are ever hardcoded or
committed to the repository.

## Provision

```bash
# Replace <suffix> with a short unique slug, e.g. gip-kv-sj01
az keyvault create \
  --name gip-kv-<suffix> \
  --resource-group <rg-name> \
  --location eastus \
  --sku standard \
  --enable-purge-protection false    # safe for dev; enable for prod

# Grant the Fabric workspace managed identity Secrets User role
az role assignment create \
  --role "Key Vault Secrets User" \
  --assignee <fabric-managed-identity-object-id> \
  --scope /subscriptions/<sub>/resourceGroups/<rg>/providers/Microsoft.KeyVault/vaults/gip-kv-<suffix>
```

## Secrets inventory

| Secret name | Description | Rotation |
|-------------|-------------|---------|
| `eia-api-key` | EIA Open Data API v2 free key | On compromise |
| `eventhub-connection-string` | Event Hub producer SAS connection string | 90 days |
| `fabric-server` | Fabric Warehouse ODBC hostname | On workspace change |
| `azure-tenant-id` | Tenant ID for local dev service principal | Static |
| `azure-client-id` | Client ID for local dev service principal | On rotation |
| `azure-client-secret` | Client secret for local dev service principal | 90 days |

## Local development

For local dev, copy `.env.example` to `.env` and populate. The `azure-identity`
SDK picks up credentials via `DefaultAzureCredential` (env vars → Azure CLI →
managed identity chain).

```python
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

credential = DefaultAzureCredential()
client = SecretClient(vault_url=os.environ["KEY_VAULT_URI"], credential=credential)
eia_key = client.get_secret("eia-api-key").value
```
