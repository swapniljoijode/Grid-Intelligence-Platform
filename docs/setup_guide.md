# Setup Guide — Grid Intelligence Platform

Step-by-step replication guide covering every manual action performed outside the codebase.
Follow in order — each part depends on the previous one.

---

## Prerequisites

| Requirement | Notes |
|---|---|
| Azure account | Free account works; trial subscription is sufficient |
| Microsoft Fabric trial | Activate at `app.fabric.microsoft.com` — 60-day limit, cannot be reactivated |
| GitHub account | Fork the repository before starting |
| Docker Desktop | Required to run the streaming producer locally |
| Azure CLI | `az` — optional but faster than the portal for SP creation |
| Python 3.11+ | For running loaders and tests locally |

---

## Part 1: External API Keys

### EIA Open Data API (free)

1. Go to `https://www.eia.gov/opendata/register.php`
2. Enter your name and email address
3. Click **Register**
4. Check your inbox — the 32-character API key arrives in the confirmation email immediately
5. Save the key — you will add it to Key Vault in Part 2

---

## Part 2: Azure Infrastructure

### 2-1. Resource Group

**Portal:** Azure portal → **Resource groups → Create**

| Field | Value |
|---|---|
| Subscription | Your Azure subscription |
| Resource group name | `gip-rg` |
| Region | Same region as your Fabric trial capacity (check: Fabric workspace → Settings → License info) |

Click **Review + create → Create**.

### 2-2. Azure Key Vault

**Portal:** Azure portal → **Key vaults → Create**

| Field | Value |
|---|---|
| Subscription | Your subscription |
| Resource group | `gip-rg` |
| Key vault name | `gip-kv-<initials>` (must be globally unique, e.g. `gip-kv-sj`) |
| Region | Same as resource group |
| Pricing tier | Standard |
| Permission model | **Vault access policy** |

Click **Review + create → Create** (takes ~30 seconds).

### 2-3. Store the EIA API key as a secret

1. Open the Key Vault → left nav → **Secrets → + Generate/Import**
2. Upload options: **Manual**
3. Name: `eia-api-key`
4. Secret value: your EIA API key from Part 1
5. Click **Create**

Repeat for the Event Hub connection string after completing Part 3-3:

| Secret name | Value |
|---|---|
| `eia-api-key` | EIA 32-char API key |
| `eventhub-connection-string` | Full SAS connection string from Eventstream Custom App source |

### 2-4. Create Service Principal

The service principal lets GitHub Actions authenticate to Azure (Key Vault, Fabric REST API).

**Option A — Azure portal (no CLI)**

1. Azure portal → **Microsoft Entra ID → App registrations → New registration**
2. Name: `gip-github-actions`
3. Supported account types: **Accounts in this organizational directory only**
4. Click **Register**
5. On the app overview page, copy:
   - **Application (client) ID** → save as `AZURE_CLIENT_ID`
   - **Directory (tenant) ID** → save as `AZURE_TENANT_ID`
6. Left nav → **Certificates & secrets → New client secret**
7. Description: `github-actions` | Expiry: 12 months | click **Add**
8. Copy the **Value** immediately (hidden after you leave) → save as `AZURE_CLIENT_SECRET`

**Option B — Azure CLI (prints all three values at once)**

```bash
az ad sp create-for-rbac --name "gip-github-actions"
# Output: appId = AZURE_CLIENT_ID, password = AZURE_CLIENT_SECRET, tenant = AZURE_TENANT_ID
```

### 2-5. Grant the Service Principal Key Vault access

**Portal:** Key Vault → **Access policies → + Create**

- Secret permissions: tick **Get** and **List**
- Principal: search for `gip-github-actions` → select it
- Click **Create**

### 2-6. Add GitHub Actions secrets and variables

In your GitHub repository → **Settings → Secrets and variables → Actions**

**Repository secrets** (encrypted):

| Name | Value |
|---|---|
| `AZURE_TENANT_ID` | Directory (tenant) ID |
| `AZURE_CLIENT_ID` | Application (client) ID |
| `AZURE_CLIENT_SECRET` | Client secret value |

**Repository variables** (plain text):

| Name | Value |
|---|---|
| `FABRIC_WORKSPACE_ID` | GUID from Fabric workspace URL: `/groups/<guid>/...` |

### 2-7. Populate local .env

```bash
cp .env.example .env
```

Fill in all values from the sections above. The `.env` file is gitignored — never commit it.

---

## Part 3: Microsoft Fabric

### 3-1. Activate trial capacity

1. Go to `https://app.fabric.microsoft.com`
2. Sign in with your Microsoft account
3. Click **Start trial** when prompted
4. Note the region (e.g. East US) and the expiry date (60 days)
5. Note: the workspace region must match the trial capacity region

**Fix region mismatch:** If you see "Unable to create item because trial capacity is not in the same region":
- Workspace → **Settings → License info → Trial capacity** → select your trial capacity

### 3-2. Create the Fabric workspace

1. Fabric portal → left nav → **Workspaces → New workspace**
2. Name: `Grid Intelligence Platform`
3. Advanced → License: select your **Trial** capacity
4. Click **Apply**

Record the workspace GUID from the URL: `app.fabric.microsoft.com/groups/<GUID>/...`
Add this as `FABRIC_WORKSPACE_ID` in GitHub variables and `.env`.

### 3-3. Create Eventhouse and run DDL

**Create Eventhouse:**
1. In the workspace → **New → Eventhouse**
2. Name: `grid-intelligence-eventhouse`
3. Click **Create**
4. Inside the Eventhouse, a KQL database is created automatically — note its name

**Run the DDL:**
1. Open the KQL database → click **Query** (or open the query editor)
2. Paste the full contents of [infra/eventhouse_setup.kql](../infra/eventhouse_setup.kql)
3. Run all sections in order:
   - Creates `GridEvents` table
   - Creates `GridEventsMapping` JSON ingestion mapping
   - Sets 90-day retention policy
   - Creates `GridEventsFiveMin` materialized view (with backfill)
4. Verify: run `.show tables` — `GridEvents` should appear

### 3-4. Create Eventstream and connect to Eventhouse

**Create Eventstream:**
1. Workspace → **New → Eventstream**
2. Name: `grid-intelligence-eventstream`
3. Click **Create**

**Add Custom App source:**
1. In the Eventstream canvas → **Add source → Custom App**
2. Fabric generates a connection string and hub name
3. Click on the **CustomEndpoint-Source** node → **Details** tab → **SAS Key Authentication**
4. Copy the full connection string (format: `Endpoint=sb://...;SharedAccessKey=...;EntityPath=<hub-name>`)
5. Add to `.env`:
   ```
   EVENTHUB_CONNECTION_STRING=<full connection string>
   EVENTHUB_NAME=<value of EntityPath= in the connection string>
   ```
6. Also store the connection string in Key Vault as `eventhub-connection-string`

**Add Eventhouse destination:**
1. Canvas → **Add destination → KQL Database**
2. Select your workspace and Eventhouse KQL database
3. Table: `GridEvents` | Mapping: `GridEventsMapping` | Format: JSON
4. Click **Save**

**Publish:**
- Click **Publish** in the top bar — the stream turns green when active

**Verify producer sends data:**
```bash
# Test locally first (no Event Hub needed)
docker compose run --rm producer python -m streaming.producer --local --count 5

# Then run in Event Hub mode
docker compose up producer
```

After ~30 seconds, run in Eventhouse query editor:
```kql
GridEvents | count
```
Row count should be non-zero.

### 3-5. Build Real-Time Dashboard

1. Workspace → **New → Real-Time Dashboard**
2. Name: `Grid Intelligence — Live`
3. Click **Create**

**Connect data source:**
- Top bar → **Add data source → KQL Database** → select your Eventhouse database

**Add tiles** (paste KQL from [docs/kql_queries.md](kql_queries.md)):

| Tile | Query | Visual |
|---|---|---|
| Demand time series | §2 "Demand time series — last 24 hours" | Line chart |
| Generation vs demand | §2 "Generation vs demand balance" | Area chart |
| GB carbon intensity | §3 "GB carbon intensity — rolling 30-minute series" | Line chart |
| Data freshness | §1 "Data freshness" | Table |
| Event count by source | §1 "Event count by source" | Stat card |
| 5-min aggregates | §5 `GridEventsFiveMin` tile | Line chart |

**Settings:** Auto-refresh → **30 seconds** → **Save**

### 3-6. Configure Activator Alert Rules

**Create Activator item:**
1. Workspace → **New → Activator** (may appear as **Reflex**)
2. Name: `grid-intelligence-activator`
3. Click **Create**

**Connect data source from within Activator:**
1. Inside the Activator item → **Get data**
2. Select **`grid-intelligence-eventstream-stream`** from the Real-Time Hub tab
3. Serialization format: **JSON**
4. Click **Next → Connect**

> **Note:** If connecting via Eventstream canvas → Add destination → Activator returns a 400 error,
> open the Activator item directly and pull from Eventstream using "Get data" instead.
> Start the producer (`docker compose up producer`) before connecting — Activator needs live data to detect the schema.

**Rule 1 — ERCOT Demand Spike:**

| Setting | Value |
|---|---|
| Name | `ERCOT Demand Spike` |
| Column | `demand_mw` |
| Operation | **Increases above** |
| Value | `60000` |
| Action | Email → `your-email@example.com` |

**Rule 2 — GB High Carbon Intensity:**

| Setting | Value |
|---|---|
| Name | `GB High Carbon Intensity` |
| Column | `carbon_intensity_gco2_kwh` |
| Operation | **Increases above** |
| Value | `250` |
| Action | Email → `your-email@example.com` |

Toggle both rules to **Active**.

**Test the alerts:**
Temporarily lower Rule 1's threshold to `35000` (synthetic ERCOT data runs 36k–43k MW), start the producer, confirm the email arrives, then restore to `60000`.

```bash
docker compose up producer
```

---

## Part 4: Local Development

### 4-1. Python environment

```bash
git clone https://github.com/swapniljoijode/Grid-Intelligence-Platform.git
cd Grid-Intelligence-Platform
pip install -r requirements.txt
pre-commit install
cp .env.example .env   # populate with your values
```

### 4-2. Run unit tests

```bash
pytest tests/ -v
# Expected: all tests pass (T1 loaders + T2 producer + health module)
```

### 4-3. Run a loader manually

```bash
# EIA demand (replace key with your actual EIA API key)
python -c "
from ingestion.eia import fetch_demand
rows = fetch_demand(start='2024-01-01T00', end='2024-01-01T06', api_key='YOUR_KEY')
print(f'{len(rows)} records fetched')
print(rows[0])
"
```

### 4-4. Run the producer

```bash
# Local mode — NDJSON to stdout, no Event Hub needed
docker compose run --rm producer python -m streaming.producer --local --count 10

# Event Hub mode — requires EVENTHUB_CONNECTION_STRING in .env
docker compose up producer

# Stop cleanly
docker compose down
```

### 4-5. Health check (while producer is running)

```bash
curl http://localhost:8080/health    # → "ok"
curl http://localhost:8080/metrics   # → JSON with events_sent, errors, last_event_time
```

---

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `403 Forbidden` from EIA API | Wrong or placeholder API key | Replace `YOUR_KEY` with actual key from EIA email |
| `CBS Token authentication failed` | Wrong Event Hub connection string | Re-copy from Eventstream → CustomEndpoint-Source → SAS Key Authentication; ensure `EVENTHUB_NAME` matches `EntityPath=` in the string |
| `DataNotAvailable 400` in Activator | No live data in stream | Start producer first, then connect Activator |
| `Workspace region mismatch` in Fabric | Workspace not linked to trial capacity | Workspace Settings → License info → select Trial capacity |
| `Cannot read subscriptionId` in Key Vault portal | Portal UI bug | Hard-refresh page or use Azure CLI `az keyvault secret set` |
