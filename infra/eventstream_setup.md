# Eventstream, Real-Time Dashboard & Activator Setup

**Phase T2-2 · T2-3 · T2-4 | Fabric portal — no code required**

These steps wire the producer output through Eventstream into Eventhouse,
build the Real-Time Dashboard, and configure the Activator threshold alerts.
Complete them after the Eventhouse DDL in [eventhouse_setup.kql](eventhouse_setup.kql)
has been run.

---

## T2-2 — Eventstream → Eventhouse

### 1. Create the Eventstream

1. In your Fabric workspace, click **New → Eventstream**.
2. Name it `grid-intelligence-eventstream`.
3. Click **Create**.

### 2. Add the source

Two options depending on your setup:

**Option A — Custom App (HTTP endpoint) — simplest for local dev:**
1. In the Eventstream canvas, click **Add source → Custom App**.
2. Fabric generates an **HTTP endpoint URL** and a **Shared Access Signature key**.
3. Copy both values into your `.env`:
   ```
   EVENTHUB_CONNECTION_STRING=Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=<key>
   EVENTHUB_NAME=<eventhub-name>
   ```
4. The producer uses `azure-eventhub` SDK which is compatible with this SAS endpoint.

**Option B — Azure Event Hub (if you provisioned one separately):**
1. Click **Add source → Azure Event Hub**.
2. Create a new connection using the namespace and hub name from your `.env`.

### 3. Add destinations

**Destination 1 — Eventhouse (KQL)**

1. Click **Add destination → KQL Database**.
2. Select your workspace and Eventhouse KQL database.
3. Set **Table** to `GridEvents`.
4. Set **Mapping** to `GridEventsMapping`.
5. Leave ingestion format as **JSON**.
6. Click **Save**.

**Destination 2 — Real-Time Dashboard** *(wire this after the dashboard is created in T2-3)*

1. Click **Add destination → Real-Time Dashboard**.
2. Select the dashboard you create in the next step.

### 4. Activate the Eventstream

Click **Publish** in the top bar. The stream turns green when data flows.

### 5. Verify with the producer

```bash
# Start the producer in local mode to verify the event schema first
docker compose run --rm producer python -m streaming.producer --local --count 5

# Then start in Event Hub mode (requires .env with EVENTHUB_CONNECTION_STRING)
docker compose up producer
```

In the Eventstream canvas, the preview pane should show events arriving within 30 s.

---

## T2-3 — Real-Time Dashboard

### 1. Create the dashboard

1. In the workspace, click **New → Real-Time Dashboard**.
2. Name it `Grid Intelligence — Live`.
3. Click **Create**.

### 2. Connect to Eventhouse

1. Click **Add data source** in the top bar.
2. Select **KQL Database**, then choose your Eventhouse database.
3. The KQL queries in [docs/kql_queries.md](../docs/kql_queries.md) are ready to paste.

### 3. Add tiles

Paste each KQL query from [docs/kql_queries.md](../docs/kql_queries.md) as a new tile.
Recommended tile layout:

| Tile | Query section | Visual |
|------|--------------|--------|
| Demand time series | § 2 — Live demand 5-min average | Line chart |
| Generation vs demand balance | § 2 — Net generation | Area chart |
| GB carbon intensity | § 3 — Rolling 30-min series | Line chart |
| Low-carbon window | § 3 — Low-carbon window | Column chart |
| Data freshness | § 1 — Freshness | Table |
| Event count by source | § 1 — Event count | Stat card |

### 4. Set auto-refresh

In the dashboard settings, set **Auto refresh** to `30 seconds`.

---

## T2-4 — Activator Alert Rules

Activator (formerly Reflex) watches a data stream and fires an action when a
threshold is crossed. Two alerts are configured here.

### 1. Create the Activator item

1. In the workspace, click **New → Activator** (may appear as **Reflex** in some tenants).
2. Name it `grid-intelligence-activator`.

### 2. Connect to the Eventstream

1. In the Activator canvas, click **Add data source**.
2. Choose **Eventstream** and select `grid-intelligence-eventstream`.
3. The fields from `GridEvent` appear as available properties.

### 3. Rule 1 — ERCOT demand spike

| Setting | Value |
|---------|-------|
| Name | `ERCOT Demand Spike` |
| Condition | `demand_mw` **>** `60000` |
| Filter | `region` = `ERCOT` AND `source` = `synthetic` |
| Action | Send email to `swapniljoijode22@gmail.com` |
| Subject | `ALERT: ERCOT demand spike — {demand_mw:.0f} MW` |

### 4. Rule 2 — GB high carbon intensity

| Setting | Value |
|---------|-------|
| Name | `GB High Carbon Intensity` |
| Condition | `carbon_intensity_gco2_kwh` **>** `250` |
| Filter | `source` = `carbonintensity.org.uk` |
| Action | Send email to `swapniljoijode22@gmail.com` |
| Subject | `ALERT: GB carbon intensity high — {carbon_intensity_gco2_kwh:.0f} gCO₂/kWh` |

### 5. Activate

Toggle both rules to **Active**. Activator evaluates the stream continuously.

To test without waiting, temporarily lower the demand threshold to `35000`
(well within normal synthetic range), confirm an email arrives, then restore to `60000`.

---

## Verification checklist

- [ ] Eventstream shows data flowing (green indicator in canvas)
- [ ] `GridEvents | count` in Eventhouse returns rows
- [ ] `GridEventsFiveMin | count` returns rows (materialized view backfill complete)
- [ ] Real-Time Dashboard tiles show live data with < 30 s refresh
- [ ] Activator sends a test alert email
- [ ] KQL queries from [docs/kql_queries.md](../docs/kql_queries.md) run without errors
