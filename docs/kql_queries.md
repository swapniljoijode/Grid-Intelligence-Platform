# KQL Monitoring Queries — Grid Intelligence Platform

Live monitoring queries for the Eventhouse KQL database (T2-5).
Run in the Eventhouse query editor or pin to the Real-Time Dashboard.

Table: `GridEvents` — schema and DDL in [infra/eventhouse_setup.kql](../infra/eventhouse_setup.kql)

---

## 1. Sanity checks

### Latest events (last 100)
```kql
GridEvents
| top 100 by timestamp desc
| project timestamp, region, source, demand_mw, carbon_intensity_gco2_kwh
```

### Event count by source
```kql
GridEvents
| where timestamp > ago(1h)
| summarize count() by source, region
```

### Data freshness — alert if silent for > 60 s
```kql
GridEvents
| summarize last_event = max(timestamp), event_count = count() by region, source
| extend staleness_seconds = datetime_diff('second', now(), last_event)
| project region, source, last_event, staleness_seconds, event_count
| order by staleness_seconds desc
```

---

## 2. Demand monitoring

### Live demand — rolling 5-minute average by region
```kql
GridEvents
| where timestamp > ago(30m) and source == "synthetic"
| summarize
    avg_demand_mw = avg(demand_mw),
    max_demand_mw = max(demand_mw),
    min_demand_mw = min(demand_mw)
  by region, bin(timestamp, 5m)
| order by timestamp desc
```

### Generation vs demand balance (net generation)
```kql
GridEvents
| where timestamp > ago(1h) and source == "synthetic"
| summarize
    avg_demand     = avg(demand_mw),
    avg_generation = avg(generation_mw)
  by region, bin(timestamp, 5m)
| extend balance_mw = avg_generation - avg_demand
| project timestamp, region, avg_demand, avg_generation, balance_mw
| order by timestamp asc
```

### Demand time series — last 24 hours (for dashboard tile)
```kql
GridEvents
| where timestamp > ago(24h) and source == "synthetic" and region == "ERCOT"
| summarize avg_demand_mw = avg(demand_mw) by bin(timestamp, 1h)
| order by timestamp asc
```

---

## 3. Carbon intensity monitoring

### GB carbon intensity — rolling 30-minute series
```kql
GridEvents
| where timestamp > ago(2h) and source == "carbonintensity.org.uk"
| project timestamp, carbon_intensity_gco2_kwh
| order by timestamp asc
```

### Carbon intensity vs synthetic demand overlay
```kql
let ci =
    GridEvents
    | where timestamp > ago(1h) and source == "carbonintensity.org.uk"
    | summarize avg_ci = avg(carbon_intensity_gco2_kwh) by bin(timestamp, 30m);
let demand =
    GridEvents
    | where timestamp > ago(1h) and source == "synthetic" and region == "ERCOT"
    | summarize avg_demand = avg(demand_mw) by bin(timestamp, 30m);
ci
| join kind=leftouter demand on timestamp
| project timestamp, avg_ci, avg_demand
| order by timestamp asc
```

### Low-carbon window identification
```kql
// Identifies periods where CI < 150 gCO2/kWh — suitable for load shifting
GridEvents
| where timestamp > ago(24h) and source == "carbonintensity.org.uk"
| summarize avg_ci = avg(carbon_intensity_gco2_kwh) by bin(timestamp, 30m)
| where avg_ci < 150
| project timestamp, avg_ci
| extend label = "low-carbon window"
| order by timestamp asc
```

---

## 4. Alerting thresholds (Activator rule queries — T2-4)

These queries are the basis for Activator alert rules. An Activator rule fires
when the query returns at least one row.

### Demand spike — ERCOT > 60 GW (alert threshold)
```kql
GridEvents
| where timestamp > ago(5m) and source == "synthetic" and region == "ERCOT"
| summarize max_demand_mw = max(demand_mw)
| where max_demand_mw > 60000
| project max_demand_mw, alert = "ERCOT demand spike"
```

### High carbon intensity — GB 30-min rolling average > 250 gCO2/kWh
```kql
GridEvents
| where timestamp > ago(30m) and source == "carbonintensity.org.uk"
| summarize avg_ci = avg(carbon_intensity_gco2_kwh)
| where avg_ci > 250
| project avg_ci, alert = "GB carbon intensity high"
```

### Producer silence — no events in last 90 s (pipeline health alert)
```kql
GridEvents
| summarize last_event = max(timestamp)
| where datetime_diff('second', now(), last_event) > 90
| project last_event, alert = "producer silence"
```

---

## 5. 5-minute aggregate view (pre-computed — faster for dashboards)

The `GridEventsFiveMin` materialized view is created in
[infra/eventhouse_setup.kql](../infra/eventhouse_setup.kql) and is
significantly faster than scanning the raw table for dashboard tiles.

```kql
GridEventsFiveMin
| where timestamp > ago(6h)
| order by timestamp desc
```

### Dashboard tile — rolling max demand (use GridEventsFiveMin)
```kql
GridEventsFiveMin
| where timestamp > ago(24h) and region == "ERCOT" and source == "synthetic"
| project timestamp, max_demand_mw
| order by timestamp asc
```

### Dashboard tile — carbon intensity trend (use GridEventsFiveMin)
```kql
GridEventsFiveMin
| where timestamp > ago(24h) and source == "carbonintensity.org.uk"
| project timestamp, avg_carbon_intensity
| order by timestamp asc
```

---

## 6. Anomaly detection (exploratory)

### Demand z-score — flag readings more than 2 SD from the hourly mean
```kql
let stats =
    GridEvents
    | where timestamp > ago(24h) and source == "synthetic"
    | summarize mean_demand = avg(demand_mw), stdev_demand = stdev(demand_mw) by region;
GridEvents
| where timestamp > ago(1h) and source == "synthetic"
| join kind=inner stats on region
| extend z_score = (demand_mw - mean_demand) / stdev_demand
| where abs(z_score) > 2
| project timestamp, region, demand_mw, z_score
| order by abs(z_score) desc
```
