# Grid Intelligence Platform

**End-to-End Data Engineering on Microsoft Fabric**

*Prepared by Swapnil Sanjay Joijode | Version 1.0 | June 2026*

---

## Contents

1. [Charter and Outcome](#1-charter-and-outcome)
2. [Objectives and Business Questions](#2-objectives-and-business-questions)
3. [Operating Model and the Free Constraint](#3-operating-model-and-the-free-constraint)
4. [Scope Decision](#4-scope-decision)
5. [Architecture](#5-architecture)
6. [Data Sources](#6-data-sources)
7. [Tools Added Beyond the Original Brief](#7-tools-added-beyond-the-original-brief)
8. [Technology and Library Inventory](#8-technology-and-library-inventory)
9. [Phase Plan](#9-phase-plan)
10. [Version Control and Tracker Integration](#10-version-control-and-tracker-integration)
11. [Transformation Engine Decision](#11-transformation-engine-decision)
12. [Risks and Mitigations](#12-risks-and-mitigations)
13. [Definition of Done and Resume Packaging](#13-definition-of-done-and-resume-packaging)
14. [Decommission and Cost-Stop Plan](#14-decommission-and-cost-stop-plan)

---

## 1. Charter and Outcome

The Grid Intelligence Platform is a production-shaped data engineering build on Microsoft Fabric that ingests, models, governs, and serves electricity grid data across both batch and real-time paths. It mirrors a full corporate data engineering lifecycle and exists as the flagship evidence artifact for a Data Strategy and Analytics Leadership profile.

The platform analyses electricity demand, generation mix, and carbon intensity, with weather as the demand driver. The US grid, specifically ERCOT (the Texas balancing authority), anchors the batch and hourly path. The Great Britain grid anchors the near real-time path. A synthetic telemetry producer supplies high-frequency events for the streaming mechanics.

Outcome target: a reproducible repository, a recorded live walkthrough, a governed semantic model with dashboards, and a theory companion, all of which persist at zero ongoing cost after the Fabric capacity is paused.

---

## 2. Objectives and Business Questions

The platform is framed around the questions a decision maker would ask, not the methods used to answer them.

- How does electricity demand relate to generation mix across the day, and how does it shift with weather?
- Which fuel sources carry peak load, and what is the renewable versus fossil share at any hour?
- How does carbon intensity move through the day, and can a low-carbon window be identified for load shifting?
- What are the demand patterns and anomalies on the ERCOT grid, and where do they deviate from forecast?
- Can the platform detect and alert on a grid event (a demand spike or a carbon intensity threshold) in near real time?
- Stretch: can short-horizon demand or carbon intensity be forecast from weather and historical load?

Each question maps to a gold-layer model and a dashboard element. No metric reaches the dashboard without a calculation definition agreed before development, per the project's planning discipline.

---

## 3. Operating Model and the Free Constraint

Microsoft Fabric is not free beyond a trial. The trial capacity runs sixty days, starts at F4 and can be raised to F64 for the window, and Fabric items become inaccessible once it ends unless capacity is purchased. A Power BI Pro license alone does not keep Fabric items alive. A tenant may activate at most five trial capacities, and canceled trials cannot be reactivated.

**Operating principle.** The resume value lives in artifacts that survive after capacity is paused: the GitHub repository, the code, the dbt project and its generated docs, the architecture diagrams, this plan, the theory companion, and a recorded walkthrough. The live Fabric demonstration is treated as a single sprint inside one clean trial window. Everything is built portable and version-controlled first, demonstrated on Fabric second, recorded third, paused fourth.

Every supporting tool outside Fabric is open source or free-tier. No paid tool, subscription, or platform enters this project. The free inventory:

- Azure Key Vault: effectively free at this secret volume.
- dbt-core and the dbt-fabric adapter: open source.
- Great Expectations or Soda Core, pytest, ruff, black, sqlfluff, pre-commit: open source.
- Docker and docker-compose: free.
- GitHub and GitHub Actions: free tier sufficient for this scope.
- Mermaid, draw.io, Excalidraw for diagrams; MkDocs with GitHub Pages for hosting docs: free.
- All data sources (EIA, Carbon Intensity, Open-Meteo): free.

---

## 4. Scope Decision

The committed scope is a complete spine that finishes, not a broad surface that stalls. A flagship that completes a full lifecycle outperforms one that half-builds many features. Simplicity is applied to the project itself, not only to the dashboards.

**Committed: the spine.** Phases T0 through T8 below: foundation and governance baseline, batch ingestion, real-time ingestion and alerting, transformation and dimensional modelling, governance and security, orchestration, CI/CD and containerisation, analytics and visualisation, and documentation with packaging.

**Fenced stretch.** Short-horizon forecasting of demand and carbon intensity using already-ingested data, the Carbon Intensity forecast endpoint, and weather features. Executed only if the spine completes with trial time remaining, and delivered as a notebook artifact that does not depend on live capacity, so it survives regardless.

**Cut, deliberately.** An external data catalog (OpenMetadata or DataHub). Fabric already provides lineage and the Purview hub. Self-hosting a heavy external catalog adds operational effort without proportional resume payoff and invites a credibility question about tool sprawl. Lineage and a business glossary are delivered natively instead.

---

## 5. Architecture

Two ingestion paths converge on OneLake. The batch path pulls EIA, Carbon Intensity history, and weather through Data Factory pipelines and notebooks into the bronze Lakehouse, raw and partitioned. The real-time path runs a containerized producer that emits synthetic telemetry and polled API readings into an Eventstream, which routes to an Eventhouse (KQL database) for live storage and to a Real-Time Dashboard, with an Activator watching thresholds. Spark notebooks promote bronze to a cleansed, conformed silver. dbt builds the gold star schema in the Warehouse. A DirectLake semantic model serves Power BI. Governance, lineage, quality, security, secrets, and cost monitoring wrap the whole.

### Capability to tool mapping

| Capability | Primary (Fabric) | Supporting (free) |
|---|---|---|
| Batch ingestion | Data Factory, Dataflows Gen2 | Python (requests, tenacity) |
| Real-time ingestion | Eventstream (HTTP/Event Hub) | Dockerised Python producer |
| Storage and medallion | OneLake, Lakehouse, Warehouse | Delta, Parquet |
| Real-time storage | Eventhouse (KQL) | KQL |
| Transformation | Spark notebooks (silver) | dbt-core + dbt-fabric (gold) |
| Data quality | dbt tests, source freshness | Great Expectations or Soda |
| Modelling | Warehouse star schema, semantic model | dbt, pydantic contracts |
| Orchestration | Data Factory pipelines | Airflow in Docker (portable) |
| Governance and security | Purview hub, labels, RLS, OneLake security | Key Vault for secrets |
| Lineage | Fabric lineage view, Purview | dbt docs lineage graph |
| Documentation | dbt docs site | Companion doc, MkDocs, GitHub Pages |
| Analytics and viz | Power BI DirectLake, Real-Time Dashboard, Activator | Branded to design philosophy |
| Containerisation | Not applicable (SaaS) | Docker for producer and dbt runtime only |
| CI/CD | Deployment Pipelines | GitHub Actions |
| Version control | Fabric Git integration | GitHub, pre-commit, semantic tags |

---

## 6. Data Sources

| Source | Path | Auth | Cadence | Provides |
|---|---|---|---|---|
| EIA Open Data API v2 | Batch + hourly | Free key | Hourly; bulk twice daily | Demand and generation by fuel and balancing authority, including ERCOT |
| UK Carbon Intensity API | Near real-time | None | Half-hourly + forecast | Carbon intensity, index, generation mix, national and regional |
| Open-Meteo | Batch + forecast | None | Hourly | Temperature and weather features (demand driver) |
| Synthetic producer | Real-time | n/a | Sub-minute | Simulated smart-meter and substation telemetry |

**Endpoints.** EIA: `https://api.eia.gov/v2/` &nbsp; Carbon Intensity: `https://api.carbonintensity.org.uk/intensity` &nbsp; Open-Meteo: `https://api.open-meteo.com/v1/forecast`

**Honesty note.** No public energy feed streams sub-second telemetry for free. The high-frequency stream is simulated, which is standard practice for load tests and demonstrations, and is labelled as such throughout. The EIA limit of 5000 observations per request is handled by offset pagination in the ingestion code.

---

## 7. Tools Added Beyond the Original Brief

Gaps in the original brief, each justified and each free.

- **Secrets management (Azure Key Vault).** An EIA API key will be held. Hardcoding it is a resume red flag. Key Vault referenced by managed identity is the senior pattern.
- **Data quality framework (Great Expectations or Soda Core).** Defect reduction is a documented strength. A flagship with no explicit quality gate undersells the actual edge. Runs as a blocking gate in CI.
- **Code quality (pre-commit, sqlfluff, ruff, black).** Enforces consistent naming and formatting mechanically on every commit.
- **Data contracts and schema evolution (pydantic).** Validate payloads at ingest and handle drift deliberately. A senior topic most candidates skip.
- **Observability and cost monitoring (Monitoring Hub, Capacity Metrics app).** Watching capacity consumption prevents a surprise bill and signals the FinOps awareness expected on a leadership track.
- **Unit testing (pytest).** Tests on the producer and utility code signal rigor alongside dbt tests.
- **Semantic layer with DirectLake.** Queries Delta directly without import or DirectQuery tradeoffs; a Fabric differentiator worth featuring.
- **Runbook and decommission plan.** How to operate it, and how to pause cleanly and stop costs before the trial ends.

---

## 8. Technology and Library Inventory

**Fabric components.** OneLake, Lakehouse, Warehouse, Data Factory pipelines, Dataflows Gen2, Spark notebooks, Eventstream, Eventhouse (KQL), Real-Time Dashboard, Activator, Power BI semantic model (DirectLake), Purview hub, Deployment Pipelines, Git integration, Monitoring Hub, Capacity Metrics app.

**Python.** requests, tenacity, pydantic, python-dotenv, pandas, pyspark, azure-identity, azure-keyvault-secrets, azure-eventhub, great-expectations (or soda-core), pytest, ruff, black, pre-commit.

**Transformation.** dbt-core, dbt-fabric (Warehouse), optionally dbt-fabricspark (Lakehouse). SQL linting with sqlfluff.

**Orchestration.** Fabric Data Factory (primary), optional Apache Airflow in Docker (portable variant).

**Containers and CI/CD.** Docker, docker-compose; GitHub Actions.

**Docs and diagrams.** dbt docs, Mermaid, MkDocs with GitHub Pages.

---

## 9. Phase Plan

Governance, version control, quality, and cost monitoring are cross-cutting. They are established in T0 and matured in dedicated phases, never bolted on at the end. Each phase below maps to the tracker task manifest.

### T0. Foundation and Governance Baseline

**Target window.** Days 1 to 5

**Objective.** Stand up the workspace, environments, repository, and the controls that everything else depends on.

**Key activities.**

- Activate the trial capacity and record the expiry date.
- Create dev, test, and prod via Deployment Pipelines.
- Connect Fabric Git integration to GitHub; create the repo skeleton and naming conventions.
- Provision Key Vault; document secrets handling; add `.env.example`.
- Draw the architecture diagram; seed the theory companion; define the data contract.
- Enable the Capacity Metrics app; define the business questions before touching data.

**Deliverables.** Repo skeleton, Deployment Pipelines, Key Vault, architecture diagram, tracker manifest.

**Theory added to companion.** Medallion architecture; OneLake, Lakehouse vs Warehouse; secrets management.

### T1. Batch Ingestion to Bronze

**Target window.** Days 6 to 12

**Objective.** Land raw EIA, Carbon Intensity history, and weather in bronze, partitioned and idempotent.

**Key activities.**

- Build Data Factory pipelines and notebooks for each source.
- Pull secrets from Key Vault; never hardcode the EIA key.
- Implement watermarking, offset pagination, and idempotent incremental loads.
- Validate payloads against pydantic contracts at ingress.

**Deliverables.** Bronze Lakehouse populated and partitioned; ingestion notebooks; contract validation.

**Theory added to companion.** Delta and Parquet; partitioning; idempotency and watermarking; data contracts.

### T2. Real-Time Ingestion and Alerting

**Target window.** Days 10 to 16

**Objective.** Stand up the streaming path end to end and prove a working alert.

**Key activities.**

- Build the Dockerised producer (synthetic telemetry plus polled readings).
- Create the Eventstream; route to Eventhouse and a Real-Time Dashboard.
- Configure an Activator alert on a carbon intensity or demand threshold.
- Document KQL queries for live monitoring.

**Deliverables.** Live stream, Eventhouse, Real-Time Dashboard, one working Activator alert.

**Theory added to companion.** KQL vs T-SQL; streaming delivery semantics; Eventhouse retention.

### T3. Transformation and Dimensional Modelling

**Target window.** Days 14 to 24

**Objective.** Promote bronze to silver, then build a tested gold star schema.

**Key activities.**

- Spark notebooks for cleansing, conforming, dedup, and slowly changing dimensions.
- dbt-fabric models for the gold star schema; stored procedures only where set-based maintenance suits T-SQL.
- Configure materializations; respect Fabric constraints (no indexes, no nested CTEs in materialized models).
- Quality gates via dbt tests and Great Expectations.

**Deliverables.** Conformed silver, gold star schema, passing quality gates, generated dbt docs.

**Theory added to companion.** Star vs snowflake; SCD; materialized vs non-materialized views; stored procedures; dbt materializations; indexing and the Fabric contrast.

### T4. Governance, Lineage, Quality, and Security

**Target window.** Days 22 to 30

**Objective.** Wrap the platform in governance and least-privilege access.

**Key activities.**

- Purview hub: sensitivity labels and a business glossary.
- Row-Level Security in the Warehouse and semantic model; OneLake security.
- dbt source freshness; observability via the Monitoring Hub.
- Enforce data contracts at layer transitions.

**Deliverables.** Labels, glossary, RLS, freshness checks, lineage view.

**Theory added to companion.** Row-Level Security and sensitivity labels; data quality gates; lineage.

### T5. Orchestration

**Target window.** Days 28 to 34

**Objective.** Coordinate batch and stream into a scheduled, observable whole.

**Key activities.**

- End-to-end scheduling and dependency management in Data Factory.
- Coordinate batch loads with the streaming path.
- Retries, failure alerting, and run logging.
- Airflow in Docker prepared as the portable variant.

**Deliverables.** Orchestrated end-to-end runs with retries and alerting.

**Theory added to companion.** Scheduling and dependencies; at-least-once vs effectively-once; DAG design.

### T6. CI/CD, Version Control, and Containerisation

**Target window.** Days 32 to 40

**Objective.** Make the build reproducible, gated, and automated.

**Key activities.**

- GitHub Actions: sqlfluff and ruff, dbt build and test, Great Expectations checks, then deploy.
- pre-commit hooks including secret scanning.
- Docker images for the producer and the dbt or quality runtime.
- Branching strategy, conventional commits, semantic version tags; Deployment Pipelines promotion.

**Deliverables.** Green CI pipeline, pre-commit hooks, Docker images, tagged releases.

**Theory added to companion.** CI/CD gates; pre-commit; containerisation boundaries in a SaaS stack.

### T7. Analytics and Visualisation

**Target window.** Days 38 to 44

**Objective.** Serve the business questions with a clean, branded model and dashboards.

**Key activities.**

- Power BI semantic model on DirectLake.
- One executive dashboard: branded, KPI-led, no clutter, no duplicate charts.
- The live Real-Time Dashboard for the streaming path.
- Validate every visual against a defined business question.

**Deliverables.** DirectLake semantic model, executive dashboard, live dashboard.

**Theory added to companion.** DirectLake vs Import vs DirectQuery; semantic modelling; dashboard design.

### T8. Documentation, Packaging, and Decommission

**Target window.** Days 42 to 48

**Objective.** Convert the build into durable evidence and stop costs.

**Key activities.**

- Finalise the theory companion, data dictionary, README, and runbook.
- Record the walkthrough; capture dashboard and Eventhouse screenshots.
- Draft CAR-structured, quantified resume bullets and LinkedIn content.
- Export OneLake data and dbt artifacts; pause or delete the trial capacity.

**Deliverables.** Complete documentation, recorded demo, resume assets, cost-stop executed.

**Theory added to companion.** Documentation structure; reproducibility; the decommission discipline.

Windows overlap deliberately, because streaming and batch can progress in parallel and modelling can begin before all sources are complete. The numbering is the dependency order, not a strict calendar.

---

## 10. Version Control and Tracker Integration

The tracker reads a versioned `tracker_tasks.yaml` as the single source of truth and upserts it idempotently through a GitHub Actions sync on file change. A command-line tool emits start, done, and fail events as work moves. Stable task identifiers must never be renumbered, because history depends on them.

Conventions for this repository, aligned to that contract:

- Repository skeleton: ingestion, streaming, transform (spark and dbt), quality, orchestration, infra, docs, tests, and `.github/workflows`.
- Protected main; feature branches; pull requests gated on a green CI pipeline.
- Conventional commit messages tagged by phase identifier (for example `feat(T1): add EIA bronze loader`).
- A `tracker_tasks.yaml` shipped in the repository, keyed T0 through T8, with stable task identifiers.
- Status emitted via the tracker CLI or API as each task moves to start, done, or fail.
- Semantic version tags per phase so releases reflect milestone completion.

**Accompanying file.** A ready-to-commit `tracker_tasks.yaml` is delivered alongside this plan, matching the tracker schema exactly.

---

## 11. Transformation Engine Decision

**Decision.** Local dbt-core with the Microsoft-maintained dbt-fabric adapter, targeting the Fabric Warehouse, executed in CI through GitHub Actions inside a container.

Reasons:

- Free and open source.
- Portable: the models, manifest, generated docs, and lineage live in the repository and survive trial expiry.
- Industry standard and interview-credible.
- The native Fabric dbt job is preview, recompiles fresh on each run with no build caching, and ties execution to capacity.

**Install.** `pip install dbt-fabric` with ODBC Driver 18; on Debian or Ubuntu install the ODBC header files first.

**Fabric Warehouse constraints that shape the models.** No traditional indexes (any index passed to the adapter is ignored), no nested CTEs in materialized models, table is the default materialization, and merge and microbatch incremental strategies are available. The native dbt job is documented in the companion so it can be discussed in interviews, even though the build uses dbt-core.

---

## 12. Risks and Mitigations

| Risk | Mitigation |
|---|---|
| Trial expiry locks Fabric items | Build portable first; record the walkthrough; export OneLake data and dbt artifacts before pausing; treat the live demo as one timed sprint. |
| Scope creep dilutes the flagship | Committed spine only; stretch fenced and non-blocking; external catalog cut. |
| Simulated stream credibility | Label it clearly; pair with real polled API data; explain the load-test rationale. |
| Vendor lock-in perception | Keep PySpark, dbt, SQL, Airflow, Docker, and GitHub Actions central and visible. |
| Preview-feature instability | Prefer generally available components for the spine; isolate preview features to optional notes. |
| Secret leakage | Key Vault for all secrets; commit only `.env.example`; secret scanning in pre-commit and CI; rotate if leaked. |
| EIA query limits | Offset pagination; incremental watermarking; schedule within rate limits. |

---

## 13. Definition of Done and Resume Packaging

Done means the spine runs end to end, batch and stream both land, gold models pass the quality gates, governance and security are in place, CI is green, the dashboard answers the business questions, and the walkthrough is recorded.

Evidence that becomes resume and interview assets:

- The repository link, architecture diagram, dashboard screenshots, and the recorded demo.
- The theory companion, demonstrating depth beyond the build.
- CAR-structured, quantified bullets drafted in T8 (volume processed, quality metrics, alerting latency).
- LinkedIn content signalling Fabric plus a transferable stack to UK and Ireland recruiters open to sponsorship.

---

## 14. Decommission and Cost-Stop Plan

- Export bronze, silver, and gold from OneLake to the repository or local store.
- Export the dbt docs as a static site; commit it.
- Capture an Eventhouse sample and all dashboard screenshots.
- Record the full walkthrough.
- Pause or delete the trial capacity before day sixty.
- Confirm no paid capacity is attached to the workspace.
- Verify all artifacts are committed to GitHub.
