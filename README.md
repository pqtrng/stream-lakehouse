# The Real-Time Viral Content and Safety Hub

A live-streaming platform focused on user engagement, AI productionization, and cost efficiency.

## Goal

Demonstrate capabilities through a real-time streaming platform that processes millions of events per second, detects viral moments and toxic behavior instantly, and optimizes cloud spending.

## The Scenario

A live-streaming platform where millions of **Gifts** and **Chat Messages** are sent per second. The system must detect **Viral Moments** and **Toxic Behavior** in real-time.

## Tech Stack (The "GCP Power" Combo)

| Layer | Technology |
|-------|------------|
| **Ingestion** | GCP Pub/Sub |
| **Processing** | Dataflow (Apache Beam) — single pipeline for streaming (toxic chat detection) and batch (daily revenue report) |
| **AI Integration** | Vertex AI — toxicity scoring for chat messages in real-time within the Dataflow pipeline |
| **Storage** | BigQuery — with Partitioning and Clustering for cost optimization |

## The "Senior" Features (Stand-outs)

- **FinOps Auditor** — Streamlit dashboard tracking Cost-per-Query in BigQuery
- **Event-Driven Action** — When AI detects a Viral Moment (e.g., 500% spike in gifts), trigger a Cloud Function to send a mock notification to a "Live Ops" Slack channel
- **Modern Formats** — Long-term logs stored in Apache Iceberg format on GCS

---

## The "Guardian" Agile Roadmap

### Sprint 1: The Walking Skeleton (Connectivity)

**Goal:** Establish end-to-end data flow from source to storage.

| Task | Description |
|------|-------------|
| 1.1 | Setup GCP Infrastructure (Pub/Sub, BigQuery) using the console for speed initially |
| 1.2 | Run the Python Mock Producer to stream chat and gift events to Pub/Sub |
| 1.3 | Deploy a basic Dataflow job to move raw JSON from Pub/Sub into a BigQuery "landing" table |
| 1.4 | Verify data integrity with simple SQL counts |

### Sprint 2: The Intelligence Layer (Logic & AI)

**Goal:** Add real-time "thinking" capabilities to the data stream.

| Task | Description |
|------|-------------|
| 2.1 | Refactor to a custom Apache Beam script |
| 2.2 | Integrate Vertex AI to score chat messages for toxicity in real-time |
| 2.3 | Implement Stateful Rate-Limiting to detect "Gift Spam" or bot behavior |
| 2.4 | Route "Alerts" and "Normal Data" to separate BigQuery tables using Side Outputs |

### Sprint 3: The Governance Layer (Security & FinOps)

**Goal:** Harden the system for production and optimize cloud costs.

| Task | Description |
|------|-------------|
| 3.1 | Enforce Data Contracts using the Pub/Sub Schema Registry (Protobuf) |
| 3.2 | Optimize BigQuery with Partitioning and Clustering to minimize query costs |
| 3.3 | Create a BigQuery Search Index on the chat log column for lightning-fast keyword lookups |
| 3.4 | Build a Looker Studio dashboard tracking "Cost-per-Million-Events" |

### Sprint 4: The Product Layer (API & Caching)

**Goal:** Make the data accessible to other services via an API.

| Task | Description |
|------|-------------|
| 4.1 | Build a FastAPI service to serve real-time safety scores |
| 4.2 | Add a Redis (Memorystore) caching layer to the API to avoid redundant BigQuery hits |
| 4.3 | Implement a WebSocket endpoint for "Instant Push Notifications" of viral spikes |
| 4.4 | Containerize the API using Docker |

### Sprint 5: The Orchestration Layer (Kubernetes & DevOps)

**Goal:** Ensure the system is resilient and scales automatically.

| Task | Description |
|------|-------------|
| 5.1 | Deploy the API and Data Producer to Google Kubernetes Engine (GKE) |
| 5.2 | Setup Horizontal Pod Autoscaling (HPA) to handle viral traffic spikes |
| 5.3 | Automate the entire infrastructure setup using Terraform |
| 5.4 | Create a GitHub Action for CI/CD (automated testing and deployment to GKE) |
