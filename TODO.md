# TODO — KafkaRealTimeMessageSizeTracker

## High Impact

- [ ] **Message Schema/Format Analysis** — Auto-detect message formats (JSON, Avro, Protobuf, plain text) and show format distribution per topic. For JSON messages, show a collapsible schema tree with field frequency/types. Useful for schema drift detection.

- [ ] **Alerting Webhooks / Notifications** — Add Slack, Discord, or generic webhook integration for alert notifications. Currently alerts only appear in the UI — adding `--webhook-url` or a webhook config panel would make it production-ready for unattended monitoring.

- [ ] **Message Compression Analysis** — Show compression ratio per topic (compare compressed wire size vs decompressed payload size). rdkafka exposes this metadata. Helps identify topics that would benefit from compression or where compression is underperforming.

- [ ] **Export / Reporting** — CSV/JSON export for stats, alerts, and time-series data. Could also add a downloadable snapshot of the health report for sharing with teams.

- [ ] **Multi-Cluster Support** — Allow connecting to multiple Kafka clusters simultaneously with a cluster switcher in the UI. Each cluster gets its own set of consumers, stats, and alerts.

## Medium Impact

- [ ] **Consumer Group Monitoring** — Beyond own consumers, show all consumer groups on the cluster with their lag, state (stable/rebalancing/dead), and member assignments.

- [ ] **Message Rate Anomaly Detection** — Beyond size-based alerts, add throughput anomaly detection (sudden drops, spikes, or flat-lines using rolling standard deviation). Flag scenarios like "topic X hasn't received messages in 5 minutes" or "throughput spike 3σ above normal."

- [ ] **Topic Configuration Viewer** — Show topic-level configs (retention, cleanup policy, min.insync.replicas, compression type, etc.) in the cluster insights overlay via `describe_configs` from the admin API.

- [ ] **Keyboard Shortcuts** — Power-user navigation: `1-5` to switch panels, `j/k` to scroll topics, `h` for health report, `c` for cluster insights, `Esc` to close overlays, `/` to search topics.

- [ ] **Dark/Light Mode Auto-Detection** — Respect `prefers-color-scheme` media query to auto-select theme on first visit.

## Polish / Nice-to-Have

- [ ] **Sparkline Mini-Charts in Sidebar** — Tiny inline sparklines next to each topic in the sidebar showing the last 60 seconds of message rate or avg size.

- [ ] **Sound Alerts** — Optional audio notification on critical alerts (Web Audio API beep), toggleable from the UI.

- [ ] **Bookmark Offsets** — Let users bookmark specific partition/offset positions for later reference, stored in localStorage.

- [ ] **Diff Mode for Messages** — Compare two messages side-by-side from the message browser, highlighting field differences. Useful for debugging schema changes.

- [ ] **REST API Docs (OpenAPI)** — Auto-generate OpenAPI/Swagger spec from actix routes, served at `/api/docs`.
