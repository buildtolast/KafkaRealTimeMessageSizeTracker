# Memory

## Docker + Rust Build Lessons
- OrbStack can crash during long Docker builds if buildx cache is corrupted. Fix: `docker buildx prune -f`
- `rdkafka` with `cmake-build` feature compiles librdkafka from C source inside Docker — takes ~3 min but works
- Debian Bookworm's `librdkafka-dev` is v1.9.x, but rdkafka-sys 4.10.0 needs >=2.12.1. Dynamic linking won't work with apt packages
- Solution: Use Cargo features (`static-kafka` = cmake-build for Docker, `dynamic-kafka` = dynamic-linking for systems with newer librdkafka)
- Rust Docker image version must match crate MSRV. `time` crate v0.3.47 needs rustc 1.88+
- `rust:1.88-bookworm` is the minimum viable Docker image for current crate ecosystem (as of Feb 2026)

## KafkaDashboard Project
- Location: ~/GIT/KafkaDashboard
- Rust backend (actix-web + rdkafka) + React frontend (react-mosaic + Vite)
- `docker compose up -d` runs full stack: Kafka + Dashboard + Seed producer
- Dashboard at http://localhost:3001, Kafka external at localhost:9094

## KafkaRealTimeMessageSizeTracker Project
- Location: ~/GIT/KafkaRealTimeMessageSizeTracker — see [memory-KafkaRealTimeMessageSizeTracker.md](memory-KafkaRealTimeMessageSizeTracker.md) for full details
- Real-time Kafka message size monitoring dashboard with alerts, analytics, and MongoDB persistence
- Rust backend (actix-web 4 + rdkafka 0.37 + mongodb 3) + embedded single-file HTML frontend (~2400 lines)
- Edition 2024, Docker: rust:1.88-bookworm multi-stage build
- `docker compose -f docker-compose.full.yml up -d` runs Kafka (KRaft) + App + seed producer + MongoDB 7
- Dev server: `cargo run -- --brokers localhost:9092 --port 5688`
- Dashboard at http://localhost:5688
- **70 tests**: config(2) + models(20) + stats(13) + alerts(14) + storage(11) + handlers(1) + browser(5) + mongo(4)
- **3 themes**: Terminal/Chalk (default), Tokyo Night, Dracula — theme switcher + localStorage
- Features: real-time size tracking, oversized message alerts with payload download, 7-day history, historical message browser (by offset/timestamp/preset), global dashboard snapshot (time-travel), topic selection picker, broker discovery, cluster insights (lag/throughput/recommendations), MongoDB persistence for oversized messages and alerts, persisted data browser, runtime-mutable alert thresholds (global defaults + per-topic), mini timeseries charts with switchable time ranges (live/1h/6h/24h/7d)
- Layout: resizable panels — drag-to-resize sidebar + panel rows/columns, priority-sorted topic cards with pulse animations

## Memory & Plan File Convention
- Memory files: `memory-ProjectName.md` — live in `.claude/projects/.../memory/` AND copied into each project's git repo at `memory/`
- Plan files: `plan-ProjectName.md` — created next to memory files
- When memory is updated in `.claude`, also update the copy in the git repo
- **Before every commit**: Always update memory files, context (MEMORY.md), and README first, then commit and push

## MongoAssist Project (formerly MongoChatBot)
- Location: ~/GIT/MongoChatBot — see [memory-MongoChatBot.md](memory-MongoChatBot.md) for full details
- Rust CLI + web app: interactive MongoDB document manager (find, browse, edit, update, rollback, backup)
- App name: **MongoAssist** — subtitle: "Interactive MongoDB Document Manager"
- Dependencies: actix-web, mongodb 3, serde, rustyline, clap, colored, tokio, thiserror
- Edition 2024, Docker: rust:1.88-bookworm multi-stage build
- `docker compose up -d` runs Mongo:7 (port 27099) + Web UI (port 3000)
- Dev server: `cargo run -- --web --port 4000` (requires `--web` flag for web mode)
- First-time walkthrough tour (localStorage: `mongoassist_walkthrough_seen`)
- **209 tests**: 189 unit + 20 integration (all pass as of Feb 2026)
- **11 themes**: Glass Terminal (default), Tokyo Night, Apple, Light, Dracula, Solarized Dark, Nord, Monokai Pro, Catppuccin Mocha, Gruvbox Dark, High Contrast
- Frontend is a single ~6400-line index.html embedded via `include_str!`
