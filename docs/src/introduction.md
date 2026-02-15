# Introduction

Zumblezay is an Axum-based Rust service that collects camera events, transcribes audio with Whisper-compatible backends, and serves HTML/JSON views for reviewing activity. It also provides OpenAI-compatible daily summaries and Bedrock-powered investigation queries over transcript history. It runs as a single binary (`zumblezay_server`) that wires three SQLite databases, background workers, and an HTTP UI.

Use this book as the user/operator guide. It pairs with the deeper code map in `docs/AI_AGENT_GUIDE.md`, which is aimed at LLM-powered tooling and contributors.
