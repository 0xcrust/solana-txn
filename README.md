## What Is This?

A full, batteries-included implementation of a highly scalable transaction-sending pipeline for Solana.

> This implementation is part of a larger project implementing bot services, with this component open-sourced to support scalable transaction pipelines.

## Features

- Provides a unified interface for maintaining a view of network state via gRPC streaming, RPC polling, or both
- Allows fine-grained combination of multiple cluster endpoints with different sources or levels of commitment
- Highly performant, asynchronous transaction-sending service
- Sends transactions to RPC or Jito block engine over RPC or gRPC
- Sends bundles to Jito over RPC or gRPC
- Supports transaction confirmation via Solana RPC/gRPC and bundle confirmation via Jito RPC
- Supports retries with custom priority fee and tip increments per retry
- Tracks detailed metrics for collecting landing statistics

## Roadmap

- Track leader schedule and send transactions directly to leader TPU

## Dependencies(and inclusions)

- `graceful-core`: Unified transaction type and traits
- `graceful-tx-logs`: Fast and complete log-parsing implementation
