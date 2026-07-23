# MergeTree Explorer

An interactive, single-file playground for learning how ClickHouse® MergeTree works on the inside — Parts, granules, sparse primary index, partitions, skip indexes, materialized views, projections, and the SharedMergeTree physical layer.

- **日本語版 (Japanese)**: https://tkykenmt.github.io/mergetree-explorer/
- English version: coming soon

## What it does

Pick a learning level (Lv1 Parts → Lv8 physical layer / SharedMergeTree) and the UI progressively unlocks concepts. Every action shows the SQL it corresponds to, a live service map animates inserts / merges / deletes, and a query pipeline shows stage-by-stage granule pruning against the Parts you actually created. Each level ships with a guided tour.

All technical facts are sourced from the official ClickHouse documentation. The sample data (a fictional SaaS clickstream) is invented.

## Development

Single self-contained HTML file (`index.html`), vanilla JS, no build step.

```bash
npm i --no-save jsdom
node test-dom.js   # DOM integration tests
```

This is an unofficial educational tool and is not affiliated with ClickHouse, Inc. ClickHouse is a registered trademark of ClickHouse, Inc.
