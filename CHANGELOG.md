# Changelog

All notable changes to GSE Terminal are documented here.

## [Unreleased]

### Added
- Opt-in, authenticated Model Context Protocol (MCP) endpoint at `POST /mcp`, enabled with `MCP_ENABLED=true`.
- Read-only MCP tools for latest quotes, bounded price history, market movers, market briefings, and Pro/Admin technical indicators.
- MCP request validation, protocol negotiation, rate limiting, and audit-log entries.
- Tests covering MCP protocol handling, authentication boundaries, tool validation, feature-flag configuration, and regressions.

### Changed
- Quote calculation is shared between the public API and MCP tools to keep output consistent.
- QuestDB now supports bounded recent OHLC retrieval for MCP history requests.
