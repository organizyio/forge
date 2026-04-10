# Changelog

All notable changes to this project are documented in this file.

Each release lists changes since the previous git tag (conventional-commit style groupings).

## [v0.0.6] - 2026-04-10

### Changed
- Maintenance and minor improvements.


## [v0.0.5] - 2026-04-10

### Fixed
- fix(release): rebase and push branch before tags


## [v0.0.3] - 2026-04-10

### Fixed
- fix(release): ignore go/v tags when resolving root release version
- fix(release): use semver in Go proxy module lookup


## [v0.0.2] - 2026-04-10

### Fixed
- fix(release): publish and query go submodule tags correctly


## [v0.0.1] - 2026-04-10

### Added
- feat: initial Organizyio Forge framework (Go/Rust IPC, CI, releases)

### Fixed
- fix(go): address CI misspell and shutdown timing assertions
- fix(go): return after t.Skipf in minimalWorkerBinary helper
- fix(go): satisfy golangci revive and US misspell
- fix(ci): Linux Unix socket name type and golangci for Go 1.26
