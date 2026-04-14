# AGENTS.md

This document provides a guide for agents working on YugabyteDB

### Deploying and running

For agents that want to deploy, configure and run YugabyteDB refer to instructions at ./docs/content/stable/quick-start

### Repo Structure

| Directory | What it contains |
|---|---|
| `src/` | Core database code: PostgreSQL fork (`src/postgres/`), YugabyteDB C++ storage engine (`src/yb/`), Odyssey connection pooler (`src/odyssey/`) |
| `java/` | Java client library, CDC connector, and DB tests |
| `managed/` | YugabyteDB Anywhere (YBA) platform — orchestration UI, CLI, node agent, and backend (Scala/Java) |
| `docs/` | Source files for the docs website (docs.yugabyte.com) |
| `python/` | Python build utilities and test infrastructure scripts |
| `build-support/` | Build system scripts, linting, and third-party dependency tooling |
| `cmake_modules/` | CMake modules for locating dependencies and custom build functions |
| `cloud/` | Docker, Kubernetes, and Grafana deployment configurations |
| `yugabyted-ui/` | Yugabyted web UI (React frontend + Go API server) |
| `architecture/` | Internal design documents and architecture specs |
| `troubleshoot/` | Troubleshooting framework backend and UI |

### Coding and Development

When working on DB code (`src/`), refer to `src/AGENTS.md` for build and test guidance

## Cursor Cloud specific instructions

### Running YugabyteDB locally

Building the core C++ database from source requires a specialized build image (`yugabyteci/yb_build_infra_almalinux9_x86_64`) and takes hours. Instead, download pre-built release binaries:

```bash
cd /tmp
wget -q https://software.yugabyte.com/releases/2025.2.2.1/yugabyte-2025.2.2.1-b1-linux-x86_64.tar.gz -O yugabyte.tar.gz
tar xzf yugabyte.tar.gz && cd yugabyte-2025.2.2.1
./bin/post_install.sh
./bin/yugabyted start --advertise_address 127.0.0.1 --daemon=true
```

After startup, connect via: `PGPASSWORD=yugabyte psql -h 127.0.0.1 -p 5433 -U yugabyte -d yugabyte`

The built-in web UI is at `http://127.0.0.1:15433/`. Stop with `./bin/yugabyted destroy`.

### Yugabyted UI development

The `yugabyted-ui/` directory contains a React frontend and Go API server. To work on the UI:

```bash
cd yugabyted-ui/ui
npm ci
npm run start          # Vite dev server on port 5173
npm run build          # Production build into ui/ directory
npm run typecheck      # TypeScript type checking (has pre-existing errors)
```

The Go API server requires the built UI assets embedded:
```bash
cd yugabyted-ui/ui && npm run build && tar cz ui | tar -C ../apiserver/cmd/server/ -xz
cd ../apiserver && go build ./cmd/server/
```

### Python build tooling

A Python venv is set up at `/workspace/.venv` with dependencies from `requirements.txt`:
```bash
source /workspace/.venv/bin/activate
```

### Key ports

| Service | Port |
|---|---|
| YSQL (PostgreSQL-compatible) | 5433 |
| YCQL (Cassandra-compatible) | 9042 |
| YB-Master web UI | 7000 |
| YB-TServer web UI | 9000 |
| Yugabyted UI | 15433 |

### Gotchas

- The `.cursor/environment.json` references a Dockerfile based on `yugabyteci/yb_build_infra_almalinux9_x86_64` which is meant for full C++ builds. On Cursor Cloud, `postgresql-client` and `python3.12-venv` need to be installed separately since we use Ubuntu.
- `go vet` and `go build` on `yugabyted-ui/apiserver` will fail if the `cmd/server/ui/` directory doesn't contain built UI assets (the Go code uses `//go:embed`). Build the UI first.
- TypeScript `typecheck` in `yugabyted-ui/ui` has pre-existing type errors in xcluster and welcome components; these are not regressions.
- The C++ lint script (`build-support/lint.sh`) requires thirdparty tools that are only available after a full C++ build.
