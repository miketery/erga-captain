# Erga Captain

This repository contains a deliberately small, single-host implementation of the
recovery path described in `ERGA_CAPTAIN_ARCHITECTURE.md`.

It does four things:

1. accepts the standard DBOS SDK Conductor WebSocket connection and records its application version;
2. tracks `HEALTHY -> DISCONNECTED -> DEAD` in PostgreSQL;
3. asks an exact-version healthy executor whether a dead executor owns pending work;
4. tells that executor to atomically recover the dead executor's DBOS workflows.

Erga Captain stores routing state in the `erga_captain` schema. Workflow state remains
in DBOS's `dbos` schema; Erga Captain never edits DBOS workflow rows.

## Run it

The defaults target `postgres://postgres:postgres@localhost:5432/erga_captain` and
listen on `127.0.0.1:8080`:

```sh
go build -o captain .
cp .env.example .env
set -a; source .env; set +a
./captain
```

Erga Captain reads configuration from its process environment; it does not load
`.env` automatically. Keep local values in the ignored `.env` file and use
`.env.example` as the committed reference.

Useful configuration:

| Variable | Default |
| --- | --- |
| `DATABASE_URL` | `postgres://postgres:postgres@localhost:5432/erga_captain?sslmode=disable` |
| `CAPTAIN_SCHEMA` | `erga_captain` |
| `CAPTAIN_ADDR` | `127.0.0.1:8080` |
| `CAPTAIN_KEY` | `local-dev-key` |
| `EXECUTOR_TIMEOUT` | `2s` |
| `RECOVERY_SWEEP_PERIOD` | `500ms` |
| `REQUEST_TIMEOUT` | `5s` |

Mesa at `/opt/mesa` now accepts these additional settings:

```sh
DBOS_CONDUCTOR_URL=ws://127.0.0.1:8080
DBOS_CONDUCTOR_KEY=local-dev-key
DBOS_EXECUTOR_ID=worker-a
DBOS_SYSTEM_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/erga_captain
DISPATCH_ENABLED=false
```

The `DBOS_CONDUCTOR_*` settings are names defined by the DBOS SDK and remain unchanged
for worker compatibility.

DBOS generates the live executor ID when its Conductor-compatible connection is
configured. The configured `DBOS_EXECUTOR_ID` is retained as executor metadata so
demo processes remain easy to identify. Every replica must use the same application
name, version, DBOS database, and queue configuration. `DISPATCH_ENABLED=false` only
isolates the failover demo from Mesa's Archie outbox polling.

## API

```text
GET    /healthz
GET    /v1/executors
GET    /v1/workflows/{workflow_id}
DELETE /v1/workflows/{workflow_id}
GET    /websocket/{application}/{captain_key}
```

The HTTP workflow operations are translated to the standard SDK `get_workflow` and
`cancel` WebSocket messages. The experiment workflow is created independently through
the public `DBOSClient.enqueue` API:

```sh
DBOS_SYSTEM_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/erga_captain \
  /opt/mesa/.venv/bin/python /opt/mesa/scripts/enqueue_conductor_sleep.py \
  failover-demo 300
```

Query the workflow to identify its SDK-generated owning executor. SIGKILL that process,
wait slightly more than `EXECUTOR_TIMEOUT`, then inspect it again:

```sh
curl http://127.0.0.1:8080/v1/workflows/failover-demo
```

Its standard `output.ExecutorID` field should now identify the surviving worker.
`scripts/failover_demo.sh` automates the complete experiment, including safe
cancellation and process cleanup.

## Verified failover

The live experiment performed while building this implementation observed:

```text
DBOS SDK worker-a owns failover-demo-1788140050
worker-a receives SIGKILL
the SDK WebSocket closes abnormally
2-second disconnect grace expires
worker-b's DBOS SDK reports pending work for worker-a/version 1
worker-b's DBOS SDK accepts recovery
DBOS reports PENDING on worker-b and recovery_attempts=2
```

The proof workflow was then cancelled and all demo processes were stopped.

## Intentional limits

This prototype auto-registers an application on first executor connection and checks
one static development key. It implements the DBOS 2.30 SDK messages needed for
registration, known-dead recovery, workflow lookup, and cancellation. It omits full
application/key management, multiple Captain hosts, remote dispatch, host reaping,
and unknown-executor scanning.
