# ai-systems-projections

Read models of ai-systems. projections reads the domain events from NATS
JetStream, keeps read model documents in MongoDB, and writes a notice for
each change to the stream `VIEWS` through an outbox (ADR 0010).
api-gateway reads the documents over HTTP. socket-gateway sends the notices
to the app. Architecture: `ARCHITECTURE.md` sections 2, 7, 10 and 12 in
`ai-systems-docs`.

```
ORCHESTRATION --durable "projections", ordered--> handler
                                                     | one MongoDB transaction:
                                                     |   tasks document (rev, asOf)
                                                     |   notice in events (the outbox)
                                                     |   processed marker
                                                     v
                         outbox relay --> VIEWS (view.tasks) --> socket-gateway --> app
api-gateway --GET /internal/tasks--> tasks + headSeq (STREAM.INFO VIEWS)
```

M1 slice: the `tasks` read model from `orchestration.TaskSubmitted`.

## Read model `tasks`

Database `projections` (`MONGODB_DATABASE`), one document for each task:

| Field       | Value                                                        |
| ----------- | ------------------------------------------------------------ |
| `_id`       | the task id                                                  |
| `title`     | the text, trimmed, the first 80 characters, trimmed again    |
| `status`    | `submitted` (M1)                                             |
| `createdAt` | `occurredAt` of the first event (a MongoDB date)             |
| `updatedAt` | `occurredAt` of the last event that changed the document     |
| `rev`       | 1 for a new document, plus 1 for each change                 |
| `asOf`      | `version` of the last source event that changed the document |

The dates come from the events, not from the clock, so a rebuild gives the
same documents. `asOf` is the checkpoint of the source stream: the handler
ignores an event with a `version` that is not higher than `asOf`. In M1
each source stream gives one document, so the document holds the
checkpoint and there is no `checkpoints` collection yet (ARCHITECTURE 7).

The pure function `project(current, event)` in `src/tasks.ts` gives the
new document and its notice. The live consumer and the rebuild use it.

## Notices on `VIEWS`

The notice outbox is the kit event store in the same database:
collections `events`, `counters` and `commands` (the kit names, in place of
`outbox_views` in ARCHITECTURE 7). The kit relay publishes each notice to
the subject in its `type`, with `Nats-Msg-Id` = the notice `eventId`. A
message is a `ViewMessageV1` of contracts v0.1.1:

```
{ eventId, type: "view.tasks", schemaVersion: 1,
  streamId: "tasks-<taskId>", version, occurredAt,
  correlationId: <source correlationId>,
  causationId:   <source eventId>,
  actor: { kind: "system", id: "projections" },
  payload: { topic: "tasks", docId: <taskId>, rev,
             data: <TaskListItemV1>, commandId: <source causationId> } }
```

In M1 a task change gives one notice, to topic `tasks`. From M2 it also
gives one to `task:<id>`.

## Query API (internal)

`GET /internal/tasks?limit=&before=` gives `TaskListResultV1`:

- `items`: newest first (`createdAt`, then `_id`, both descending).
- `limit`: 1 to 100, default 50.
- `before`: an ISO 8601 UTC time. The page has tasks with `createdAt`
  before it. Give the `createdAt` of the last item to get the next page.
  Limit: two tasks with the same `createdAt` millisecond at a page border
  can make the next page skip one of them.
- `headSeq`: the last sequence of `VIEWS` (`$JS.API.STREAM.INFO.VIEWS`),
  read before the documents. A change after the read has a higher
  sequence. If NATS does not answer: 503.

Also `/health`, `/ready` (MongoDB, NATS, consumer, relay) and `/metrics`.

## Rebuild

`npm run rebuild` (function `rebuild` in `src/rebuild.io.ts`):

1. Drops `tasks`, `events` (the notices), `counters` and `processed`, and
   makes their indexes and the position counter again.
2. Reads all events from `GET <ORCHESTRATION_URL>/internal/events?afterPosition=<n>&limit=100`
   page by page, until a page is empty.
3. Applies each event with the same `project` as the live consumer, and
   writes the documents and the notices again.

Decision: a rebuild writes the notices again, with the same `rev` values,
because the same events give the same documents. The relay publishes them
when the service starts again. The app has these revs already and drops
the notices (ARCHITECTURE 10.2), so no screen changes. A notice that was
not published before the rebuild is published now, so no change is lost.
The cost is one burst of notices on `VIEWS` for each rebuild.

Rules:

- Stop the service first (scale the Deployment to 0). The live consumer
  must not write while the rebuild runs. Start it again after the rebuild.
  The durable consumer continues from its last ack; an event that comes
  again is ignored by `asOf`.
- If the rebuild stops in the middle, run it again. It starts from
  nothing each time.
- Env vars: `MONGODB_URI`, `MONGODB_DATABASE`, `ORCHESTRATION_URL`,
  `LOG_LEVEL`. No NATS.

Assumption, until orchestration (M1-06) has the replay API: the answer is
JSON `{ "events": [ <event envelope with "position">, ... ] }`, in
`position` order, as kit `readAll` gives them. Each event is checked with
`parseEvent` of contracts.

## Config

| Env var             | Default       | Meaning                                                               |
| ------------------- | ------------- | --------------------------------------------------------------------- |
| `MONGODB_URI`       | required      | MongoDB replica set URI                                               |
| `MONGODB_DATABASE`  | `projections` | database                                                              |
| `NATS_SERVERS`      | required      | comma-separated NATS URLs                                             |
| `NATS_USER`         | required      | `ai-systems-projections`                                              |
| `NATS_PASSWORD`     | required      | its password                                                          |
| `ORCHESTRATION_URL` | required      | base URL of orchestration (rebuild only)                              |
| `PORT`              | `8080`        | HTTP port                                                             |
| `LOG_LEVEL`         | `info`        | pino level                                                            |
| `RELEASE`           | `local`       | release name in logs and traces                                       |
| `OTEL_*`            | none          | OpenTelemetry SDK settings, for example `OTEL_EXPORTER_OTLP_ENDPOINT` |

The kit service name is fixed to `projections`: the durable consumer is
`projections` and the inbox prefix is `_INBOX_projections` (ADR 0023). The
NATS user `ai-systems-projections` has only these grants (gitops).

## Install

The kit and contracts come from git tags (see the kit README for
`allowScripts`):

```json
"@famashines/ai-systems-contracts": "github:famashines/ai-systems-contracts#v0.1.1",
"@famashines/ai-systems-kit": "github:famashines/ai-systems-kit#v0.1.0"
```

The kit has its own copy of contracts v0.1.0. One `zod` and one `mongodb`
copy are shared.

## Image

`Dockerfile`: Node 26 slim by digest, `npm ci --omit=dev`, the TypeScript
source runs with Node type stripping (`node src/main.ts`), uid 1000. Run
it with a read-only root file system; it writes no files.

## Known limits

- An event type that is not in the installed contracts goes to the DLQ
  (kit default). Release the contracts and projections before a producer
  sends a new type.
- The `before` cursor tie, see the query API.

## Commands

See `CLAUDE.md`.
