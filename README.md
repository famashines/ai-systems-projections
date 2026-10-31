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
| `title`     | the cleaned text, at most 80 graphemes, see below            |
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

Title: control characters (`\p{Cc}`, so also `\r`, `\n`, tab and ESC),
lone surrogates (`\p{Cs}`), the line and paragraph separators (U+2028,
U+2029) and the bidi controls (U+061C, U+200E, U+200F, U+202A to U+202E,
U+2066 to U+2069) become a space. Then the text is trimmed, cut after 80
graphemes (`Intl.Segmenter`, so an emoji with ZWJ stays whole) or before
it is longer than 200 UTF-16 units (the limit of `TaskListItemV1.title`),
and trimmed again. If the first grapheme alone is longer than 200 units,
the title is its first code points up to 200 units. An empty result
becomes the fixed title `(no title)`, because `title` must not be empty.

The pure function `project(current, event)` in `src/tasks.ts` gives the
new document and its notice. The live consumer and the rebuild use it.

## Notices on `VIEWS`

The notice outbox is the kit event store in the same database:
collections `events`, `counters` and `commands` (the kit names, in place of
`outbox_views` in ARCHITECTURE 7). The kit relay publishes each notice to
the subject in its `type`, with `Nats-Msg-Id` = the notice `eventId`. A
message is a `ViewMessageV1` of contracts v0.2.0:

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

`GET /internal/tasks?limit=&before=&beforeId=` gives `TaskListResultV1`:

- `items`: newest first (`createdAt`, then `_id`, both descending).
- `limit`: 1 to 100, default 50.
- `before` and `beforeId`: the cursor. Give the `createdAt` and the
  `taskId` of the last item to get the next page. The page has the tasks
  that come after that item in the order, also tasks with the same
  `createdAt`. `before` alone gives the tasks with `createdAt` before it
  (it can skip a task with the same millisecond). `beforeId` without
  `before` gives 400.
- `headSeq`: the last sequence of `VIEWS` (`$JS.API.STREAM.INFO.VIEWS`),
  read before the documents. A change after the read has a higher
  sequence. If NATS does not answer: 503.

Also `/health`, `/ready` (MongoDB, NATS, consumer, relay) and `/metrics`.

## Rebuild

`npm run rebuild` (function `rebuild` in `src/rebuild.io.ts`; in the image
`--entrypoint node` with the argument `src/rebuild-main.ts`):

```
write the marker { _id: "running" } in "rebuild"   (a pod that starts now refuses)
check the durable "projections": num_waiting > 0 ?  -- yes --> refuse, exit 1
wait about 6 s, check again                          -- yes --> refuse, exit 1
                    (on a refusal, remove the marker if this run wrote it)
drop tasks, events (the notices), counters, processed; make indexes again
GET <ORCHESTRATION_URL>/internal/events?afterPosition=<n>&limit=100, page by page
  each event: parseEvent --> fail: skip and count (type in the log)
                         --> ok:   same project() and transaction as live
remove the marker
```

- A running service always has a waiting pull on its durable consumer, so
  the check finds a pod that was not stopped. The check needs the NATS env
  vars (`CONSUMER.INFO` is in the grants).
- While the marker exists, the service refuses to start: it logs "a rebuild
  runs or did not end" and exits with 1. If the rebuild stops in the
  middle, the marker stays. Run the rebuild again to the end; it starts
  from nothing each time.
- An event that does not parse is skipped and counted, as the live
  consumer sends it to the DLQ. The log gives the count and the types.
- The replay API of orchestration (orchestration PR #1): `limit` is 100 by
  default and 1000 at most. The answer is JSON
  `{ "events": [ <EventEnvelopeV1 with "position">, ... ] }` in
  `position` order. The next page starts after the last `position`; an
  empty list is the end.

Decision: a rebuild writes the notices again, with the same `rev` values,
because the same events give the same documents. The relay publishes them
when the service starts again, so `VIEWS` stores them a second time. The
stream limits keep this bounded (256 MiB, 7 days, discard old); a client
with a `lastSeq` that the stream does not keep any more gets `resync`.
Clients already have these revs and drop the notices (ARCHITECTURE 10.2).
A notice that was not published before the rebuild is published now, so
no change is lost.

After a rebuild that fixes a projection bug, the new documents can differ
but have the same `rev`. Open clients drop the notices and keep the wrong
data until they load a snapshot again (a reconnect with `resync`, or a
new start of the app).

Start the service again after the rebuild. The durable consumer continues
from its last ack; an event that comes again is ignored by `asOf`.

## Config

| Env var             | Default       | Meaning                                                               |
| ------------------- | ------------- | --------------------------------------------------------------------- |
| `MONGODB_URI`       | required      | MongoDB replica set URI                                               |
| `MONGODB_DATABASE`  | `projections` | database                                                              |
| `NATS_SERVERS`      | required      | comma-separated NATS URLs                                             |
| `NATS_USER`         | required      | `ai-systems-projections`                                              |
| `NATS_PASSWORD`     | required      | its password                                                          |
| `ORCHESTRATION_URL` | required      | base URL of orchestration (the rebuild uses it)                       |
| `PORT`              | `8080`        | HTTP port                                                             |
| `LOG_LEVEL`         | `info`        | pino level                                                            |
| `RELEASE`           | `local`       | release name in logs and traces                                       |
| `OTEL_*`            | none          | OpenTelemetry SDK settings, for example `OTEL_EXPORTER_OTLP_ENDPOINT` |

The service and the rebuild use the same env vars. The kit service name is
fixed to `projections`: the durable consumer is `projections` and the
inbox prefix is `_INBOX_projections` (ADR 0023). The NATS user
`ai-systems-projections` has only these grants (gitops).

## Install

The kit and contracts come from git tags (see the kit README for
`allowScripts`):

```json
"@famashines/ai-systems-contracts": "github:famashines/ai-systems-contracts#v0.2.0",
"@famashines/ai-systems-kit": "github:famashines/ai-systems-kit#v0.1.0"
```

npm uses this one contracts copy also for the kit. One `zod` and one
`mongodb` copy are shared.

## Image

`Dockerfile`: Node 26 slim by digest, `npm ci --omit=dev --omit=optional`
in a build stage, no npm or npx in the runtime stage, uid 1000,
`ENTRYPOINT ["node", "src/main.ts"]` (Node type stripping). Run it with a
read-only root file system; it writes no files. node is PID 1, so
`main.ts` exits with 143 on SIGTERM or SIGINT while it connects; after the
start the kit shutdown takes over.

Release: a push to `main` runs `.github/workflows/release.yml`. It builds
the image, makes a CycloneDX SBOM (a workflow artifact), and scans the
image with grype. A HIGH or CRITICAL finding with a fix stops the
release. Then it pushes `ghcr.io/famashines/ai-systems-projections:sha-<commit>` and
opens a pull request in `famashines/gitops` that sets the tag and the
digest in `kubernetes/ai-systems/ai-systems-projections/prod`. The owner merges it. The
job needs the Actions secret `GITOPS_TOKEN`.

## Known limits

- An event type that is not in the installed contracts goes to the DLQ
  (kit default). Release the contracts and projections before a producer
  sends a new type.
- `commandId` in a notice is the `causationId` of the source event. That
  is correct only for events that a command caused. Look at it again in
  M3, when process managers cause events.
- The `asOf` rule cannot see a gap: if a version went to the DLQ, a later
  version is applied and the gap stays. From M2, with streams of more than
  one event, check `version === asOf + 1`.

## Commands

See `CLAUDE.md`.
