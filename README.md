# ai-systems-projections

Read models of ai-systems. projections reads the domain events from NATS
JetStream, keeps read model documents in MongoDB, and writes a notice for
each change to the stream `VIEWS` through an outbox (ADR 0010).
api-gateway reads the documents over HTTP. socket-gateway sends the notices
to the app. Architecture: `ARCHITECTURE.md` sections 2, 7, 10 and 12 in
`ai-systems-docs`.

```
ORCHESTRATION --+
CONVERSATION  --+  one durable ordered consumer "projections"
LLM           --+  for each stream (filter <context>.>)
BUDGET        --+          |
AGENTS        --+          v
                        handler, one MongoDB transaction:
                          read model documents (rev, asOf)
                          notices in events (the outbox)
                          processed marker
                           |
                           v
   outbox relay --> VIEWS (view.<topic>) --> socket-gateway --> app
api-gateway --GET /internal/...--> read models + headSeq (STREAM.INFO VIEWS)
```

| Read model     | Source events                                                                                                                                                  | Topic               |
| -------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------- |
| `tasks`        | `orchestration.TaskSubmitted`, `RunStarted`, `RunBlocked`, `RunCompleted`, `RunFailed`                                                                         | `tasks`             |
| `chat`         | `conversation.OwnerMessagePosted`, `AgentReplyPosted`; the `taskId` from `orchestration.TaskSubmitted`; the cost from `llm.CallCompleted` and `llm.CallFailed` | `conversation:<id>` |
| `budget_month` | `budget.BudgetSet`, `FundsReserved`, `ReservationSettled`, `ReservationExpired`                                                                                | `budget`            |
| `agents`       | `agents.AgentCreated`, `AgentVersionPublished`, `AgentVersionActivated`, `AgentEnabled`, `AgentDisabled`                                                       | `agents`            |

Other event types change nothing and give no notice.

## Read model `tasks`

Database `projections` (`MONGODB_DATABASE`), one document for each task:

| Field       | Value                                                        |
| ----------- | ------------------------------------------------------------ |
| `_id`       | the task id                                                  |
| `title`     | the cleaned text, at most 80 graphemes, see below            |
| `status`    | see below                                                    |
| `createdAt` | `occurredAt` of the first event (a MongoDB date)             |
| `updatedAt` | `occurredAt` of the last event that changed the document     |
| `rev`       | 1 for a new document, plus 1 for each change                 |
| `asOf`      | `version` of the last source event that changed the document |

Status: `TaskSubmitted` gives `submitted`, `RunStarted` `running`,
`RunBlocked` `blocked`, `RunCompleted` `completed`, `RunFailed` `failed`.
A run event for a task with no document changes nothing.

The dates come from the events, not from the clock, so a rebuild gives the
same documents. `asOf` is the checkpoint of the source stream: the handler
ignores an event with a `version` that is not higher than `asOf`. Each
task stream gives one document, so the document holds the checkpoint and
there is no `checkpoints` collection (ARCHITECTURE 7).

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

## Read model `chat`

One document for each message, `_id` = `messageId`, with
`conversationId`, `role`, `text`, `createdAt` (`occurredAt` of the
event), `intent` and `taskId` (owner), `taskId` and `costMicroUsd`
(agent), `rev` and `asOf` (the version of the conversation event). The
item is `ConversationMessageV1`.

Three streams change one message, and nothing orders them. So each
change works in both orders, also in a rebuild that replays one service
after the other:

```
owner message (new_task)   TaskSubmitted { conversationId, messageId }
        \                     /
         chat_links { _id: messageId, taskId? }   both write it
        message.taskId = link.taskId; rev = 1, or 2 with the taskId

agent reply (with runId)   llm.CallCompleted | CallFailed
        \                     /
         chat_costs { _id: taskId, calls: { "<callId>:<attempt>": cost } }
        message.costMicroUsd = sum of calls; rev = 1 + number of calls
```

- `rev` comes from the state, not from the order: the same events give
  the same `rev` and the same document live and after a rebuild.
- Both sides of a join write the link or cost document with a `rev`
  check. Two changes at the same time cannot both miss each other: one
  gets a write conflict or `ConcurrencyError`, and the kit consumer
  applies it again.
- The cost of an agent message is the cost of all LLM attempts of its
  task, completed and failed (`costMicroUsd` of each attempt once). Only
  replies from a run (`runId`, from `RunCompleted`, `RunFailed` or
  `RunBlocked`) get it. A refusal reply (`MessageRefused`) has no cost.
  `costMicroUsd` is absent until the first cost event of the task comes.
  `RunCompleted.costMicroUsd` is not used: it misses failed and blocked
  runs.
- A steer or answer message has its `taskId` from the event.
- Each notice of an owner message has its `commandId` (the
  `causationId` of `OwnerMessagePosted`, which conversation-hub sets to
  the command id). Agent messages have none.

## Read model `budget_month`

One document for each month (`_id` = `YYYY-MM`, from the stream id
`budget-month-<YYYY-MM>`) with `spent`, `reserved`, a copy of the cap,
`rev`, and two checkpoints: `monthVersion` and `capVersion`. The cap
(`budget.BudgetSet` on `budget-cap-month`) is also in `budget_cap`.

- `FundsReserved` adds to `reserved`. `ReservationSettled` takes
  `reservedMicroUsd` from `reserved` and adds `actualMicroUsd` to
  `spent`. `ReservationExpired` moves `amountMicroUsd` from `reserved` to
  `spent`. `ReservationRefused` and `ThresholdCrossed` change nothing.
- `BudgetSet` changes every month document (the cap applies to every
  month) and makes the document of the month of its `occurredAt` in
  `OWNER_TIME_ZONE`. Each changed month gets a notice.
- The view is `BudgetMonthV1` with the rule of budget:
  `remaining = max(0, cap - spent - reserved)`,
  `overCap = max(0, spent - cap)`, both `0` with no cap
  (`capMicroUsd: null`). `asOf` is `monthVersion`, or `capVersion` while
  the month has no event.
- A month with no document and a cap gives the cap with zeros, `rev` 0
  and `asOf` of the cap. No cap and no document gives 404, because
  `asOf` must be 1 or more.

## Read model `agents`

One document for each agent stream `agent-<id>`, the same rules as
agent-manager: `enabled` is `false` after `AgentCreated`,
`latestVersion` is the highest published version, `activeVersion` the
last activated one. The item is `AgentListItemV1`.

## Notices on `VIEWS`

The notice outbox is the kit event store in the same database:
collections `events`, `counters` and `commands` (the kit names, in place of
`outbox_views` in ARCHITECTURE 7). The kit relay publishes each notice to
the subject in its `type`, with `Nats-Msg-Id` = the notice `eventId`. A
message is a `ViewMessageV1` of contracts v0.3.0:

```
{ eventId, type: "view.tasks", schemaVersion: 1,
  streamId: "notice-<source eventId>", version, occurredAt,
  correlationId: <source correlationId>,
  causationId:   <source eventId>,
  actor: { kind: "system", id: "projections" },
  payload: { topic: "tasks", docId: <taskId>, rev,
             data: <TaskListItemV1>, commandId: <source causationId> } }
```

All notices of one source event are in one outbox stream
`notice-<eventId>`, so one transaction can write notices for several
documents (a `TaskSubmitted` of the chat gives a `tasks` notice and a
`conversation:<id>` notice). The data of each notice is the item of its
topic in `NoticeDataV1`. `commandId` is only on `tasks` notices of
`TaskSubmitted` and on owner message notices. No `task:<id>` notice yet
(the task detail read model is M3).

## Query API (internal)

| Route                                                               | Result                       |
| ------------------------------------------------------------------- | ---------------------------- |
| `GET /internal/tasks?limit=&before=&beforeId=`                      | `TaskListResultV1`           |
| `GET /internal/conversations/:id/messages?limit=&before=&beforeId=` | `ConversationThreadResultV1` |
| `GET /internal/budget?month=YYYY-MM`                                | `BudgetMonthV1`              |
| `GET /internal/agents`                                              | `AgentListResultV1`          |

Lists:

- `items`: newest first (`createdAt`, then `_id`, both descending).
- `limit`: 1 to 100, default 50.
- `before` and `beforeId`: the cursor. Give the `createdAt` and the id
  (`taskId` or `messageId`) of the last item to get the next page. The
  page has the items that come after that item in the order, also items
  with the same `createdAt`. `before` alone gives the items with
  `createdAt` before it (it can skip an item with the same millisecond).
  `beforeId` without `before` gives 400.
- `headSeq`: the last sequence of `VIEWS` (`$JS.API.STREAM.INFO.VIEWS`),
  read before the documents. A change after the read has a higher
  sequence. If NATS does not answer: 503.

Thread:

- An unknown thread gives 200 with no items.
- A page has at most 900 KiB of JSON, because api-gateway reads at most
  1 MiB from an upstream. The service stops before the item that passes
  this limit. A message is at most about 600 KB (100,000 characters, 6
  bytes each after JSON escaping), so one message always fits.
- So a page can have fewer items than `limit`. Only an empty page is the
  end. The cursor is the last item that the page has, so the next page
  starts at the first item that did not fit.

Budget: `month` is optional; the default is the current month in
`OWNER_TIME_ZONE` (the clock of the route, not of the projection). 404
`not_found` when no cap is set and the month has no document.

Agents: all agents, by `name`. No paging (few agents).

Also `/health`, `/ready` (MongoDB, NATS, `consumer_orchestration`,
`consumer_conversation`, `consumer_llm`, `consumer_budget`,
`consumer_agents`, `relay`) and `/metrics`.

## Rebuild

`npm run rebuild` (function `rebuild` in `src/rebuild.io.ts`; in the image
`--entrypoint node` with the argument `src/rebuild-main.ts`):

```
write the marker { _id: "running" } in "rebuild"   (a pod that starts now refuses)
check the durable "projections": num_waiting > 0 ?  -- yes --> refuse, exit 1
wait about 6 s, check again                          -- yes --> refuse, exit 1
                    (on a refusal, remove the marker if this run wrote it)
drop the read models (tasks, chat, chat_links, chat_costs, budget_month,
  budget_cap, agents), events (the notices), counters, processed;
  make indexes again
for each source: orchestration, conversation-hub, ai-proxy, budget, agent-manager
  GET <URL>/internal/events?afterPosition=<n>&limit=100, page by page
  each event: parseEvent --> fail: skip and count (type in the log)
                         --> ok:   same handler and transaction as live
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
- The replay API of each service: `limit` is 100 by default and 1000 at
  most. The answer is JSON
  `{ "events": [ <EventEnvelopeV1 with "position">, ... ] }` in
  `position` order. The next page starts after the last `position`; an
  empty list is the end.

Decision: a rebuild writes the notices again. The last notice of each
document has the same `rev` and data as live, because the same events give
the same documents. In the chat the steps before it can differ (for
example one notice with `rev` 2 in place of 1 and 2), because the sources
come in another order. The relay publishes them
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

| Env var                | Default                                   | Meaning                                                               |
| ---------------------- | ----------------------------------------- | --------------------------------------------------------------------- |
| `MONGODB_URI`          | required                                  | MongoDB replica set URI                                               |
| `MONGODB_DATABASE`     | `projections`                             | database                                                              |
| `NATS_SERVERS`         | required                                  | comma-separated NATS URLs                                             |
| `NATS_USER`            | required                                  | `ai-systems-projections`                                              |
| `NATS_PASSWORD`        | required                                  | its password                                                          |
| `ORCHESTRATION_URL`    | required                                  | base URL of orchestration (the rebuild uses it)                       |
| `CONVERSATION_HUB_URL` | `http://ai-systems-conversation-hub:8080` | base URL of conversation-hub (rebuild)                                |
| `AI_PROXY_URL`         | `http://ai-systems-ai-proxy:8080`         | base URL of ai-proxy (rebuild)                                        |
| `BUDGET_URL`           | `http://ai-systems-budget:8080`           | base URL of budget (rebuild)                                          |
| `AGENT_MANAGER_URL`    | `http://ai-systems-agent-manager:8080`    | base URL of agent-manager (rebuild)                                   |
| `OWNER_TIME_ZONE`      | `Europe/Berlin`                           | IANA time zone of the budget month; the same value as budget          |
| `PORT`                 | `8080`                                    | HTTP port                                                             |
| `LOG_LEVEL`            | `info`                                    | pino level                                                            |
| `RELEASE`              | `local`                                   | release name in logs and traces                                       |
| `OTEL_*`               | none                                      | OpenTelemetry SDK settings, for example `OTEL_EXPORTER_OTLP_ENDPOINT` |

The service and the rebuild use the same env vars. The kit service name is
fixed to `projections`: the durable consumer is `projections` and the
inbox prefix is `_INBOX_projections` (ADR 0023). The NATS user
`ai-systems-projections` has only these grants (gitops).

## Install

The kit and contracts come from git tags (see the kit README for
`allowScripts`):

```json
"@famashines/ai-systems-contracts": "github:famashines/ai-systems-contracts#v0.3.0",
"@famashines/ai-systems-kit": "github:famashines/ai-systems-kit#v0.3.0"
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

Release: when `ci` passes on a push to `main`, `.github/workflows/release.yml`
builds the image of that commit, makes a CycloneDX SBOM (a workflow
artifact), and scans the image with grype. A HIGH or CRITICAL finding that
has a fix stops the release. Findings with no fix do not stop it; they are
in the job log. Then it pushes `ghcr.io/famashines/ai-systems-projections:sha-<commit>`
and opens or updates the pull request from the branch
`release/ai-systems-projections-prod` in `famashines/gitops`. That pull request sets the
tag and the digest in `kubernetes/ai-systems/ai-systems-projections/prod`. The owner
merges it. The job needs the org secret `GITOPS_TOKEN` with access to this
repo. A manual run (`workflow_dispatch`) works only on `main`.

## Known limits

- An event type that is not in the installed contracts goes to the DLQ
  (kit default). Release the contracts and projections before a producer
  sends a new type.
- `commandId` in a notice is the `causationId` of the source event. That
  is correct only for events that a command caused. Look at it again in
  M3, when process managers cause events.
- The `asOf` rule cannot see a gap: if a version went to the DLQ, a later
  version is applied and the gap stays. A check for `version === asOf + 1`
  is not done: events with no effect do not move `asOf`, and a gap would
  stop the stream until a rebuild.
- A task with more than one run reply (a resume after `RunBlocked`, from
  M3) shows the cost of the whole task on each run reply.
- A client that holds the zero budget view of a month with no document
  (`rev` 0) gets the next `BudgetSet` notice only for the month of that
  event. At a month start, the first reserve of the month makes the
  document and its notice.
- An LLM event of a task that is not in a thread also makes a
  `chat_costs` document. It is small and never shown.

## Commands

See `CLAUDE.md`.
