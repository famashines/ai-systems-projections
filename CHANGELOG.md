# Changelog

## Unreleased

- Contracts v0.4.1. Projections must run v0.4.1 before orchestration
  sends the M3 events (deploy order of contracts 0.4.1, step 1).
- `task_details` read model (`TaskDetailV1`) from the task stream: plan
  with `SubtaskReassigned`, subtask states and the latest checkpoint of
  each subtask from the status table, `currentCall`, the cost from
  `CallSettled`. Notices on `task:<id>`.
- `task_outputs` (`TaskOutputV1`): the run result from `RunCompleted` and
  each subtask output from `SubtaskCompleted`.
- `GET /internal/tasks/:id`, `GET /internal/tasks/:id/output`,
  `GET /internal/tasks/:id/subtasks/:sid/output`.
- `tasks`: the status of all task events of the status table
  (`clarifying`, `planning`, `awaiting_approval`, `paused`, `cancelled`,
  `rejected` are new).

- Contracts and kit v0.3.0.
- `chat` read model (`ConversationMessageV1`) from `CONVERSATION`, with the
  `taskId` of a new task from `orchestration.TaskSubmitted` and the cost
  of each agent reply from `llm.CallCompleted` and `llm.CallFailed`, in
  any order. Notices on `conversation:<id>`.
- `GET /internal/conversations/:id/messages`: 200 with no items for an
  unknown thread; a page has at most 900 KiB of JSON.
- `budget_month` read model (`BudgetMonthV1`) from `BUDGET`, notices on
  `budget`, `GET /internal/budget?month=` (404 with no cap and no spend).
- `agents` read model (`AgentListItemV1`) from `AGENTS`, notices on
  `agents`, `GET /internal/agents`.
- `tasks`: status from `RunStarted`, `RunBlocked`, `RunCompleted`,
  `RunFailed`; other orchestration events change nothing.
- One durable ordered consumer `projections` for each of `ORCHESTRATION`,
  `CONVERSATION`, `LLM`, `BUDGET` and `AGENTS`.
- Notices of one source event are in the outbox stream
  `notice-<eventId>`.
- Rebuild from five replay APIs. New env vars: `CONVERSATION_HUB_URL`,
  `AI_PROXY_URL`, `BUDGET_URL`, `AGENT_MANAGER_URL`, `OWNER_TIME_ZONE`.

- Release workflow: image, CycloneDX SBOM, grype scan, push to GHCR
  by tag and digest, and a release pull request in `famashines/gitops`.

## 0.1.0

- `tasks` read model from `orchestration.TaskSubmitted`: title, status,
  `createdAt`, `updatedAt`, `rev` and `asOf`, with the `asOf` checkpoint.
- Durable ordered consumer `projections` on `ORCHESTRATION`, idempotent
  with `processed` in the same transaction.
- `VIEWS` notices (`ViewMessageV1`, topic `tasks`) through the kit outbox
  in the same transaction as the document.
- `GET /internal/tasks` with `limit`, the `before` and `beforeId` cursor,
  and `headSeq`.
- `npm run rebuild` from the orchestration replay API, with the running
  service check, the `rebuild` marker and skipped bad events.
- Title: graphemes, no control or bidi characters, at most 200 units.
- Dockerfile, CI (verify, audit, gitleaks, image, integration).
