# Changelog

## 0.1.0

- `tasks` read model from `orchestration.TaskSubmitted`: title, status,
  `createdAt`, `updatedAt`, `rev` and `asOf`, with the `asOf` checkpoint.
- Durable ordered consumer `projections` on `ORCHESTRATION`, idempotent
  with `processed` in the same transaction.
- `VIEWS` notices (`ViewMessageV1`, topic `tasks`) through the kit outbox
  in the same transaction as the document.
- `GET /internal/tasks` with `limit`, `before` and `headSeq`.
- `npm run rebuild` from the orchestration replay API.
- Dockerfile, CI (verify, audit, gitleaks, image, integration).
