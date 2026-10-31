# CLAUDE.md for ai-systems-projections

Rules for all ai-systems repos: `ai-systems-docs/CLAUDE.md`
(local: `../../docs/CLAUDE.md`).

## Commands

| Command                    | Does                                                  |
| -------------------------- | ----------------------------------------------------- |
| `npm ci`                   | install from the lock file                            |
| `npm run check`            | `tsc --noEmit` (TypeScript 7)                         |
| `npm run lint`             | ESLint (strict type-checked) and Prettier check       |
| `npm test`                 | `node --test` with the coverage gate: 70% lines       |
| `npm run verify`           | check, lint and test                                  |
| `npm run test:integration` | `*.integration.ts` with Testcontainers (needs Docker) |
| `npm start`                | the service (`node src/main.ts`)                      |
| `npm run rebuild`          | rebuild the read models (stop the service first)      |

## Layout

```
src/tasks.ts          project(current, event): the tasks document and its notice (no I/O)
src/chat.ts           chat messages, the taskId link, cost per reply, page by bytes (no I/O)
src/budget.ts         budget month numbers and the cap (no I/O)
src/agents.ts         agent list documents (no I/O)
src/notices.ts        the VIEWS notice of a document
src/query.ts          GET /internal/{tasks,conversations/:id/messages,budget,agents}, with ports
src/config.ts         env schema of the service and the rebuild
src/projector.io.ts   MongoDB writes in one transaction, notice store, list, headSeq, migrations
src/rebuild.io.ts     rebuild, its guard and marker, the replay API client
src/app.io.ts         five consumers, relay and routes together
src/main.ts           service wiring
src/rebuild-main.ts   rebuild command
```

Code with MongoDB or NATS is in `*.io.ts` and `*main.ts` files. The unit
coverage gate does not include them; the integration test covers them.

## Integration test

`src/projections.integration.ts` starts MongoDB and NATS with the real
grants of `ai-systems-projections` (copied from gitops) and an `admin`
that makes the streams. It waits with `waitFor`, never with a fixed sleep.
The CI job runs on `ubuntu-22.04` (see the kit `CLAUDE.md` for the
MongoDB and kernel reason).

## TypeScript 6 and 7

As in the kit (ADR 0022): `typescript-7` does `check`, `typescript` (6) is
only for ESLint. Never run `npx tsc`.
