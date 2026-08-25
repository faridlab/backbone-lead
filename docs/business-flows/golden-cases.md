# Golden cases — lead dedup / merge + assignment

Mirrors the executable oracle one-to-one: `tests/lead_merge_cases.rs` (NG/DG/MG/IDM/RF/F) and
`tests/lead_merge_routes_test.rs` (R), plus the pure-logic unit goldens inside
`src/application/service/lead_merge.rs`. Every case runs against real Postgres with the `lead`
schema (scratch database; see each file header for the harness).

## NG — normalization goldens (generated match-key columns)

| Id | Input | Expected key |
|---|---|---|
| NG-1 | phone `+62 812-3456-789` | `628123456789` |
| NG-1 | phone `0812-3456-789` (the motivating join) | `628123456789` — same key |
| NG-1 | phone `812-3456-7890` (bare local) | `6281234567890` — gains the `62` country code |
| NG-1 | phone `+1 555 0100` (non-Indonesian) | `15550100` — digits only, no prefix guess |
| NG-2 | email `  Foo@BAR.id ` | `foo@bar.id` |
| NG-2 | organization `  PT   Cipta  ` | `pt cipta` — whitespace collapsed |
| NG-3 | phone/email/org `""` or NULL | key NULL — keyless leads never cluster |

## DG — duplicate-candidate grouping

| Id | Case | Expected |
|---|---|---|
| DG-1 | two leads, `+62 812-3456-789` vs `0812-3456-789` | one `phone` group, `memberCount` 2, members in confidence order, `suggestedMasterId` set |
| DG-2 | same pair also matches on `Andi@Mail.ID` vs `andi@mail.id` | two per-key groups sharing members (no cluster collapse) |
| DG-3 | pair merged (one absorbed) | group leaves the scan |
| DG-4 | one side soft-deleted (`metadata.deleted_at`) | group leaves the scan |
| DG-5 | same phone in companies A and B | no group in either — company-scoped end to end |

## MG — confidence-ordered master pick

| Id | Case | Expected master |
|---|---|---|
| MG-1 | converted vs qualified vs contacted vs new vs junk, same key | the converted lead (party anchor preserved untouched); junk/lost rank last; a converted lead CAN be master |
| MG-2 | junk (older) vs lost (newer) — same bottom rank | the newer `created_at` wins |
| MG-3 | full tie (status, party, created_at) | the smallest uuid — and repeated scans keep picking it |

**MG-4 field fill (master-wins):** master's non-null `notes` / `campaignId` / `ownerUserId` /
`salesTeamId` kept; null `organizationName` / `phone` / `email` filled from the dupe;
`fieldsFilled` names exactly the filled fields; `party_id` / `converted_at` never filled.
Attribution rides the same rule (MA section): a null UTM trio fills from the first attributed
dupe, a master's own UTM values win.
Unit-golden: `fill_or_keep_prefers_master_then_first_dupe`.

## IDM — idempotence

| Id | Case | Expected |
|---|---|---|
| IDM-1 | repeat the identical pinned merge | same `absorbedIds`, `merged_at` NOT re-stamped, no event |
| IDM-2 | absorb id already owned by another master | reported in `alreadyAbsorbedElsewhere` under its REAL master, zero writes |
| IDM-3 | pin an already-absorbed lead as master | silent redirect to the ultimate master, `redirectedFrom` set, new dupes land there |

## RF — refusals

| Id | Case | Expected |
|---|---|---|
| RF-1 | absorb target is converted | 422 `absorb_converted`, atomic: no fill, no absorb anywhere in the batch |
| RF-2 | self-absorb / empty absorb batch / 6 pinned absorbs / 7 auto ids / ghost id / `min_group_size=1` | 422 `absorb_self` / 422 `invalid_batch` ×3 / 404 `not_found` (fence shape) / 422 `invalid_input` |
| RF-3 | `LeadMerged` event | published post-commit (a fresh connection at publish time already sees the absorb), once per real merge, silent on the idempotent no-op |

## F — RLS fence over the new columns (probe-role walk, `lead_probe_rls` NOLOGIN)

| Id | Case | Expected |
|---|---|---|
| F-1 | role unbound | zero rows, including reads of `owner_user_id` |
| F-2 | bound to A, merge fetch names B's lead | zero-row miss (the fence's 404 shape) |
| F-3 | bound to A, cross-company INSERT touching `owner_user_id` / UPDATE on B's row | WITH CHECK rejects the insert; fenced update touches zero rows |
| F-4 | duplicate scan bound to A | exactly A's groups; B's lead never leaks in |

## R — HTTP surface (guarded mount, in-process router, forged HS256 company tokens)

| Id | Case | Expected |
|---|---|---|
| R-1 | `POST /leads` (camelCase body incl. `ownerUserId`/`salesTeamId`) | 201 `{id}`; tenant from the token, assignment stored as given |
| R-1 | `GET /leads/duplicates-candidates?min_group_size=2&limit=50` | 200; groups with `matchReason.{keyKind,keyValue}`, `memberCount`, `suggestedMasterId`, camelCase member projection |
| R-1 | `POST /leads/:id/merge` body `{absorbIds:[…]}` | 200 `{masterId, redirectedFrom, absorbedIds, alreadyAbsorbedElsewhere, fieldsFilled}` |
| R-1 | `POST /leads/merge` body `{leadIds:[…]}` | 200, same shape |
| R-1 | `?min_group_size=abc`; capture with no contact channel | 422 `invalid_input` (typed module shape, not the extractor's 400) |
| R-1 | `GET /leads/merge` | 405 — static segment beats `:id` |
| R-2 | absent token on every verb; token without a company claim | 401 before any handler |
| R-2 | `POST /definitely-not-a-route` | plain 404 — `route_layer` keeps unmatched paths 404, not 401 |
| R-2 | company B pins company A's lead | 404 `not_found` (fence shape over HTTP) |
| R-3 | `GET /leads/count`, `GET /leads/:id`, unknown id | 200 / 200 / 404 — the generated read surface rides along unchanged (host wraps its own auth around the whole mount) |

## Unit goldens (pure logic, no DB)

- `status_precedence_orders_the_master_pick` — converted > qualified > new; junk/lost share the
  bottom rank (recency decides, not status).
- `anchor_then_recency_then_uuid_tiebreak` — party anchor beats recency; newer beats older;
  absent timestamp last; full tie → smaller uuid.
- `fill_or_keep_prefers_master_then_first_dupe` — master's non-null value wins; first non-null
  dupe fills a null; empty dupes leave it null.
