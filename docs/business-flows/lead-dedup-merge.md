# Lead dedup / merge + assignment

**Actors:** a salesperson or sales-ops user reviewing duplicate leads (any authenticated
principal carrying a company token).

**Preconditions:**

- The actor's token names exactly one company (`company_auth`); every statement the flow runs is
  fenced to it (ADR-0008 RLS).
- Leads exist in any capture format — `+62 812-3456-789`, `0812-3456-789`, `  Foo@BAR.id `,
  `PT   Cipta` — the flow matches on normalized keys, not raw text.

## Main path

1. **Scan.** `GET /leads/duplicates-candidates` returns candidate groups: live leads of the
   company sharing a normalized key —
   - phone / WhatsApp: digits only, Indonesian prefixes canonicalized (`0…` → `62…`, bare `8…` → `62…`),
   - email: trimmed + lowercased,
   - organization name: trimmed + lowercased + internal whitespace collapsed.
   Groups are **per key**: a pair matching on both phone and email yields two groups sharing
   members (connected-component clustering is deliberately not attempted — merging either group
   dissolves the overlap on the next scan). Each group carries the match reason
   (`keyKind` + `keyValue`), the member projection in confidence order, and a
   `suggestedMasterId`.
2. **Pick a master.** Either accept the suggestion, pin any member
   (`POST /leads/:id/merge`), or let the module pick (`POST /leads/merge` over 2..=6 lead ids).
   Both merge verbs answer with `masterId`, `absorbedIds`, `redirectedFrom`,
   `alreadyAbsorbedElsewhere`, and `fieldsFilled` — the host's re-point contract.
3. **Merge.** One transaction: fetch row-locked → classify every id → fill the master's null
   fields → soft-absorb each dupe. Absorbed leads are **never deleted**: they keep
   `merged_into_lead_id` + `merged_at`, which excludes them from every future scan and makes
   them permanently ineligible as master or dupe.
4. **Event.** After the transaction commits — and only if a lead was newly absorbed — the module
   publishes `LeadMerged { lead_id, absorbed_ids, company_id }` through its event sink.

## Business rules

**Confidence order (one total order, used for the suggested master, the auto pick, and the
field-fill order):**

1. Status precedence: converted > qualified > contacted > new > junk/lost (junk and lost share
   the bottom rank) > anything unknown.
2. Then a party anchor (`party_id` set) outranks none.
3. Then the NEWEST `created_at` wins; a missing timestamp ranks last.
4. Then the smallest `id` (deterministic tiebreak — repeated scans pick identically).

**Master-wins field fill:** the master's non-null values always win; each null lead-owned field
(`organizationName`, `phone`, `whatsappNo`, `email`, `notes`, `campaignId`, `ownerUserId`,
`salesTeamId`, `utmSource`, `utmMedium`, `utmCampaign`) fills from the first non-null dupe in
confidence order — attribution survives a merge, so the won roll-up still knows where the lead
came from; the names of filled
fields are reported in `fieldsFilled`. `party_id` and `converted_at` are **never** filled —
conversion is once per lead.

**Converted leads:** may be a master, NEVER a dupe (the party anchor is one-shot). Naming one as
an absorb target refuses the whole request atomically — HTTP 422 `absorb_converted`, nothing
written, not even for innocent members of the same batch.

**Idempotence:** re-merging the same batch is a silent no-op (same answer, `merged_at` not
re-stamped, no event). An id already absorbed into a DIFFERENT master changes nothing and comes
back in `alreadyAbsorbedElsewhere` under its real master. Pinning an already-absorbed lead
redirects the merge to its ultimate master (`redirectedFrom` reports it).

**Batch caps:** a pinned merge absorbs 1..=5 leads; an auto merge picks from 2..=6. Both
refuse with 422 `invalid_batch` before touching the table; absorbing the master itself is
422 `absorb_self`.

**Cross-tenant:** a foreign or unknown id simply does not resolve — the fence-shaped answer is
404 `not_found`, never a leak that the id exists elsewhere. The RLS fence covers the new
columns with the table (probe walk in the goldens).

**Module boundary — assignment and re-points:**

- `owner_user_id` / `sales_team_id` are **stored only**. Autofill, round-robin, and
  leader-fallback defaults are the composing service's job; do not look for them here.
- Cross-module re-points (deal opportunities, mail activities referencing an absorbed lead id)
  are the **host's** job, exactly as qualify/convert orchestration already is. The merge
  response carries `masterId` + `absorbedIds` precisely so the host can re-point; this module
  takes no dependency on deal/activity.

## Alternate / failure paths

| Path | Result |
|---|---|
| Absorb target is converted | 422 `absorb_converted`, atomic rollback |
| Absorb list empty / oversized, or master in its own absorb list | 422 `invalid_batch` / `absorb_self` (pre-DB) |
| Foreign / unknown id (any side) | 404 `not_found` (fence shape) |
| Malformed scan parameter | 422 `invalid_input` |
| Missing / tenantless token on any verb | 401 |
| Corrupt absorb cycle (unreachable by construction) | 500 `chain_too_deep` |

## Postconditions

- Dupes soft-absorbed (never deleted), master filled, one `LeadMerged` event per real merge.
- The next duplicate scan no longer lists the merged group.
- Read surface unchanged; the guarded mount adds capture + scan + merge verbs and does NOT
  mount generic mutation (no client can PUT a lead into a bogus pipeline state or un-merge by
  nulling `merged_into_lead_id`).

**Executable oracle:** `tests/lead_merge_cases.rs` + `tests/lead_merge_routes_test.rs`,
mirrored case-for-case in [golden-cases.md](golden-cases.md).
