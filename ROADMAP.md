# Dataset Availability Roadmap

Last updated: 2026-10-29.

This is a living maintenance record. Update statuses and evidence after each
availability review, export delivery and package release. It does not itself
run checks or send notifications; no scheduled monitor is deployed by this plan.

## Current priority and evidence

- The 2026-10-27 handoff reports 20 tables for
  `Ownership History (Semi-Annual)`: `entities`, `links_2007` through
  `links_2024`, and `links_current`. That is 18 year-named link tables plus
  `links_current`, not 19 confirmed annual snapshots.
- On 2026-10-29, the CBS SFTP owner confirmed that `links_2025` has now been
  exported. This supersedes the earlier absence report. No independent live
  SFTP check was run by the agent; the exact export completion date, snapshot
  cutoff and revision policy have not been supplied.
- Release 1.4.13's packaged metadata has no `links_2025` entry. Package version,
  upstream publication and CBS export freshness are separate facts.
- The user updated the source `data_products.xlsx` catalog with the Parquet
  `links_2025` table. Local workbook inspection on 2026-10-29 confirms that
  Ownership History now lists `entities`, `links_2007` through `links_2025`,
  and `links_current`: 21 catalog entries, including 19 year-named link tables.
  Both dictionaries, date metadata, batch template and README/MkDocs are now
  updated, with focused regressions. Shared metadata is inherited from the
  existing annual-link entries; independent live schema validation and a new
  release are still pending.
- Do not assume `links_current` represents 2025 or the extraction date. Confirm
  its effective date and the historical tables' cutoff/revision policy with the
  provider. Export/marker timestamps and row-level `information_date` values
  are not interchangeable with a confirmed snapshot date.

## Delivery queue

| ID | Priority | Work | Status / dependency | Responsible role |
| --- | --- | --- | --- | --- |
| DSET-001 | Urgent | Deliver the 2025 Ownership History export to CBS SFTP | Done: CBS SFTP owner confirmed delivery on 2026-10-29; provider snapshot semantics remain a follow-up | CBS data/SFTP owner with Moody's contact |
| DSET-002 | Urgent | Incorporate verified table metadata, tests and docs into a new release | In progress: catalog, dictionaries, date/batch metadata, docs and regressions updated; live schema validation and release pending | Package maintainer |
| DSET-003 | High | Establish an approved inventory baseline and read-only drift report | Planned; can proceed independently of the 2025 delivery | Package maintainer with CBS data owner |
| DSET-004 | High | Automate recurring inventory checks and actionable alerts | Planned; requires DSET-003, an approved runner and credential handling | CBS operations with package maintainer |
| DSET-005 | High | Retire CBS CSV-service connections/catalog entries while preserving CSV file processing | Implemented locally; validation and release pending | Package maintainer |

## Export first, package second

1. DSET-001 delivery is confirmed by the CBS SFTP owner on 2026-10-29. Retain
   that confirmation as evidence, separately from provider publication and
   package release dates. Ask the provider how the snapshot cutoff is defined,
   whether it derives from `links_current`, and whether it can be revised.
2. Before releasing metadata, record the selected export marker/path and
   readable schema from an authorized check. The owner confirmed export, but
   these details were not independently inspected here. An empty directory or
   a workbook entry alone is not proof of completed delivery. Record the
   confirmed effective date separately when supplied.
3. DSET-002: Reconcile `data_products.xlsx`, `data_dict.xlsx`,
   `data_dict_available.xlsx`, `date_cols.xlsx` and `products.xlsx` where relevant
   to their documented roles. Do not manufacture rows for unverified tables or
   remove broader dictionary entries merely because CBS does not license them.
4. Add focused metadata consistency and discovery regressions. Verify that a
   newly listed remote table is not hidden by older packaged metadata and that
   existing selection, date and batch-template behavior remains valid.
5. Update README/MkDocs and the changelog, rebuild the wheel and sdist, run tests
   and package checks, and verify the built metadata before releasing. Preserve
   the policy excluding `data_dict_available.xlsx` from distributions.

Done means both the confirmed CBS export and a verified package release are
recorded. Neither an export request nor a package rebuild alone closes this work.

## Ongoing dataset-change checks

Start with a manual weekly review while the metadata release is pending, and review
again before each release and after provider/CBS export notifications. These are
maintenance expectations, not an installed scheduler.

For DSET-003, compare a fresh full inventory from a new non-interactive session
with the last approved baseline. `tables_available(reset=True)` restores cached
inventory; it is not a remote refresh. Use `interactive=False` and
`server_cleanup=False`; inventory checks must not process records or delete
exports. Discovery may stage its small export marker locally.

- Compare product/table/format identities and report additions, removals and
  location changes separately from normal export-version/timestamp changes.
- Reconcile licensed/available metadata against the approved CBS scope, not
  against every entry in the broad `data_dict.xlsx` dictionary.
- Record check time/timezone, package version, export identifiers, marker
  timestamps, provider-confirmed effective dates when known, and evidence for
  each decision. Missing dates remain unknown, not inferred from filenames.
- Where authorized, inspect schemas without retaining researcher records and
  report column/type changes separately from inventory changes.
- Preserve the last good baseline when discovery fails. Mark the check failed
  or unverified rather than reporting an empty inventory as removed datasets.
- Have the data owner review drift before updating the baseline or metadata.
  Do not automatically expand approved researcher extraction scopes; existing
  fail-closed inventory checks should continue to require explicit approval.

DSET-004 should run the same read-only comparison on an approved private runner
using managed credentials, with alerts for actionable drift or failed checks.
Keep live SFTP checks separate from public, credential-free unit-test CI. Tests
should cover added/removed tables, duplicate format variants, export rollover,
metadata mismatches and connection failures without exposing source records.

## Review log

| Reviewed | Evidence / outcome | Next action |
| --- | --- | --- |
| 2026-10-27 | Handoff and user report: no `links_2025` on CBS SFTP; release 1.4.13 metadata also lacks it. No provider schedule or new live SFTP check confirmed here. | CBS data owner to confirm upstream availability and arrange export; package update remains gated on verified delivery. |
| 2026-10-29 | User decision: retire CBS CSV service. Remove its fallback account and catalog entries; keep Parquet and local/custom CSV workflows. No live SFTP check or server-side deletion performed. | Validate package changes and include in the next release; track the 2025 export separately. |
| 2026-10-29 | CBS SFTP owner confirmed `links_2025` has been exported. Local inspection confirms the user's new Parquet catalog row and 21 Ownership History entries. README/MkDocs updated; no independent live SFTP check performed. | DSET-001 delivery complete. Finish DSET-002 metadata/schema reconciliation, tests and release; obtain provider snapshot cutoff/revision details. |
| 2026-10-29 | Extended the 18 shared ownership-link column entries in each dictionary to `links_2025`; added its date-helper and batch-template rows from existing annual-link metadata. Preserved user catalog edits, types, remarks, flags and native Excel formatting/filters. Validation: 24 metadata tests and all 490 repository tests passed; Ruff passed for the updated tests, and offline public dictionary/date lookups succeeded. No live schema check performed. | Build/release and perform an authorized live schema smoke check. Existing custom batch workbooks remain unchanged. |

After each review, append evidence here and update the delivery queue. On
completion, record the export reference/effective date and package release,
then move the item out of the active queue. Keep unresolved dates explicit.
