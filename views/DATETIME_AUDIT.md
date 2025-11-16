# Athena View Datetime Audit (2025-11-15)

This document inventories datetime usage across `athena_saved_queries/views` and cross-references the `athena_fhir_prd_db_schema_11152025.csv` schema snapshot. It highlights high-risk casting patterns (e.g., `CAST(timestamp AS DATE)`) that can return `NULL` in Athena/Presto when timezone metadata is present, and proposes consistent transformation strategies for the `v_*.sql` and `v2_*.sql` views.

## 1. View-Level Pattern Summary

| View SQL | # date/time tokens | `CAST(... AS DATE)` from column | `CAST(SUBSTR(... ) AS DATE)` | `FROM_ISO8601_TIMESTAMP` calls | `DATE()` calls | `DATE_PARSE()` calls |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| v2_appointments.sql | 7 | 0 | 1 | 0 | 1 | 2 |
| v2_document_reference_enriched.sql | 2 | 0 | 0 | 0 | 0 | 0 |
| v2_imaging.sql | 27 | 0 | 0 | 2 | 4 | 6 |
| v2_procedure_specimen_link.sql | 6 | 0 | 0 | 0 | 2 | 2 |
| v2_procedures_tumor.sql | 24 | 0 | 2 | 0 | 1 | 8 |
| v2_radiation_episode_enrichment.sql | 66 | 0 | 0 | 0 | 0 | 4 |
| v2_radiation_treatment_episodes.sql | 109 | 0 | 0 | 0 | 0 | 0 |
| v_audiology_assessments.sql | 77 | 0 | 9 | 0 | 0 | 0 |
| v_autologous_stem_cell_collection.sql | 70 | 0 | 0 | 0 | 8 | 0 |
| v_autologous_stem_cell_transplant.sql | 57 | 0 | 0 | 0 | 0 | 0 |
| v_binary_files.sql | 15 | 0 | 0 | 2 | 2 | 4 |
| v_chemo_medications.sql | 12 | 0 | 0 | 0 | 0 | 6 |
| v_chemo_treatment_episodes.sql | 56 | 0 | 0 | 0 | 0 | 3 |
| v_concomitant_medications.sql | 157 | 0 | 6 | 0 | 3 | 0 |
| v_encounters.sql | 10 | 0 | 2 | 0 | 1 | 4 |
| v_hydrocephalus_diagnosis.sql | 42 | 0 | 0 | 0 | 1 | 0 |
| v_hydrocephalus_procedures.sql | 21 | 0 | 0 | 0 | 1 | 0 |
| v_imaging_corticosteroid_use.sql | 103 | 0 | 0 | 0 | 28 | 0 |
| v_measurements.sql | 30 | 0 | 0 | 1 | 4 | 0 |
| v_medications.sql | 16 | 0 | 0 | 0 | 0 | 14 |
| v_molecular_tests.sql | 23 | 0 | 1 | 2 | 1 | 2 |
| v_ophthalmology_assessments.sql | 78 | 0 | 9 | 0 | 0 | 0 |
| v_pathology_diagnostics.sql | 112 | **11** | 0 | 6 | 3 | 0 |
| v_patient_demographics.sql | 9 | 0 | 0 | 0 | 1 | 2 |
| v_problem_list_diagnoses.sql | 107 | 0 | 0 | 0 | 0 | 31 |
| v_radiation_care_plan_hierarchy.sql | 3 | 0 | 0 | 0 | 0 | 3 |
| v_radiation_documents.sql | 19 | 0 | 0 | 4 | 0 | 4 |
| v_radiation_summary.sql | 42 | 0 | 0 | 0 | 1 | 0 |
| v_radiation_treatment_appointments.sql | 32 | 0 | 0 | 0 | 0 | 0 |
| v_radiation_treatments.sql | 142 | 0 | 0 | 0 | 0 | 0 |
| v_unified_patient_timeline.sql | 111 | **6** | 0 | 3 | 6 | 0 |
| v_visits_unified.sql | 23 | 0 | 0 | 0 | 4 | 12 |

> **How to read this table**: high counts in the `CAST(... AS DATE)` and `CAST(SUBSTR(... ) AS DATE)` columns flag spots where TIMESTAMP WITH TIME ZONE values are cast directly to `DATE`, which is where Athena returns `NULL`. The other columns show whether ISO8601 parsing or generic `DATE()` wrapping is already in use.

## 2. Schema cross-reference for datetime columns

Parsing `athena_fhir_prd_db_schema_11152025.csv` produced 129 datetime-aware columns. Representative examples that feed the views:

| Base table / view | Column | Declared type | Notes |
| --- | --- | --- | --- |
| `procedure` | `performed_date_time` | `varchar` | ISO8601 string; consumers must parse before converting to `DATE`. |
| `specimen` | `collection_collected_date_time` | `varchar` | Same ISO8601 requirement. |
| `observation` | `effective_date_time` | `varchar` | Should go through `FROM_ISO8601_TIMESTAMP`. |
| `diagnostic_report` | `effective_date_time` | `varchar` | Same as above. |
| `patient` | `birth_date` | `varchar` | Already `YYYY-MM-DD` but stored as string. |
| `v_procedures_tumor` | `proc_performed_date_time` | `timestamp(3)` | Already normalized TIMESTAMP; prefer `DATE(proc_performed_date_time)` instead of `CAST(... AS DATE)`. |
| `v_radiation_treatment_appointments` | `appointment_start` | `timestamp(3)` | Downstream views should call `DATE(appointment_start)`. |
| `v_molecular_tests` | `mt_test_date` | `date` | Already truncated to date; no extra cast required. |
| `v_pathology_diagnostics` | `diagnostic_datetime` | `timestamp(3)` | Derived via `from_iso8601_timestamp`. |
| `v_concomitant_medications` | `start_datetime` | `timestamp(3)` | Already TIMESTAMP; `DATE()` works directly. |

A full export of the 129 rows grouped by table is available via the analysis script (`schema_datetime_summary.py`, see instructions below) and can be regenerated whenever the Athena schema changes.

## 3. High-risk casting hotspots

### 3.1 `v_pathology_diagnostics.sql`

Problematic lines (current implementation):

```sql
CAST(proc_performed_date_time AS DATE) as surgery_date,
TRY(CAST(CAST(s.collection_collected_date_time AS VARCHAR) AS DATE)) as collection_date,
TRY(CAST(CAST(o.effective_date_time AS VARCHAR) AS DATE)) as diagnostic_date,
...
CAST(ud.linked_procedure_datetime AS DATE) as linked_procedure_date
```

Why it breaks:

* `proc_performed_date_time` already arrives as `TIMESTAMP(3)` from `v_procedures_tumor`; the direct `CAST(... AS DATE)` returns `NULL` when upstream encoded timezone metadata.
* `specimen.collection_collected_date_time` and `observation.effective_date_time` are stored as VARCHAR ISO8601 timestamps. The current double-cast `CAST(CAST(col AS VARCHAR) AS DATE)` bypasses timezone handling.

Recommended replacements:

```sql
DATE(proc_performed_date_time) as surgery_date,
DATE(TRY(from_iso8601_timestamp(s.collection_collected_date_time))) as collection_date,
DATE(TRY(from_iso8601_timestamp(o.effective_date_time))) as diagnostic_date,
DATE(ud.linked_procedure_datetime) as linked_procedure_date
```

### 3.2 `v_unified_patient_timeline.sql`

Problematic lines:

```sql
CAST(vp.procedure_date AS DATE) as event_date,
CAST(vi.imaging_date AS DATE) as event_date,
CAST(TRY(FROM_ISO8601_TIMESTAMP(vcm.medication_start_date)) AS DATE) as event_date,
CAST(vmt.mt_test_date AS DATE) as event_date,
CAST(vrta.appointment_start AS DATE) as event_date
```

Issues & fixes:

* `vp.procedure_date`, `vi.imaging_date`, `vmt.mt_test_date`, and `vrta.appointment_start` are already typed as DATE or TIMESTAMP in their source views. Prefer wrapping with `DATE(...)` rather than `CAST(... AS DATE)`.
* Medication dates come in as VARCHAR ISO8601 strings. Converting to TIMESTAMP first (via `FROM_ISO8601_TIMESTAMP`) is correct; follow up with `DATE(...)` to drop the time component without hitting the `CAST` bug:

```sql
DATE(vp.procedure_date) as event_date,
DATE(vi.imaging_date) as event_date,
DATE(TRY(FROM_ISO8601_TIMESTAMP(vcm.medication_start_date))) as event_date,
DATE(vmt.mt_test_date) as event_date,
DATE(vrta.appointment_start) as event_date
```

### 3.3 `v_audiology_assessments.sql`, `v_ophthalmology_assessments.sql`, `v_concomitant_medications.sql`, `v_encounters.sql`

These views rely heavily on `CAST(SUBSTR(col, 1, 10) AS DATE)` to strip the date portion. While it works for strictly ISO8601 strings, it fails when timezone offsets or milliseconds are present (e.g., `2024-03-01T10:54:16.000-07:00`). Swap the substring with explicit parsing:

```sql
DATE(TRY(from_iso8601_timestamp(o.effective_date_time))) as assessment_date
```

For `start_datetime`/`stop_datetime` columns that are already TIMESTAMP(3) (per schema), drop the substring entirely and call `DATE(start_datetime)`.

### 3.4 `v_molecular_tests.sql`

Mixes multiple patterns:

* `CAST(FROM_ISO8601_TIMESTAMP(mt.result_datetime) AS DATE)` should become `DATE(TRY(from_iso8601_timestamp(mt.result_datetime)))`.
* `TRY(CAST(SUBSTR(sl.specimen_collection_date, 1, 10) AS TIMESTAMP(3)))` should be `TRY(from_iso8601_timestamp(sl.specimen_collection_date))` with a trailing `DATE()` when you only need the date.

## 4. Standardization playbook

1. **Always land on TIMESTAMP(3)**
   * Strings with timezone → `TRY(from_iso8601_timestamp(col))`
   * Strings without timezone → `TRY(date_parse(col, '%Y-%m-%d %H:%i:%s'))`
   * Numeric epoch → `from_unixtime(col)`

2. **Truncate to DATE without CAST**
   * Use `DATE(normalized_ts)` instead of `CAST(normalized_ts AS DATE)`.

3. **Wrap risky expressions in `TRY(...)`**
   * Prevents entire row failures when malformed timestamps appear.

4. **Centralize conversion snippets** (proposed helper CTE):

```sql
WITH datetime_norm AS (
    SELECT
        TRY(from_iso8601_timestamp(proc_performed_date_time)) AS proc_ts,
        DATE(TRY(from_iso8601_timestamp(proc_performed_date_time))) AS proc_date
    FROM ...
)
```

5. **Document source types**
   * Keep a lightweight lookup (derived from the CSV) that states “`procedure.performed_date_time` = VARCHAR (ISO8601)”. Developers can confirm the correct parser at a glance.

## 5. Regenerating the audit

Re-run the helper script to refresh the tables above after altering views or refreshing the Athena schema snapshot:

```bash
cd /Users/resnick/Documents/GitHub
python scripts/audit_datetime_usage.py  # see snippet below
```

`audit_datetime_usage.py` example skeleton:

```python
import pathlib, re, csv
BASE = pathlib.Path('athena_saved_queries/views')
SCHEMA = pathlib.Path('/Users/resnick/Downloads/athena_fhir_prd_db_schema_11152025.csv')
# ...collect stats shown in sections 1–2...
```

Feel free to adapt the script to emit CSV/JSON for downstream QA pipelines.

## 6. Implementation backlog (prioritized)

| Priority | View | Work item | Notes |
| --- | --- | --- | --- |
| 🔴 | `v_pathology_diagnostics.sql` | Replace every `CAST(... AS DATE)` with `DATE(TRY(from_iso8601_timestamp(...)))` or `DATE(timestamp_col)` as outlined in §3.1. | Blocks downstream `v_unified_patient_timeline` joins because `surgery_date` becomes `NULL`. |
| 🔴 | `v_unified_patient_timeline.sql` | Normalize each event block to call `DATE(...)` (procedures, imaging, medications, lab tests, appointments). | Central timeline consumers (BRIM workflows) ingest these fields. |
| 🟠 | `v_audiology_assessments.sql`, `v_ophthalmology_assessments.sql` | Replace `SUBSTR(..., 1, 10)` truncations with `DATE(TRY(from_iso8601_timestamp(...)))`. | Prevents silent NULLs when milliseconds/timezones appear. |
| 🟠 | `v_concomitant_medications.sql` | Drop string truncation; rely on `DATE(start_datetime)`/`DATE(stop_datetime)` because source CTE already casts to TIMESTAMP. | Simplifies steroid/imaging overlap logic. |
| 🟠 | `v_molecular_tests.sql` | Use `DATE(TRY(from_iso8601_timestamp(mt.result_datetime)))` and normalize specimen collection timestamps. | Aligns with `v_pathology_diagnostics` specimen handling. |
| 🟢 | `v2_*` family | Audit new `v2_` queries whenever a datetime column is added; enforce helper macros before PR approval. | Prevent regression as new UCSF feeds land. |

Legend: 🔴 = blocking downstream workflows today, 🟠 = high priority but with mitigations, 🟢 = preventive/guardrail work.
