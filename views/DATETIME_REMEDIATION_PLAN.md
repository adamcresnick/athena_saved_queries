# Athena View Datetime Findings & Remediation Plan (2025-11-15)

## Document Objectives

1. Capture the datetime defects detected in `athena_saved_queries/views/*.sql`.
2. Record validation evidence (schema + Athena samples) proving why each fix is necessary.
3. Provide concrete remediation steps per view (including SQL snippets and rationale).

---

## 1. Executive Summary

- Root cause: mixed datetime handling (direct `CAST(... AS DATE)`, `CAST(SUBSTR(...,1,10) AS DATE)`, and missing ISO8601 parsing) against columns that actually store timezone-aware strings. Athena drops timezone metadata when casting, returning `NULL` for entire rows.
- Impacted flows: downstream `v_unified_patient_timeline`, BRIM extraction workflows, and any logic depending on `surgery_date`, `event_date`, or medication timelines.
- Priority order (blocking first):
	1. `v_pathology_diagnostics.sql`
	2. `v_unified_patient_timeline.sql`
	3. `v_audiology_assessments.sql` + `v_ophthalmology_assessments.sql`
	4. `v_concomitant_medications.sql`
	5. `v_molecular_tests.sql`

---

## 2. Validation Evidence

| Column | Base table/view | Schema type | Sample values | Evidence source |
| --- | --- | --- | --- | --- |
| `observation.effective_date_time` | `fhir_prd_db.observation` | `varchar` | `2012-01-19T12:49:00Z` (10 rows identical) | `/Users/resnick/Downloads/d8540f01-28bc-4a21-bf1e-19490c334332.csv` |
| `specimen.collection_collected_date_time` | `fhir_prd_db.specimen` | `varchar` | `2015-03-12T08:18:24-05:00`, etc. (UTC offsets present) | `/Users/resnick/Downloads/3887c8ec-0e5b-46e3-b711-c3a47537fc05.csv` |
| `v_procedures_tumor.proc_performed_date_time` | `v_procedures_tumor` | `timestamp(3)` | Derived TIMESTAMP (no timezone) | `athena_fhir_prd_db_schema_11152025.csv` |
| `v_concomitant_medications.start_datetime` | `v_concomitant_medications` | `timestamp(3)` | Derived from upstream `TRY(from_iso8601_timestamp(...))` | View definition |

Takeaways:

1. **Raw FHIR columns remain VARCHAR ISO8601** → must use `DATE(TRY(from_iso8601_timestamp(col)))`.
2. **Derived views already emit TIMESTAMP(3)** → use `DATE(timestamp_col)` instead of `CAST(... AS DATE)`.

---

## 3. Per-View Findings & Fixes

### 3.1 `v_pathology_diagnostics.sql`

- **Issue**: Lines 15, 40, 64, 179, 299, 332, 461, 725 use `CAST(... AS DATE)` directly on TIMESTAMP(3) (`proc_performed_date_time`) or double-cast VARCHAR ISO8601 values.
- **Impact**: `surgery_date`, `collection_date`, and `diagnostic_date` become `NULL` when timezone info exists, breaking joins in `v_unified_patient_timeline` and date-difference calculations.
- **Fix**:
	```sql
	DATE(proc_performed_date_time) as surgery_date,
	DATE(TRY(from_iso8601_timestamp(s.collection_collected_date_time))) as collection_date,
	DATE(TRY(from_iso8601_timestamp(o.effective_date_time))) as diagnostic_date,
	DATE(ud.linked_procedure_datetime) as linked_procedure_date
	```
- **Why**: `proc_performed_date_time` is already TIMESTAMP(3); wrapping with `DATE()` preserves values. Raw FHIR columns require ISO8601 parsing to keep timezone info before truncating.

### 3.2 `v_unified_patient_timeline.sql`

- **Issue**: Each event block (`procedures`, `imaging`, `medications`, `labs`, `appointments`) uses `CAST(... AS DATE)` even when the source column is already DATE/TIMESTAMP, or converts ISO strings without parsing.
- **Impact**: Timeline rows show `NULL` `event_date`, causing BRIM workflow mis-ordering.
- **Fix**:
	```sql
	DATE(vp.procedure_date) as event_date,
	DATE(vi.imaging_date) as event_date,
	DATE(TRY(from_iso8601_timestamp(vcm.medication_start_date))) as event_date,
	DATE(vmt.mt_test_date) as event_date,
	DATE(vrta.appointment_start) as event_date
	```
- **Why**: Aligns with actual types (DATE or TIMESTAMP) and enforces ISO parsing only where needed (medications still store VARCHAR string).

### 3.3 `v_audiology_assessments.sql`

- **Issue**: Extensive use of `CAST(SUBSTR(column, 1, 10) AS DATE)` on `observation.effective_date_time`, `condition.onset_date_time`, etc.
- **Impact**: Any value with milliseconds or timezone offset (e.g., `-07:00`) is truncated to invalid strings, returning `NULL` dates.
- **Fix**:
	```sql
	DATE(TRY(from_iso8601_timestamp(o.effective_date_time))) as assessment_date
	```
	Apply the same pattern to `c.onset_date_time`, `p.performed_date_time`, etc.
- **Why**: ISO parsing tolerates milliseconds and offsets; `DATE()` truncates without losing timezone fidelity.

### 3.4 `v_ophthalmology_assessments.sql`

- **Issue**: Same `SUBSTR` pattern as audiology plus redundant casts inside UNION blocks.
- **Fix**: Replace all date derivations with `DATE(TRY(from_iso8601_timestamp(...)))`. Deduplicate repeated expressions by factoring into CTEs if preferred.
- **Why**: Ensures documents/orders with timezone offsets still produce valid assessment dates.

### 3.5 `v_concomitant_medications.sql`

- **Issue**: Standardized start/stop dates ultimately call `CAST(SUBSTR(am.start_datetime, 1, 10) AS DATE)` even though upstream CTE already casts to TIMESTAMP(3).
- **Fix**:
	```sql
	DATE(am.start_datetime) as conmed_start_date,
	DATE(am.stop_datetime) as conmed_stop_date
	```
	Similar for chemo start/stop/authored timestamps.
- **Why**: `start_datetime` columns are TIMESTAMP(3); direct `DATE()` avoids string manipulation and preserves `NULL` semantics.

### 3.6 `v_molecular_tests.sql`

- **Issues**:
	- `CAST(FROM_ISO8601_TIMESTAMP(mt.result_datetime) AS DATE)` lacks `TRY`, failing on malformed strings.
	- `TRY(CAST(SUBSTR(sl.specimen_collection_date, 1, 10) AS TIMESTAMP(3)))` truncates strings rather than parsing.
- **Fix**:
	```sql
	DATE(TRY(from_iso8601_timestamp(mt.result_datetime))) as mt_test_date,
	DATE(TRY(from_iso8601_timestamp(sl.specimen_collection_date))) as mt_specimen_collection_date
	```
- **Why**: Aligns with actual storage (VARCHAR ISO) and prevents hard failures when result strings are missing timezone info.

---

## 4. Implementation Checklist

| Priority | Task | Files | Owner | Status |
| --- | --- | --- | --- | --- |
| 🔴 | Replace direct `CAST(... AS DATE)` and double-casts in pathology view | `v_pathology_diagnostics.sql` | Data Eng | Pending |
| 🔴 | Normalize event dates in unified timeline view | `v_unified_patient_timeline.sql` | Data Eng | Pending |
| 🟠 | Swap all `SUBSTR` casts for ISO parsing in specialty assessments | `v_audiology_assessments.sql`, `v_ophthalmology_assessments.sql` | Data Eng | Pending |
| 🟠 | Leverage TIMESTAMP columns directly in conmed view | `v_concomitant_medications.sql` | Data Eng | Pending |
| 🟠 | Standardize molecular test dates | `v_molecular_tests.sql` | Data Eng | Pending |
| 🟢 | Add lint/check script (`scripts/audit_datetime_usage.py`) to CI to prevent regressions | Repo tooling | Pending |

Completion definition: each view PR replaces risky casts with ISO parsing or `DATE(timestamp)` and includes a targeted Athena regression test (e.g., verifying no `NULL` dates for previously failing patient IDs).
