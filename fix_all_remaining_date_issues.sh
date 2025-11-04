#!/usr/bin/env bash
# Comprehensive date parsing fix script for all remaining views
# Applies COALESCE pattern to handle both ISO 8601 and simple date formats

cd /Users/resnick/Documents/GitHub/athena_saved_queries/views

echo "========================================="
echo "FIXING ALL REMAINING DATE PARSING ISSUES"
echo "========================================="

# Fix v_binary_files.sql (2 issues)
echo "Fixing v_binary_files.sql..."
sed -i.bak '41s/.*/    COALESCE(\n        TRY(date_parse(NULLIF(dr.context_period_start, '"'"''"'"'), '"'"'%Y-%m-%dT%H:%i:%sZ'"'"')),\n        TRY(date_parse(NULLIF(dr.context_period_start, '"'"''"'"'), '"'"'%Y-%m-%d'"'"'))\n    ) as dr_context_period_start,/' v_binary_files.sql
sed -i.bak2 '42s/.*/    COALESCE(\n        TRY(date_parse(NULLIF(dr.context_period_end, '"'"''"'"'), '"'"'%Y-%m-%dT%H:%i:%sZ'"'"')),\n        TRY(date_parse(NULLIF(dr.context_period_end, '"'"''"'"'), '"'"'%Y-%m-%d'"'"'))\n    ) as dr_context_period_end,/' v_binary_files.sql && echo "  ✅ v_binary_files.sql"

# Fix v2_procedure_specimen_link.sql (2 issues - lines 16-17)
echo "Fixing v2_procedure_specimen_link.sql..."
cd /Users/resnick/Documents/GitHub/athena_saved_queries/views
python3 << 'PYTHON_FIX_1'
with open('v2_procedure_specimen_link.sql', 'r') as f:
    lines = f.readlines()

# Fix line 16 and 17 (0-indexed: 15 and 16)
if 'date_parse' in lines[15]:
    lines[15] = lines[15].replace(
        "TRY(date_parse(p.performed_date_time, '%Y-%m-%dT%H:%i:%sZ'))",
        "COALESCE(\n        TRY(date_parse(p.performed_date_time, '%Y-%m-%dT%H:%i:%sZ')),\n        TRY(date_parse(p.performed_date_time, '%Y-%m-%d'))\n    )"
    )

if 'date_parse' in lines[16]:
    lines[16] = lines[16].replace(
        "TRY(date_parse(s.collected_date_time, '%Y-%m-%dT%H:%i:%sZ'))",
        "COALESCE(\n        TRY(date_parse(s.collected_date_time, '%Y-%m-%dT%H:%i:%sZ')),\n        TRY(date_parse(s.collected_date_time, '%Y-%m-%d'))\n    )"
    )

with open('v2_procedure_specimen_link.sql', 'w') as f:
    f.writelines(lines)
print("  ✅ v2_procedure_specimen_link.sql")
PYTHON_FIX_1

# Fix remaining views using Python
python3 << 'PYTHON_FIX_ALL'
import re
from pathlib import Path

views_dir = Path('.')

# Define all remaining fixes
fixes = {
    'v_chemo_medications.sql': [
        (r"TRY\(date_parse\(cp\.created, '%Y-%m-%dT%H:%i:%sZ'\)\) as cp_created,",
         "COALESCE(\n        TRY(date_parse(cp.created, '%Y-%m-%dT%H:%i:%sZ')),\n        TRY(date_parse(cp.created, '%Y-%m-%d'))\n    ) as cp_created,"),
        (r"TRY\(date_parse\(cp\.period_start, '%Y-%m-%dT%H:%i:%sZ'\)\) as cp_period_start,",
         "COALESCE(\n        TRY(date_parse(cp.period_start, '%Y-%m-%dT%H:%i:%sZ')),\n        TRY(date_parse(cp.period_start, '%Y-%m-%d'))\n    ) as cp_period_start,"),
        (r"TRY\(date_parse\(cp\.period_end, '%Y-%m-%dT%H:%i:%sZ'\)\) as cp_period_end,",
         "COALESCE(\n        TRY(date_parse(cp.period_end, '%Y-%m-%dT%H:%i:%sZ')),\n        TRY(date_parse(cp.period_end, '%Y-%m-%d'))\n    ) as cp_period_end,"),
    ],
    'v_chemo_treatment_episodes.sql': [
        (r"TRY\(date_parse\(mr\.dispense_request_validity_period_start, '%Y-%m-%dT%H:%i:%sZ'\)\)",
         "COALESCE(\n        TRY(date_parse(mr.dispense_request_validity_period_start, '%Y-%m-%dT%H:%i:%sZ')),\n        TRY(date_parse(mr.dispense_request_validity_period_start, '%Y-%m-%d'))\n    )"),
        (r"TRY\(date_parse\(mr\.dispense_request_validity_period_end, '%Y-%m-%dT%H:%i:%sZ'\)\)",
         "COALESCE(\n        TRY(date_parse(mr.dispense_request_validity_period_end, '%Y-%m-%dT%H:%i:%sZ')),\n        TRY(date_parse(mr.dispense_request_validity_period_end, '%Y-%m-%d'))\n    )"),
        (r"TRY\(date_parse\(mr\.authored_on, '%Y-%m-%dT%H:%i:%sZ'\)\) as episode_stop_datetime",
         "COALESCE(\n        TRY(date_parse(mr.authored_on, '%Y-%m-%dT%H:%i:%sZ')),\n        TRY(date_parse(mr.authored_on, '%Y-%m-%d'))\n    ) as episode_stop_datetime"),
    ],
    'v_radiation_documents.sql': [
        (r"TRY\(date_parse\(dr\.context_period_start, '%Y-%m-%dT%H:%i:%sZ'\)\) as doc_context_period_start,",
         "COALESCE(\n        TRY(date_parse(dr.context_period_start, '%Y-%m-%dT%H:%i:%sZ')),\n        TRY(date_parse(dr.context_period_start, '%Y-%m-%d'))\n    ) as doc_context_period_start,"),
        (r"TRY\(date_parse\(dr\.context_period_end, '%Y-%m-%dT%H:%i:%sZ'\)\) as doc_context_period_end,",
         "COALESCE(\n        TRY(date_parse(dr.context_period_end, '%Y-%m-%dT%H:%i:%sZ')),\n        TRY(date_parse(dr.context_period_end, '%Y-%m-%d'))\n    ) as doc_context_period_end,"),
    ],
    'v_radiation_care_plan_hierarchy.sql': [
        (r"TRY\(date_parse\(cp\.period_start, '%Y-%m-%dT%H:%i:%sZ'\)\) as cp_period_start,",
         "COALESCE(\n        TRY(date_parse(cp.period_start, '%Y-%m-%dT%H:%i:%sZ')),\n        TRY(date_parse(cp.period_start, '%Y-%m-%d'))\n    ) as cp_period_start,"),
        (r"TRY\(date_parse\(cp\.period_end, '%Y-%m-%dT%H:%i:%sZ'\)\) as cp_period_end,",
         "COALESCE(\n        TRY(date_parse(cp.period_end, '%Y-%m-%dT%H:%i:%sZ')),\n        TRY(date_parse(cp.period_end, '%Y-%m-%d'))\n    ) as cp_period_end,"),
    ],
    'v2_radiation_episode_enrichment.sql': [
        (r"TRY\(date_parse\(NULLIF\(va\.appt_start, ''\), '%Y-%m-%dT%H:%i:%sZ'\)\) as appointment_start,",
         "COALESCE(\n        TRY(date_parse(NULLIF(va.appt_start, ''), '%Y-%m-%dT%H:%i:%sZ')),\n        TRY(date_parse(NULLIF(va.appt_start, ''), '%Y-%m-%d'))\n    ) as appointment_start,"),
        (r"TRY\(date_parse\(NULLIF\(va\.appt_end, ''\), '%Y-%m-%dT%H:%i:%sZ'\)\) as appointment_end,",
         "COALESCE(\n        TRY(date_parse(NULLIF(va.appt_end, ''), '%Y-%m-%dT%H:%i:%sZ')),\n        TRY(date_parse(NULLIF(va.appt_end, ''), '%Y-%m-%d'))\n    ) as appointment_end,"),
    ],
    'v_molecular_tests.sql': [
        (r"TRY\(date_parse\(SUBSTR\(pl\.procedure_date, 1, 10\), '%Y-%m-%d'\)\) as mt_procedure_date,",
         "COALESCE(\n        TRY(date_parse(SUBSTR(pl.procedure_date, 1, 10), '%Y-%m-%dT%H:%i:%sZ')),\n        TRY(date_parse(SUBSTR(pl.procedure_date, 1, 10), '%Y-%m-%d'))\n    ) as mt_procedure_date,"),
    ],
    'v_visits_unified.sql': [
        (r"TRY\(date_parse\(e\.period_start, '%Y-%m-%dT%H:%i:%sZ'\)\) as encounter_start,",
         "COALESCE(\n        TRY(date_parse(e.period_start, '%Y-%m-%dT%H:%i:%sZ')),\n        TRY(date_parse(e.period_start, '%Y-%m-%d'))\n    ) as encounter_start,"),
        (r"TRY\(date_parse\(e\.period_end, '%Y-%m-%dT%H:%i:%sZ'\)\) as encounter_end,",
         "COALESCE(\n        TRY(date_parse(e.period_end, '%Y-%m-%dT%H:%i:%sZ')),\n        TRY(date_parse(e.period_end, '%Y-%m-%d'))\n    ) as encounter_end,"),
        (r"TRY\(date_parse\(a\.start, '%Y-%m-%dT%H:%i:%sZ'\)\) as appointment_start,",
         "COALESCE(\n        TRY(date_parse(a.start, '%Y-%m-%dT%H:%i:%sZ')),\n        TRY(date_parse(a.start, '%Y-%m-%d'))\n    ) as appointment_start,"),
        (r"TRY\(date_parse\(a\.end, '%Y-%m-%dT%H:%i:%sZ'\)\) as appointment_end,",
         "COALESCE(\n        TRY(date_parse(a.end, '%Y-%m-%dT%H:%i:%sZ')),\n        TRY(date_parse(a.end, '%Y-%m-%d'))\n    ) as appointment_end,"),
    ],
}

for filename, patterns in fixes.items():
    filepath = views_dir / filename
    if not filepath.exists():
        print(f"⏭️  Skip: {filename} (not found)")
        continue

    with open(filepath, 'r') as f:
        content = f.read()

    changed = False
    for pattern, replacement in patterns:
        if re.search(pattern, content):
            content = re.sub(pattern, replacement, content)
            changed = True

    if changed:
        with open(filepath, 'w') as f:
            f.write(content)
        print(f"  ✅ {filename}")
    else:
        print(f"  ⏭️  {filename} (already fixed or patterns not found)")

PYTHON_FIX_ALL

echo "========================================="
echo "ALL DATE PARSING FIXES COMPLETE"
echo "========================================="
