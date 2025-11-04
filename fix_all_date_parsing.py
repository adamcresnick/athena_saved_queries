#!/usr/bin/env python3
"""
Batch fix all date parsing issues across deployed views.

This script applies the COALESCE pattern to handle both ISO 8601 and simple date formats:
    COALESCE(
        TRY(date_parse(field, '%Y-%m-%dT%H:%i:%sZ')),
        TRY(date_parse(field, '%Y-%m-%d'))
    ) as field_name

Based on validation report findings.
"""

from pathlib import Path
import re

# Define fixes for each view
FIXES = {
    'v_patient_demographics.sql': {
        'line': 7,
        'old': "    TRY(date_parse(pa.birth_date, '%Y-%m-%d')) as pd_birth_date,",
        'new': """    COALESCE(
        TRY(date_parse(pa.birth_date, '%Y-%m-%dT%H:%i:%sZ')),
        TRY(date_parse(pa.birth_date, '%Y-%m-%d'))
    ) as pd_birth_date,"""
    },
    'v2_appointments.sql': {
        'line': 4,
        'old': "    TRY(date_parse(NULLIF(a.start, ''), '%Y-%m-%d')) as appointment_date,",
        'new': """    COALESCE(
        TRY(date_parse(NULLIF(a.start, ''), '%Y-%m-%dT%H:%i:%sZ')),
        TRY(date_parse(NULLIF(a.start, ''), '%Y-%m-%d'))
    ) as appointment_date,"""
    },
}

# Multi-line fixes (need to read and replace larger blocks)
MULTI_LINE_FIXES = {
    'v_encounters.sql': [
        {
            'old_pattern': r"TRY\(date_parse\(e\.period_start, '%Y-%m-%dT%H:%i:%sZ'\)\) as period_start,",
            'new': """COALESCE(
        TRY(date_parse(e.period_start, '%Y-%m-%dT%H:%i:%sZ')),
        TRY(date_parse(e.period_start, '%Y-%m-%d'))
    ) as period_start,"""
        },
        {
            'old_pattern': r"TRY\(date_parse\(e\.period_end, '%Y-%m-%dT%H:%i:%sZ'\)\) as period_end,",
            'new': """COALESCE(
        TRY(date_parse(e.period_end, '%Y-%m-%dT%H:%i:%sZ')),
        TRY(date_parse(e.period_end, '%Y-%m-%d'))
    ) as period_end,"""
        }
    ],
    'v2_imaging.sql': [
        {
            'old_pattern': r"TRY\(date_parse\(dr\.issued, '%Y-%m-%dT%H:%i:%sZ'\)\) as report_issued,",
            'new': """COALESCE(
        TRY(date_parse(dr.issued, '%Y-%m-%dT%H:%i:%sZ')),
        TRY(date_parse(dr.issued, '%Y-%m-%d'))
    ) as report_issued,"""
        },
        {
            'old_pattern': r"TRY\(date_parse\(dr\.effective_period_start, '%Y-%m-%dT%H:%i:%sZ'\)\) as report_effective_period_start,",
            'new': """COALESCE(
        TRY(date_parse(dr.effective_period_start, '%Y-%m-%dT%H:%i:%sZ')),
        TRY(date_parse(dr.effective_period_start, '%Y-%m-%d'))
    ) as report_effective_period_start,"""
        },
        {
            'old_pattern': r"TRY\(date_parse\(dr\.effective_period_stop, '%Y-%m-%dT%H:%i:%sZ'\)\) as report_effective_period_stop,",
            'new': """COALESCE(
        TRY(date_parse(dr.effective_period_stop, '%Y-%m-%dT%H:%i:%sZ')),
        TRY(date_parse(dr.effective_period_stop, '%Y-%m-%d'))
    ) as report_effective_period_stop,"""
        }
    ],
    'v_medications.sql': [
        {
            'old_pattern': r"TRY\(date_parse\(mr\.dispense_request_validity_period_start, '%Y-%m-%dT%H:%i:%sZ'\)\) as mr_validity_period_start,",
            'new': """COALESCE(
        TRY(date_parse(mr.dispense_request_validity_period_start, '%Y-%m-%dT%H:%i:%sZ')),
        TRY(date_parse(mr.dispense_request_validity_period_start, '%Y-%m-%d'))
    ) as mr_validity_period_start,"""
        },
        {
            'old_pattern': r"TRY\(date_parse\(mr\.dispense_request_validity_period_end, '%Y-%m-%dT%H:%i:%sZ'\)\) as mr_validity_period_end,",
            'new': """COALESCE(
        TRY(date_parse(mr.dispense_request_validity_period_end, '%Y-%m-%dT%H:%i:%sZ')),
        TRY(date_parse(mr.dispense_request_validity_period_end, '%Y-%m-%d'))
    ) as mr_validity_period_end,"""
        },
        {
            'old_pattern': r"TRY\(date_parse\(mr\.authored_on, '%Y-%m-%dT%H:%i:%sZ'\)\) as mr_authored_on,",
            'new': """COALESCE(
        TRY(date_parse(mr.authored_on, '%Y-%m-%dT%H:%i:%sZ')),
        TRY(date_parse(mr.authored_on, '%Y-%m-%d'))
    ) as mr_authored_on,"""
        },
        {
            'old_pattern': r"TRY\(date_parse\(cp\.created, '%Y-%m-%dT%H:%i:%sZ'\)\) as cp_created,",
            'new': """COALESCE(
        TRY(date_parse(cp.created, '%Y-%m-%dT%H:%i:%sZ')),
        TRY(date_parse(cp.created, '%Y-%m-%d'))
    ) as cp_created,"""
        },
        {
            'old_pattern': r"TRY\(date_parse\(cp\.period_start, '%Y-%m-%dT%H:%i:%sZ'\)\) as cp_period_start,",
            'new': """COALESCE(
        TRY(date_parse(cp.period_start, '%Y-%m-%dT%H:%i:%sZ')),
        TRY(date_parse(cp.period_start, '%Y-%m-%d'))
    ) as cp_period_start,"""
        },
        {
            'old_pattern': r"TRY\(date_parse\(cp\.period_end, '%Y-%m-%dT%H:%i:%sZ'\)\) as cp_period_end,",
            'new': """COALESCE(
        TRY(date_parse(cp.period_end, '%Y-%m-%dT%H:%i:%sZ')),
        TRY(date_parse(cp.period_end, '%Y-%m-%d'))
    ) as cp_period_end,"""
        }
    ],
}

def apply_fixes(views_dir: Path):
    """Apply all fixes to SQL files"""
    print("=" * 100)
    print("APPLYING DATE PARSING FIXES TO ALL VIEWS")
    print("=" * 100)

    fixed_count = 0

    # Single-line fixes
    for filename, fix_info in FIXES.items():
        filepath = views_dir / filename
        if not filepath.exists():
            print(f"\n⚠️  SKIP: {filename} not found")
            continue

        print(f"\n📝 Fixing: {filename} (line {fix_info['line']})")

        with open(filepath, 'r') as f:
            content = f.read()

        if fix_info['old'] not in content:
            print(f"   ⚠️  WARNING: Old pattern not found, may already be fixed")
            continue

        new_content = content.replace(fix_info['old'], fix_info['new'])

        with open(filepath, 'w') as f:
            f.write(new_content)

        print(f"   ✅ Fixed")
        fixed_count += 1

    # Multi-line regex fixes
    for filename, replacements in MULTI_LINE_FIXES.items():
        filepath = views_dir / filename
        if not filepath.exists():
            print(f"\n⚠️  SKIP: {filename} not found")
            continue

        print(f"\n📝 Fixing: {filename} ({len(replacements)} patterns)")

        with open(filepath, 'r') as f:
            content = f.read()

        changes_made = 0
        for replacement in replacements:
            if re.search(replacement['old_pattern'], content):
                content = re.sub(replacement['old_pattern'], replacement['new'], content)
                changes_made += 1
            else:
                print(f"   ⚠️  Pattern not found (may already be fixed): {replacement['old_pattern'][:60]}...")

        if changes_made > 0:
            with open(filepath, 'w') as f:
                f.write(content)
            print(f"   ✅ Fixed ({changes_made} replacements)")
            fixed_count += 1
        else:
            print(f"   ⚠️  No changes made")

    print(f"\n{'=' * 100}")
    print(f"SUMMARY: Fixed {fixed_count} files")
    print(f"{'=' * 100}")

if __name__ == '__main__':
    views_dir = Path('views')
    apply_fixes(views_dir)
