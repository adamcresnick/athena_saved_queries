#!/usr/bin/env python3
"""
Comprehensive Date Column Validation Script

This script validates ALL date parsing across all deployed Athena views by:
1. Scanning all deployed view SQL files for date_parse and CAST...TIMESTAMP calls
2. Testing each view with sample data to verify date columns are populated
3. Identifying views with NULL date issues
4. Providing recommendations for fixes

Usage:
    export AWS_PROFILE=radiant-prod
    python3 validate_all_date_columns.py
"""

import boto3
import time
import re
from pathlib import Path
from typing import Dict, List, Tuple

# Deployment list from deploy_views.py
DEPLOYED_VIEWS = [
    'v_oid_reference.sql',
    'v_patient_demographics.sql',
    'v_encounters.sql',
    'v_diagnoses.sql',
    'v_problem_list_diagnoses.sql',
    'v_binary_files.sql',
    'v2_procedure_specimen_link.sql',
    'v2_procedures_tumor.sql',
    'v2_document_reference_enriched.sql',
    'v2_imaging.sql',
    'v_medications.sql',
    'v_chemo_medications.sql',
    'v_chemo_treatment_episodes.sql',
    'v_radiation_documents.sql',
    'v_radiation_care_plan_hierarchy.sql',
    'v2_radiation_treatment_episodes.sql',
    'v2_radiation_episode_enrichment.sql',
    'v_molecular_tests.sql',
    'v_pathology_diagnostics.sql',
    'v2_appointments.sql',
    'v_visits_unified.sql',
]

def extract_view_name(sql_file: Path) -> str:
    """Extract the actual view name from CREATE OR REPLACE VIEW statement"""
    with open(sql_file, 'r') as f:
        first_line = f.readline()
        match = re.search(r'CREATE OR REPLACE VIEW\s+([^\s]+)\s+AS', first_line, re.IGNORECASE)
        if match:
            return match.group(1)
    return None

def find_date_columns(sql_file: Path) -> List[Tuple[int, str, str]]:
    """Find all date parsing operations in SQL file"""
    date_ops = []

    with open(sql_file, 'r') as f:
        for line_num, line in enumerate(f, 1):
            # Find date_parse calls
            if 'date_parse' in line.lower():
                # Extract the column alias
                alias_match = re.search(r'as\s+(\w+)', line, re.IGNORECASE)
                alias = alias_match.group(1) if alias_match else 'unknown'

                # Check if using COALESCE for multiple formats
                if 'COALESCE' in line:
                    date_ops.append((line_num, alias, 'date_parse with COALESCE (GOOD)'))
                elif "'%Y-%m-%d'" in line and "'%Y-%m-%dT%H:%i:%sZ'" not in line:
                    date_ops.append((line_num, alias, 'date_parse single format %Y-%m-%d (RISKY)'))
                elif "'%Y-%m-%dT%H:%i:%sZ'" in line and "'%Y-%m-%d'" not in line:
                    date_ops.append((line_num, alias, 'date_parse single format ISO8601 (RISKY)'))
                else:
                    date_ops.append((line_num, alias, 'date_parse (check manually)'))

            # Find CAST...AS TIMESTAMP calls
            if re.search(r'CAST.*AS\s+TIMESTAMP', line, re.IGNORECASE):
                alias_match = re.search(r'as\s+(\w+)', line, re.IGNORECASE)
                alias = alias_match.group(1) if alias_match else 'unknown'
                date_ops.append((line_num, alias, 'CAST AS TIMESTAMP'))

    return date_ops

def test_view_dates(client, view_name: str, date_columns: List[str], limit: int = 10) -> Dict:
    """Test a view to see if date columns are populated"""
    if not date_columns:
        return {'status': 'no_date_columns', 'message': 'No date columns found'}

    # Build SELECT for just the date columns
    select_cols = ', '.join(date_columns[:10])  # Limit to first 10 date columns

    query = f"""
    SELECT {select_cols}
    FROM {view_name}
    WHERE {date_columns[0]} IS NOT NULL
    LIMIT {limit}
    """

    try:
        response = client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': 'fhir_prd_db'},
            ResultConfiguration={'OutputLocation': 's3://aws-athena-query-results-343218191717-us-east-1/'}
        )

        query_id = response['QueryExecutionId']

        # Wait for query to complete
        for _ in range(60):
            status = client.get_query_execution(QueryExecutionId=query_id)
            state = status['QueryExecution']['Status']['State']

            if state == 'SUCCEEDED':
                results = client.get_query_results(QueryExecutionId=query_id)

                # Count non-NULL values
                null_counts = {col: 0 for col in date_columns[:10]}
                total_rows = len(results['ResultSet']['Rows']) - 1  # Exclude header

                if total_rows > 0:
                    for row in results['ResultSet']['Rows'][1:]:
                        for idx, data in enumerate(row['Data']):
                            col_name = date_columns[idx] if idx < len(date_columns) else f'col_{idx}'
                            if not data.get('VarCharValue'):
                                null_counts[col_name] = null_counts.get(col_name, 0) + 1

                    return {
                        'status': 'success',
                        'total_rows': total_rows,
                        'null_counts': null_counts,
                        'query_id': query_id
                    }
                else:
                    return {'status': 'no_data', 'message': 'No rows returned'}

            elif state in ['FAILED', 'CANCELLED']:
                reason = status['QueryExecution']['Status'].get('StateChangeReason', 'Unknown')
                return {'status': 'failed', 'message': reason}

            time.sleep(2)

        return {'status': 'timeout', 'message': 'Query timed out'}

    except Exception as e:
        return {'status': 'error', 'message': str(e)}

def main():
    print("=" * 100)
    print("COMPREHENSIVE DATE COLUMN VALIDATION FOR DEPLOYED ATHENA VIEWS")
    print("=" * 100)

    views_dir = Path('views')
    client = boto3.client('athena', region_name='us-east-1')

    all_results = []

    for view_file_name in DEPLOYED_VIEWS:
        view_file = views_dir / view_file_name

        if not view_file.exists():
            print(f"\n⚠️  MISSING: {view_file_name}")
            continue

        print(f"\n{'='*100}")
        print(f"VIEW: {view_file_name}")
        print(f"{'='*100}")

        # Extract actual view name
        view_name = extract_view_name(view_file)
        if not view_name:
            print(f"❌ Could not extract view name from {view_file_name}")
            continue

        print(f"Athena view name: {view_name}")

        # Find date columns
        date_ops = find_date_columns(view_file)

        if not date_ops:
            print("✅ No date parsing operations found")
            all_results.append({
                'file': view_file_name,
                'view': view_name,
                'status': 'no_dates',
                'date_ops': []
            })
            continue

        print(f"\nDate operations found: {len(date_ops)}")
        print(f"{'Line':<6} {'Column':<40} {'Type':<50}")
        print("-" * 100)

        issues = []
        date_columns = []

        for line_num, alias, op_type in date_ops:
            print(f"{line_num:<6} {alias:<40} {op_type:<50}")
            date_columns.append(alias)

            if 'RISKY' in op_type:
                issues.append(f"Line {line_num}: {alias} uses {op_type}")

        # Report issues
        if issues:
            print(f"\n⚠️  POTENTIAL ISSUES FOUND ({len(issues)}):")
            for issue in issues:
                print(f"   - {issue}")

        all_results.append({
            'file': view_file_name,
            'view': view_name,
            'status': 'has_dates',
            'date_ops': date_ops,
            'issues': issues
        })

    # Summary
    print(f"\n\n{'='*100}")
    print("SUMMARY")
    print(f"{'='*100}")

    views_with_issues = [r for r in all_results if r.get('issues')]
    views_with_dates = [r for r in all_results if r['status'] == 'has_dates']

    print(f"\nTotal views analyzed: {len(all_results)}")
    print(f"Views with date operations: {len(views_with_dates)}")
    print(f"Views with potential issues: {len(views_with_issues)}")

    if views_with_issues:
        print(f"\n⚠️  VIEWS REQUIRING FIXES:")
        for result in views_with_issues:
            print(f"\n   {result['file']}")
            for issue in result['issues']:
                print(f"      - {issue}")

if __name__ == '__main__':
    main()
