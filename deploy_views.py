#!/usr/bin/env python3
"""
Deploy Athena Views to fhir_prd_db

This script deploys SQL views from the views/ directory to AWS Athena.
"""

import boto3
import time
import sys
from pathlib import Path

# Views to deploy in dependency order
VIEWS_TO_DEPLOY = [
    # Core reference views (no dependencies)
    'v_oid_reference.sql',
    'v_patient_demographics.sql',

    # Base clinical views
    'v_encounters.sql',
    'v_diagnoses.sql',
    'v_problem_list_diagnoses.sql',
    'v_binary_files.sql',

    # Procedure views
    'v2_procedure_specimen_link.sql',
    'v2_procedures_tumor.sql',

    # Document reference view
    'v2_document_reference_enriched.sql',

    # Imaging views
    'v2_imaging.sql',

    # Medication/chemo views
    'v_medications.sql',
    'v_chemo_medications.sql',
    'v_chemo_treatment_episodes.sql',

    # Radiation views (in dependency order)
    'v_radiation_documents.sql',
    'v_radiation_care_plan_hierarchy.sql',
    'v2_radiation_treatment_episodes.sql',
    'v2_radiation_episode_enrichment.sql',

    # Molecular/pathology
    'v_molecular_tests.sql',
    'v_pathology_diagnostics.sql',

    # Visits/appointments
    'v2_appointments.sql',
    'v_visits_unified.sql',
]

def deploy_view(client, view_file: Path, database: str = 'fhir_prd_db') -> bool:
    """Deploy a single view to Athena"""

    print(f"\n{'='*80}")
    print(f"Deploying: {view_file.name}")
    print('='*80)

    try:
        # Read SQL file
        sql = view_file.read_text()

        # Execute CREATE OR REPLACE VIEW
        response = client.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={'Database': database},
            ResultConfiguration={
                'OutputLocation': 's3://aws-athena-query-results-343218191717-us-east-1/'
            }
        )

        query_id = response['QueryExecutionId']
        print(f"Query ID: {query_id}")

        # Wait for completion
        for i in range(60):
            status = client.get_query_execution(QueryExecutionId=query_id)
            state = status['QueryExecution']['Status']['State']

            if state == 'SUCCEEDED':
                print(f"✅ SUCCESS: {view_file.name}")
                return True
            elif state in ['FAILED', 'CANCELLED']:
                reason = status['QueryExecution']['Status'].get('StateChangeReason', 'Unknown')
                print(f"❌ FAILED: {view_file.name}")
                print(f"   Reason: {reason}")
                return False

            time.sleep(2)

        print(f"⏱️  TIMEOUT: {view_file.name}")
        return False

    except Exception as e:
        print(f"❌ ERROR: {view_file.name}")
        print(f"   Exception: {e}")
        return False

def main():
    print("="*80)
    print("ATHENA VIEW DEPLOYMENT")
    print("="*80)
    print(f"Database: fhir_prd_db")
    print(f"Views to deploy: {len(VIEWS_TO_DEPLOY)}")
    print()

    # Initialize Athena client
    client = boto3.client('athena', region_name='us-east-1')

    # Views directory
    views_dir = Path(__file__).parent / 'views'

    # Deploy views
    results = {
        'success': [],
        'failed': [],
        'skipped': []
    }

    for view_filename in VIEWS_TO_DEPLOY:
        view_path = views_dir / view_filename

        if not view_path.exists():
            print(f"\n⚠️  SKIPPED: {view_filename} (file not found)")
            results['skipped'].append(view_filename)
            continue

        success = deploy_view(client, view_path)

        if success:
            results['success'].append(view_filename)
        else:
            results['failed'].append(view_filename)

    # Summary
    print("\n" + "="*80)
    print("DEPLOYMENT SUMMARY")
    print("="*80)
    print(f"✅ Successful: {len(results['success'])}")
    print(f"❌ Failed: {len(results['failed'])}")
    print(f"⚠️  Skipped: {len(results['skipped'])}")

    if results['failed']:
        print("\nFailed views:")
        for view in results['failed']:
            print(f"  - {view}")
        sys.exit(1)

    if results['skipped']:
        print("\nSkipped views:")
        for view in results['skipped']:
            print(f"  - {view}")

    print("\n✅ All views deployed successfully!")
    sys.exit(0)

if __name__ == '__main__':
    main()
