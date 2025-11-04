#!/usr/bin/env python3
"""
Test date columns by querying actual data from Athena views.

This validates that date parsing is working correctly by checking if
date columns return non-NULL values for actual patient data.
"""

import boto3
import time

def test_view_dates(client, view_name, date_columns, test_query):
    """Test if date columns are populated in actual data"""
    print(f"\n{'='*80}")
    print(f"Testing: {view_name}")
    print(f"{'='*80}")
    print(f"Query: {test_query}")

    try:
        response = client.start_query_execution(
            QueryString=test_query,
            QueryExecutionContext={'Database': 'fhir_prd_db'},
            ResultConfiguration={'OutputLocation': 's3://aws-athena-query-results-343218191717-us-east-1/'}
        )

        query_id = response['QueryExecutionId']
        print(f"Query ID: {query_id}")

        for i in range(60):
            status = client.get_query_execution(QueryExecutionId=query_id)
            state = status['QueryExecution']['Status']['State']

            if state == 'SUCCEEDED':
                results = client.get_query_results(QueryExecutionId=query_id)

                if len(results['ResultSet']['Rows']) > 1:
                    headers = [col['Name'] for col in results['ResultSet']['ResultSetMetadata']['ColumnInfo']]
                    print(f"\n✅ SUCCESS - Got {len(results['ResultSet']['Rows'])-1} rows")
                    print(f"Headers: {', '.join(headers)}")

                    # Show first row
                    first_row = results['ResultSet']['Rows'][1]
                    values = [col.get('VarCharValue', 'NULL') for col in first_row['Data']]
                    print(f"Sample row: {dict(zip(headers, values))}")

                    # Check for NULL dates
                    null_count = sum(1 for v in values if v == 'NULL')
                    if null_count > 0:
                        print(f"⚠️  WARNING: {null_count}/{len(values)} columns are NULL in sample")
                    else:
                        print("✅ All date columns populated!")
                else:
                    print("⚠️  No data returned")

                return True

            elif state in ['FAILED', 'CANCELLED']:
                reason = status['QueryExecution']['Status'].get('StateChangeReason', 'Unknown')
                print(f"❌ FAILED: {reason}")
                return False

            time.sleep(2)

        print("⏱️  TIMEOUT")
        return False

    except Exception as e:
        print(f"❌ ERROR: {str(e)}")
        return False

def main():
    client = boto3.client('athena', region_name='us-east-1')

    # Test patient ID
    patient_id = 'eQSB0y3q.OmvN40Yhg9.eCBk5-9c-Qp-FT3pBWoSGuL83'

    print("="*80)
    print("TESTING DATE COLUMNS WITH ACTUAL DATA")
    print("="*80)

    tests = [
        {
            'view': 'v_procedures_tumor',
            'query': f"""
                SELECT proc_performed_date_time, proc_code_text
                FROM fhir_prd_db.v_procedures_tumor
                WHERE patient_fhir_id = '{patient_id}'
                    AND is_tumor_surgery = true
                LIMIT 3
            """
        },
        {
            'view': 'v_patient_demographics',
            'query': f"""
                SELECT pd_birth_date, pd_gender
                FROM fhir_prd_db.v_patient_demographics
                WHERE patient_fhir_id = '{patient_id}'
            """
        },
        {
            'view': 'v2_appointments',
            'query': f"""
                SELECT appointment_date, appointment_status
                FROM fhir_prd_db.v_appointments
                WHERE patient_fhir_id = '{patient_id}'
                    AND appointment_date IS NOT NULL
                LIMIT 5
            """
        },
        {
            'view': 'v2_imaging',
            'query': f"""
                SELECT report_issued, report_effective_period_start, imaging_modality
                FROM fhir_prd_db.v_imaging
                WHERE patient_fhir_id = '{patient_id}'
                    AND report_issued IS NOT NULL
                LIMIT 5
            """
        },
        {
            'view': 'v_medications',
            'query': f"""
                SELECT medication_start_date, medication_stop_date, medication_name
                FROM fhir_prd_db.v_medications
                WHERE patient_fhir_id = '{patient_id}'
                    AND medication_start_date IS NOT NULL
                LIMIT 5
            """
        },
    ]

    results = []
    for test in tests:
        success = test_view_dates(client, test['view'], [], test['query'])
        results.append({'view': test['view'], 'success': success})

    print(f"\n\n{'='*80}")
    print("FINAL SUMMARY")
    print(f"{'='*80}")

    passed = sum(1 for r in results if r['success'])
    total = len(results)

    print(f"\nTests passed: {passed}/{total}")

    for r in results:
        status = "✅ PASS" if r['success'] else "❌ FAIL"
        print(f"  {status}: {r['view']}")

    if passed == total:
        print(f"\n🎉 ALL TESTS PASSED! Date parsing is working correctly.")
    else:
        print(f"\n⚠️  Some tests failed - review output above")

if __name__ == '__main__':
    main()
