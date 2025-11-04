CREATE OR REPLACE VIEW fhir_prd_db.v_autologous_stem_cell_collection AS

WITH collection_procedures AS (
    -- Identify autologous stem cell collection procedures
    SELECT DISTINCT
        p.subject_reference as patient_fhir_id,
        p.id as procedure_fhir_id,
        p.code_text as procedure_description,

        -- Standardize collection date
        TRY(CAST(CASE
            WHEN LENGTH(p.performed_date_time) = 10
            THEN p.performed_date_time || 'T00:00:00Z'
            ELSE p.performed_date_time
        END AS TIMESTAMP(3))) as collection_datetime,

        -- Extract method from coding
        COALESCE(pc.code_coding_display, p.code_text) as collection_method,
        pc.code_coding_code as collection_cpt_code,

        p.status as procedure_status,
        p.outcome_text as procedure_outcome

    FROM fhir_prd_db.procedure p
    LEFT JOIN fhir_prd_db.procedure_code_coding pc
        ON pc.procedure_id = p.id
        AND pc.code_coding_system LIKE '%cpt%'

    WHERE (
        -- CPT code for apheresis/stem cell collection
        pc.code_coding_code IN ('38231', '38232', '38241')

        -- Text matching for collection procedures
        OR LOWER(p.code_text) LIKE '%apheresis%'
        OR LOWER(p.code_text) LIKE '%stem cell%collection%'
        OR LOWER(p.code_text) LIKE '%stem cell%harvest%'
        OR LOWER(p.code_text) LIKE '%peripheral blood%progenitor%'
        OR LOWER(p.code_text) LIKE '%pbsc%collection%'
        OR LOWER(p.code_text) LIKE '%marrow%harvest%'
    )
    AND p.status IN ('completed', 'in-progress', 'preparation')
),

cd34_counts AS (
    -- Extract CD34+ cell counts from observations
    SELECT DISTINCT
        o.subject_reference as patient_fhir_id,
        o.id as observation_fhir_id,

        -- Standardize measurement date
        TRY(CAST(CASE
            WHEN LENGTH(o.effective_date_time) = 10
            THEN o.effective_date_time || 'T00:00:00Z'
            ELSE o.effective_date_time
        END AS TIMESTAMP(3))) as measurement_datetime,

        o.code_text as measurement_type,
        o.value_quantity_value as cd34_count,
        o.value_quantity_unit as cd34_unit,

        -- Categorize CD34 source
        CASE
            WHEN LOWER(o.code_text) LIKE '%apheresis%' THEN 'apheresis_product'
            WHEN LOWER(o.code_text) LIKE '%marrow%' THEN 'marrow_product'
            WHEN LOWER(o.code_text) LIKE '%peripheral%blood%' THEN 'peripheral_blood'
            WHEN LOWER(o.code_text) LIKE '%pbsc%' THEN 'peripheral_blood'
            ELSE 'unspecified'
        END as cd34_source,

        -- Calculate adequacy (≥5×10⁶/kg is adequate)
        CASE
            WHEN o.value_quantity_value IS NOT NULL THEN
                CASE
                    WHEN LOWER(o.value_quantity_unit) LIKE '%10%6%kg%'
                         OR LOWER(o.value_quantity_unit) LIKE '%million%kg%' THEN
                        CASE
                            WHEN CAST(o.value_quantity_value AS DOUBLE) >= 5.0 THEN 'adequate'
                            WHEN CAST(o.value_quantity_value AS DOUBLE) >= 2.0 THEN 'minimal'
                            ELSE 'inadequate'
                        END
                    ELSE 'unit_unclear'
                END
            ELSE NULL
        END as cd34_adequacy

    FROM fhir_prd_db.observation o

    WHERE LOWER(o.code_text) LIKE '%cd34%'
      AND (
        LOWER(o.code_text) LIKE '%apheresis%'
        OR LOWER(o.code_text) LIKE '%marrow%'
        OR LOWER(o.code_text) LIKE '%collection%'
        OR LOWER(o.code_text) LIKE '%harvest%'
        OR LOWER(o.code_text) LIKE '%stem%cell%'
        OR LOWER(o.code_text) LIKE '%pbsc%'
        OR LOWER(o.code_text) LIKE '%progenitor%'
      )
      AND o.value_quantity_value IS NOT NULL
),

mobilization_timing_bounds AS (
    -- Aggregate timing bounds for mobilization medications
    SELECT
        medication_request_id,
        MIN(dosage_instruction_timing_repeat_bounds_period_start) as earliest_bounds_start,
        MAX(dosage_instruction_timing_repeat_bounds_period_end) as latest_bounds_end
    FROM fhir_prd_db.medication_request_dosage_instruction
    WHERE dosage_instruction_timing_repeat_bounds_period_start IS NOT NULL
       OR dosage_instruction_timing_repeat_bounds_period_end IS NOT NULL
    GROUP BY medication_request_id
),

mobilization_agents AS (
    -- Identify mobilization medications (G-CSF, Plerixafor)
    SELECT DISTINCT
        mr.subject_reference as patient_fhir_id,
        mr.id as medication_request_fhir_id,

        -- Standardize start date
        TRY(CAST(CASE
            WHEN mtb.earliest_bounds_start IS NOT NULL THEN
                CASE
                    WHEN LENGTH(mtb.earliest_bounds_start) = 10
                    THEN mtb.earliest_bounds_start || 'T00:00:00Z'
                    ELSE mtb.earliest_bounds_start
                END
            WHEN LENGTH(mr.authored_on) = 10
                THEN mr.authored_on || 'T00:00:00Z'
            ELSE mr.authored_on
        END AS TIMESTAMP(3))) as mobilization_start_datetime,

        -- Standardize end date
        TRY(CAST(CASE
            WHEN mtb.latest_bounds_end IS NOT NULL THEN
                CASE
                    WHEN LENGTH(mtb.latest_bounds_end) = 10
                    THEN mtb.latest_bounds_end || 'T00:00:00Z'
                    ELSE mtb.latest_bounds_end
                END
            WHEN mr.dispense_request_validity_period_end IS NOT NULL THEN
                CASE
                    WHEN LENGTH(mr.dispense_request_validity_period_end) = 10
                    THEN mr.dispense_request_validity_period_end || 'T00:00:00Z'
                    ELSE mr.dispense_request_validity_period_end
                END
            ELSE NULL
        END AS TIMESTAMP(3))) as mobilization_stop_datetime,

        COALESCE(m.code_text, mr.medication_reference_display) as medication_name,
        mcc.code_coding_code as rxnorm_code,
        mcc.code_coding_display as rxnorm_display,

        -- Categorize mobilization agent
        CASE
            WHEN mcc.code_coding_code IN ('105585', '139825') THEN 'filgrastim'
            WHEN mcc.code_coding_code = '358810' THEN 'pegfilgrastim'
            WHEN mcc.code_coding_code = '847232' THEN 'plerixafor'
            WHEN LOWER(m.code_text) LIKE '%filgrastim%' THEN 'filgrastim'
            WHEN LOWER(m.code_text) LIKE '%neupogen%' THEN 'filgrastim'
            WHEN LOWER(m.code_text) LIKE '%neulasta%' THEN 'pegfilgrastim'
            WHEN LOWER(m.code_text) LIKE '%plerixafor%' THEN 'plerixafor'
            WHEN LOWER(m.code_text) LIKE '%mozobil%' THEN 'plerixafor'
            ELSE 'other_mobilization'
        END as mobilization_agent_type,

        mr.status as medication_status

    FROM fhir_prd_db.medication_request mr
    LEFT JOIN mobilization_timing_bounds mtb ON mr.id = mtb.medication_request_id
    LEFT JOIN fhir_prd_db.medication m
        ON m.id = mr.medication_reference_reference
    LEFT JOIN fhir_prd_db.medication_code_coding mcc
        ON mcc.medication_id = m.id
        AND mcc.code_coding_system = 'http://www.nlm.nih.gov/research/umls/rxnorm'

    WHERE (
        -- RxNorm codes for mobilization agents (ingredients + major formulations)
        mcc.code_coding_code IN (
            -- Filgrastim (G-CSF)
            '68442',    -- Filgrastim (ingredient)
            '1649944',  -- filgrastim 0.3 MG/ML 1 ML injection (high volume formulation)
            '1649963',  -- filgrastim 0.3 MG/ML 1.6 ML injection (high volume formulation)

            -- Pegfilgrastim (long-acting G-CSF)
            '338036',   -- Pegfilgrastim (ingredient)
            '727539',   -- pegfilgrastim 10 MG/ML 0.6 ML prefilled syringe (highest volume formulation)

            -- Plerixafor (CXCR4 antagonist)
            '733003',   -- Plerixafor (ingredient)
            '828700'    -- plerixafor 20 MG/ML 1.2 ML injection (standard formulation)
        )

        -- Text matching for G-CSF and mobilization agents (brands + generics)
        OR LOWER(m.code_text) LIKE '%filgrastim%'
        OR LOWER(m.code_text) LIKE '%pegfilgrastim%'
        OR LOWER(m.code_text) LIKE '%plerixafor%'
        OR LOWER(m.code_text) LIKE '%g-csf%'

        -- Filgrastim brands
        OR LOWER(m.code_text) LIKE '%neupogen%'
        OR LOWER(m.code_text) LIKE '%granix%'
        OR LOWER(m.code_text) LIKE '%zarxio%'  -- biosimilar
        OR LOWER(m.code_text) LIKE '%nivestim%'

        -- Pegfilgrastim brands
        OR LOWER(m.code_text) LIKE '%neulasta%'
        OR LOWER(m.code_text) LIKE '%fulphila%'
        OR LOWER(m.code_text) LIKE '%udenyca%'
        OR LOWER(m.code_text) LIKE '%ziextenzo%'
        OR LOWER(m.code_text) LIKE '%nyvepria%'
        OR LOWER(m.code_text) LIKE '%stimufend%'

        -- Plerixafor brands
        OR LOWER(m.code_text) LIKE '%mozobil%'

        -- Also check medication_reference_display
        OR LOWER(mr.medication_reference_display) LIKE '%filgrastim%'
        OR LOWER(mr.medication_reference_display) LIKE '%pegfilgrastim%'
        OR LOWER(mr.medication_reference_display) LIKE '%plerixafor%'
        OR LOWER(mr.medication_reference_display) LIKE '%neupogen%'
        OR LOWER(mr.medication_reference_display) LIKE '%neulasta%'
        OR LOWER(mr.medication_reference_display) LIKE '%zarxio%'
        OR LOWER(mr.medication_reference_display) LIKE '%fulphila%'
        OR LOWER(mr.medication_reference_display) LIKE '%ziextenzo%'
        OR LOWER(mr.medication_reference_display) LIKE '%mozobil%'
        OR LOWER(mr.medication_reference_display) LIKE '%g-csf%'
    )
    AND mr.status IN ('active', 'completed', 'stopped')
),

product_quality AS (
    -- Extract product quality and viability metrics
    SELECT DISTINCT
        o.subject_reference as patient_fhir_id,
        o.id as observation_fhir_id,

        TRY(CAST(CASE
            WHEN LENGTH(o.effective_date_time) = 10
            THEN o.effective_date_time || 'T00:00:00Z'
            ELSE o.effective_date_time
        END AS TIMESTAMP(3))) as measurement_datetime,

        o.code_text as quality_metric,
        o.value_quantity_value as metric_value,
        o.value_quantity_unit as metric_unit,
        o.value_string as metric_value_text,

        -- Categorize quality metrics
        CASE
            WHEN LOWER(o.code_text) LIKE '%viability%' THEN 'viability'
            WHEN LOWER(o.code_text) LIKE '%volume%' THEN 'volume'
            WHEN LOWER(o.code_text) LIKE '%tnc%' OR LOWER(o.code_text) LIKE '%total%nucleated%' THEN 'total_nucleated_cells'
            WHEN LOWER(o.code_text) LIKE '%sterility%' THEN 'sterility'
            WHEN LOWER(o.code_text) LIKE '%contamination%' THEN 'contamination'
            ELSE 'other_quality'
        END as quality_metric_type

    FROM fhir_prd_db.observation o

    WHERE (
        (LOWER(o.code_text) LIKE '%stem%cell%' OR LOWER(o.code_text) LIKE '%apheresis%')
        AND (
            LOWER(o.code_text) LIKE '%viability%'
            OR LOWER(o.code_text) LIKE '%volume%'
            OR LOWER(o.code_text) LIKE '%tnc%'
            OR LOWER(o.code_text) LIKE '%total%nucleated%'
            OR LOWER(o.code_text) LIKE '%sterility%'
            OR LOWER(o.code_text) LIKE '%contamination%'
            OR LOWER(o.code_text) LIKE '%quality%'
        )
    )
)

-- Main query: Combine all collection data sources
SELECT DISTINCT
    -- Patient identifier
    COALESCE(
        cp.patient_fhir_id,
        cd34.patient_fhir_id,
        ma.patient_fhir_id,
        pq.patient_fhir_id
    ) as patient_fhir_id,

    -- Collection procedure details
    cp.procedure_fhir_id as collection_procedure_fhir_id,
    cp.collection_datetime,
    cp.collection_method,
    cp.collection_cpt_code,
    cp.procedure_status as collection_status,
    cp.procedure_outcome as collection_outcome,

    -- CD34+ cell count metrics
    cd34.observation_fhir_id as cd34_observation_fhir_id,
    TRY(CAST(cd34.measurement_datetime AS TIMESTAMP(3))) as cd34_measurement_datetime,
    cd34.cd34_count,
    cd34.cd34_unit,
    cd34.cd34_source,
    cd34.cd34_adequacy,

    -- Mobilization agent details
    ma.medication_request_fhir_id as mobilization_medication_fhir_id,
    ma.medication_name as mobilization_agent_name,
    ma.rxnorm_code as mobilization_rxnorm_code,
    ma.mobilization_agent_type,
    ma.mobilization_start_datetime,
    ma.mobilization_stop_datetime,
    ma.medication_status as mobilization_status,

    -- Calculate days from mobilization start to collection
    CASE
        WHEN ma.mobilization_start_datetime IS NOT NULL
             AND cp.collection_datetime IS NOT NULL THEN
            DATE_DIFF('day',
                DATE(CAST(ma.mobilization_start_datetime AS TIMESTAMP)),
                DATE(CAST(cp.collection_datetime AS TIMESTAMP))
            )
        ELSE NULL
    END as days_from_mobilization_to_collection,

    -- Product quality metrics
    pq.observation_fhir_id as quality_observation_fhir_id,
    pq.quality_metric,
    pq.quality_metric_type,
    pq.metric_value,
    pq.metric_unit,
    pq.metric_value_text,
    TRY(CAST(pq.measurement_datetime AS TIMESTAMP(3))) as quality_measurement_datetime,

    -- Data completeness indicator
    CASE
        WHEN cp.procedure_fhir_id IS NOT NULL
             AND cd34.observation_fhir_id IS NOT NULL
             AND ma.medication_request_fhir_id IS NOT NULL THEN 'complete'
        WHEN cp.procedure_fhir_id IS NOT NULL
             AND cd34.observation_fhir_id IS NOT NULL THEN 'missing_mobilization'
        WHEN cp.procedure_fhir_id IS NOT NULL
             AND ma.medication_request_fhir_id IS NOT NULL THEN 'missing_cd34'
        WHEN cp.procedure_fhir_id IS NOT NULL THEN 'procedure_only'
        ELSE 'incomplete'
    END as data_completeness

FROM collection_procedures cp
FULL OUTER JOIN cd34_counts cd34
    ON cp.patient_fhir_id = cd34.patient_fhir_id
    AND ABS(DATE_DIFF('day',
        DATE(CAST(cp.collection_datetime AS TIMESTAMP)),
        DATE(CAST(cd34.measurement_datetime AS TIMESTAMP))
    )) <= 7  -- CD34 measured within 7 days of collection
FULL OUTER JOIN mobilization_agents ma
    ON COALESCE(cp.patient_fhir_id, cd34.patient_fhir_id) = ma.patient_fhir_id
    AND ma.mobilization_start_datetime <= COALESCE(cp.collection_datetime, cd34.measurement_datetime)
    AND DATE_DIFF('day',
        DATE(CAST(ma.mobilization_start_datetime AS TIMESTAMP)),
        DATE(CAST(COALESCE(cp.collection_datetime, cd34.measurement_datetime) AS TIMESTAMP))
    ) <= 21  -- Mobilization within 21 days before collection
LEFT JOIN product_quality pq
    ON COALESCE(cp.patient_fhir_id, cd34.patient_fhir_id, ma.patient_fhir_id) = pq.patient_fhir_id
    AND ABS(DATE_DIFF('day',
        DATE(CAST(COALESCE(cp.collection_datetime, cd34.measurement_datetime) AS TIMESTAMP)),
        DATE(CAST(pq.measurement_datetime AS TIMESTAMP))
    )) <= 7  -- Quality metrics within 7 days of collection

ORDER BY
    patient_fhir_id,
    collection_datetime,
    mobilization_start_datetime,
    cd34_measurement_datetime;