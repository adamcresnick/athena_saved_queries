CREATE OR REPLACE VIEW fhir_prd_db.v_autologous_stem_cell_transplant AS
WITH
-- 1. Transplant status from condition table (HIGHEST YIELD: 1,981 records)
transplant_conditions AS (
    SELECT
        c.subject_reference as patient_fhir_id,
        c.id as condition_id,
        ccc.code_coding_code as icd10_code,
        ccc.code_coding_display as diagnosis,

        -- Standardized dates (append T00:00:00Z if date-only)
        CASE
            WHEN LENGTH(c.onset_date_time) = 10 THEN c.onset_date_time || 'T00:00:00Z'
            ELSE c.onset_date_time
        END as condition_onset,
        CASE
            WHEN LENGTH(c.recorded_date) = 10 THEN c.recorded_date || 'T00:00:00Z'
            ELSE c.recorded_date
        END as recorded_date,

        -- Autologous flag from ICD-10 codes
        CASE
            WHEN ccc.code_coding_code = '108631000119101' THEN true  -- History of autologous BMT
            WHEN ccc.code_coding_code = '848081' THEN true           -- History of autologous SCT
            WHEN LOWER(ccc.code_coding_display) LIKE '%autologous%' THEN true
            ELSE false
        END as confirmed_autologous,

        -- Confidence level
        CASE
            WHEN ccc.code_coding_code IN ('108631000119101', '848081') THEN 'high'
            WHEN LOWER(ccc.code_coding_display) LIKE '%autologous%' THEN 'medium'
            ELSE 'low'
        END as confidence_level,

        'condition' as data_source

    FROM fhir_prd_db.condition c
    INNER JOIN fhir_prd_db.condition_code_coding ccc
        ON c.id = ccc.condition_id
    WHERE ccc.code_coding_code IN ('Z94.84', 'Z94.81', '108631000119101', '848081', 'V42.82', 'V42.81')
       OR LOWER(ccc.code_coding_display) LIKE '%stem%cell%transplant%'
),

-- 2. Transplant procedures (19 records, 17 patients)
transplant_procedures AS (
    SELECT
        p.subject_reference as patient_fhir_id,
        p.id as procedure_id,
        p.code_text as procedure_description,
        pcc.code_coding_code as cpt_code,

        -- Standardized dates
        CASE
            WHEN LENGTH(p.performed_date_time) = 10 THEN p.performed_date_time || 'T00:00:00Z'
            ELSE p.performed_date_time
        END as procedure_date,
        CASE
            WHEN LENGTH(p.performed_period_start) = 10 THEN p.performed_period_start || 'T00:00:00Z'
            ELSE p.performed_period_start
        END as performed_period_start,

        -- Autologous flag from CPT codes or text
        CASE
            WHEN LOWER(p.code_text) LIKE '%autologous%' THEN true
            WHEN pcc.code_coding_code = '38241' THEN true  -- Autologous CPT
            ELSE false
        END as confirmed_autologous,

        -- Confidence level
        CASE
            WHEN pcc.code_coding_code = '38241' THEN 'high'
            WHEN LOWER(p.code_text) LIKE '%autologous%' THEN 'medium'
            ELSE 'low'
        END as confidence_level,

        'procedure' as data_source

    FROM fhir_prd_db.procedure p
    LEFT JOIN fhir_prd_db.procedure_code_coding pcc
        ON p.id = pcc.procedure_id
    WHERE (
        LOWER(p.code_text) LIKE '%stem%cell%'
        OR LOWER(p.code_text) LIKE '%autologous%'
        OR LOWER(p.code_text) LIKE '%bone%marrow%transplant%'
        OR pcc.code_coding_code IN ('38241', '38240')
    )
    AND p.status = 'completed'
),

-- 3. Transplant date from observations (359 exact dates)
transplant_dates_obs AS (
    SELECT
        o.subject_reference as patient_fhir_id,

        -- Standardized dates
        CASE
            WHEN LENGTH(o.effective_date_time) = 10 THEN o.effective_date_time || 'T00:00:00Z'
            ELSE o.effective_date_time
        END as transplant_date,
        o.value_string as transplant_date_value,

        'observation' as data_source

    FROM fhir_prd_db.observation o
    WHERE LOWER(o.code_text) LIKE '%hematopoietic%stem%cell%transplant%transplant%date%'
       OR LOWER(o.code_text) LIKE '%stem%cell%transplant%date%'
),

-- 4. CD34+ counts (validates stem cell collection/engraftment)
cd34_counts AS (
    SELECT
        o.subject_reference as patient_fhir_id,

        -- Standardized dates
        CASE
            WHEN LENGTH(o.effective_date_time) = 10 THEN o.effective_date_time || 'T00:00:00Z'
            ELSE o.effective_date_time
        END as collection_date,
        o.value_quantity_value as cd34_count,
        o.value_quantity_unit as unit,

        'cd34_count' as data_source

    FROM fhir_prd_db.observation o
    WHERE LOWER(o.code_text) LIKE '%cd34%'
)

-- MAIN SELECT: Combine all data sources
SELECT
    COALESCE(tc.patient_fhir_id, tp.patient_fhir_id, tdo.patient_fhir_id, cd34.patient_fhir_id) as patient_fhir_id,

    -- Condition data (transplant status)
    tc.condition_id as cond_id,
    tc.icd10_code as cond_icd10_code,
    tc.diagnosis as cond_transplant_status,
    TRY(CAST(tc.condition_onset AS TIMESTAMP(3))) as cond_onset_datetime,
    TRY(CAST(tc.recorded_date AS TIMESTAMP(3))) as cond_recorded_datetime,
    tc.confirmed_autologous as cond_autologous_flag,
    tc.confidence_level as cond_confidence,

    -- Procedure data
    tp.procedure_id as proc_id,
    tp.procedure_description as proc_description,
    tp.cpt_code as proc_cpt_code,
    TRY(CAST(tp.procedure_date AS TIMESTAMP(3))) as proc_performed_datetime,
    TRY(CAST(tp.performed_period_start AS TIMESTAMP(3))) as proc_period_start,
    tp.confirmed_autologous as proc_autologous_flag,
    tp.confidence_level as proc_confidence,

    -- Transplant date from observation
    TRY(CAST(tdo.transplant_date AS TIMESTAMP(3))) as obs_transplant_datetime,
    tdo.transplant_date_value as obs_transplant_value,

    -- CD34 count data (validates stem cell collection)
    TRY(CAST(cd34.collection_date AS TIMESTAMP(3))) as cd34_collection_datetime,
    cd34.cd34_count as cd34_count_value,
    cd34.unit as cd34_unit,

    -- Best available transplant date
    TRY(CAST(COALESCE(tp.procedure_date, tdo.transplant_date, tc.condition_onset) AS TIMESTAMP(3))) as transplant_datetime,

    -- Confirmed autologous flag (HIGH confidence)
    CASE
        WHEN tc.confirmed_autologous = true OR tp.confirmed_autologous = true THEN true
        ELSE false
    END as confirmed_autologous,

    -- Overall confidence level
    CASE
        WHEN tc.confidence_level = 'high' OR tp.confidence_level = 'high' THEN 'high'
        WHEN tc.confidence_level = 'medium' OR tp.confidence_level = 'medium' THEN 'medium'
        ELSE 'low'
    END as overall_confidence,

    -- Data sources present (for validation)
    CASE WHEN tc.patient_fhir_id IS NOT NULL THEN true ELSE false END as has_condition_data,
    CASE WHEN tp.patient_fhir_id IS NOT NULL THEN true ELSE false END as has_procedure_data,
    CASE WHEN tdo.patient_fhir_id IS NOT NULL THEN true ELSE false END as has_transplant_date_obs,
    CASE WHEN cd34.patient_fhir_id IS NOT NULL THEN true ELSE false END as has_cd34_data,

    -- Data quality score (0-4 based on sources present)
    (CASE WHEN tc.patient_fhir_id IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN tp.patient_fhir_id IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN tdo.patient_fhir_id IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN cd34.patient_fhir_id IS NOT NULL THEN 1 ELSE 0 END) as data_completeness_score

FROM transplant_conditions tc
FULL OUTER JOIN transplant_procedures tp
    ON tc.patient_fhir_id = tp.patient_fhir_id
FULL OUTER JOIN transplant_dates_obs tdo
    ON COALESCE(tc.patient_fhir_id, tp.patient_fhir_id) = tdo.patient_fhir_id
FULL OUTER JOIN cd34_counts cd34
    ON COALESCE(tc.patient_fhir_id, tp.patient_fhir_id, tdo.patient_fhir_id) = cd34.patient_fhir_id

WHERE COALESCE(tc.patient_fhir_id, tp.patient_fhir_id, tdo.patient_fhir_id, cd34.patient_fhir_id) IS NOT NULL

ORDER BY patient_fhir_id, transplant_datetime;