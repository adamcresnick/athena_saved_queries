CREATE OR REPLACE VIEW fhir_prd_db.v_hydrocephalus_procedures AS
WITH
-- Primary shunt procedures (1,196 procedures)
shunt_procedures AS (
    SELECT
        p.subject_reference as patient_fhir_id,
        p.id as procedure_id,
        p.code_text,
        p.status as proc_status,

        -- ALL DATE FIELDS (standardized)
        TRY(CAST(CASE
            WHEN LENGTH(p.performed_date_time) = 10 THEN p.performed_date_time || 'T00:00:00Z'
            ELSE p.performed_date_time
        END AS TIMESTAMP(3))) as proc_performed_datetime,
        TRY(CAST(CASE
            WHEN LENGTH(p.performed_period_start) = 10 THEN p.performed_period_start || 'T00:00:00Z'
            ELSE p.performed_period_start
        END AS TIMESTAMP(3))) as proc_period_start,
        TRY(CAST(CASE
            WHEN LENGTH(p.performed_period_end) = 10 THEN p.performed_period_end || 'T00:00:00Z'
            ELSE p.performed_period_end
        END AS TIMESTAMP(3))) as proc_period_end,

        p.category_text as proc_category_text,
        p.outcome_text as proc_outcome_text,
        p.location_display as proc_location,
        p.encounter_reference as proc_encounter_ref,

        -- Shunt type classification
        CASE
            WHEN LOWER(p.code_text) LIKE '%ventriculoperitoneal%'
                 OR LOWER(p.code_text) LIKE '%vp%shunt%'
                 OR LOWER(p.code_text) LIKE '%v-p%shunt%'
                 OR LOWER(p.code_text) LIKE '%vps%' THEN 'VPS'
            WHEN LOWER(p.code_text) LIKE '%endoscopic%third%ventriculostomy%'
                 OR LOWER(p.code_text) LIKE '%etv%' THEN 'ETV'
            WHEN LOWER(p.code_text) LIKE '%external%ventricular%drain%'
                 OR LOWER(p.code_text) LIKE '%evd%'
                 OR LOWER(p.code_text) LIKE '%temporary%' THEN 'EVD'
            WHEN LOWER(p.code_text) LIKE '%ventriculoatrial%'
                 OR LOWER(p.code_text) LIKE '%va%shunt%' THEN 'VA Shunt'
            WHEN LOWER(p.code_text) LIKE '%ventriculopleural%' THEN 'Ventriculopleural'
            ELSE 'Other'
        END as shunt_type,

        -- Procedure category
        CASE
            WHEN LOWER(p.code_text) LIKE '%placement%'
                 OR LOWER(p.code_text) LIKE '%insertion%'
                 OR LOWER(p.code_text) LIKE '%creation%' THEN 'Placement'
            WHEN LOWER(p.code_text) LIKE '%revision%'
                 OR LOWER(p.code_text) LIKE '%replacement%' THEN 'Revision'
            WHEN LOWER(p.code_text) LIKE '%removal%'
                 OR LOWER(p.code_text) LIKE '%explant%' THEN 'Removal'
            WHEN LOWER(p.code_text) LIKE '%reprogram%' THEN 'Reprogramming'
            WHEN LOWER(p.code_text) LIKE '%evd%'
                 OR LOWER(p.code_text) LIKE '%temporary%' THEN 'Temporary EVD'
            WHEN LOWER(p.code_text) LIKE '%etv%'
                 OR LOWER(p.code_text) LIKE '%ventriculostomy%' THEN 'ETV'
            ELSE 'Other'
        END as procedure_category

    FROM fhir_prd_db.procedure p
    WHERE p.subject_reference IS NOT NULL
      AND (
          LOWER(p.code_text) LIKE '%shunt%'
          OR LOWER(p.code_text) LIKE '%ventriculostomy%'
          OR LOWER(p.code_text) LIKE '%ventricular%drain%'
          OR LOWER(p.code_text) LIKE '%csf%diversion%'
      )
),

-- Procedure reason codes 
procedure_reasons AS (
    SELECT
        prc.procedure_id,
        LISTAGG(prc.reason_code_text, ' | ') WITHIN GROUP (ORDER BY prc.reason_code_text) as reasons_text,
        LISTAGG(DISTINCT prc.reason_code_coding, ' | ') WITHIN GROUP (ORDER BY prc.reason_code_coding) as reason_codes,

        -- Confirmed hydrocephalus indication
        MAX(CASE
            WHEN LOWER(prc.reason_code_text) LIKE '%hydroceph%' THEN true
            WHEN LOWER(prc.reason_code_text) LIKE '%increased%intracranial%pressure%' THEN true
            WHEN LOWER(prc.reason_code_text) LIKE '%ventriculomegaly%' THEN true
            WHEN prc.reason_code_coding LIKE '%G91%' THEN true
            WHEN prc.reason_code_coding LIKE '%Q03%' THEN true
            ELSE false
        END) as confirmed_hydrocephalus

    FROM fhir_prd_db.procedure_reason_code prc
    WHERE prc.procedure_id IN (SELECT procedure_id FROM shunt_procedures)
    GROUP BY prc.procedure_id
),

-- Procedure body sites 
procedure_body_sites AS (
    SELECT
        pbs.procedure_id,
        LISTAGG(pbs.body_site_text, ' | ') WITHIN GROUP (ORDER BY pbs.body_site_text) as body_sites_text,
        LISTAGG(DISTINCT pbs.body_site_coding, ' | ') WITHIN GROUP (ORDER BY pbs.body_site_coding) as body_site_codes,

        -- Anatomical location flags
        MAX(CASE WHEN LOWER(pbs.body_site_text) LIKE '%lateral%ventricle%' THEN true ELSE false END) as lateral_ventricle,
        MAX(CASE WHEN LOWER(pbs.body_site_text) LIKE '%third%ventricle%' THEN true ELSE false END) as third_ventricle,
        MAX(CASE WHEN LOWER(pbs.body_site_text) LIKE '%fourth%ventricle%' THEN true ELSE false END) as fourth_ventricle,
        MAX(CASE WHEN LOWER(pbs.body_site_text) LIKE '%periton%' THEN true ELSE false END) as peritoneum,
        MAX(CASE WHEN LOWER(pbs.body_site_text) LIKE '%atri%' THEN true ELSE false END) as atrium

    FROM fhir_prd_db.procedure_body_site pbs
    WHERE pbs.procedure_id IN (SELECT procedure_id FROM shunt_procedures)
    GROUP BY pbs.procedure_id
),

-- Procedure performers (surgeon documentation)
procedure_performers AS (
    SELECT
        pp.procedure_id,
        LISTAGG(pp.performer_actor_display, ' | ') WITHIN GROUP (ORDER BY pp.performer_actor_display) as performers,
        LISTAGG(DISTINCT pp.performer_function_text, ' | ') WITHIN GROUP (ORDER BY pp.performer_function_text) as performer_roles,

        -- Surgeon flag
        MAX(CASE
            WHEN LOWER(pp.performer_function_text) LIKE '%surg%' THEN true
            WHEN LOWER(pp.performer_actor_display) LIKE '%surg%' THEN true
            ELSE false
        END) as has_surgeon

    FROM fhir_prd_db.procedure_performer pp
    WHERE pp.procedure_id IN (SELECT procedure_id FROM shunt_procedures)
    GROUP BY pp.procedure_id
),

-- Procedure notes (programmable valve detection)
procedure_notes AS (
    SELECT
        pn.procedure_id,
        LISTAGG(pn.note_text, ' | ') WITHIN GROUP (ORDER BY pn.note_time) as notes_text,

        -- Programmable valve mentions
        MAX(CASE
            WHEN LOWER(pn.note_text) LIKE '%programmable%' THEN true
            WHEN LOWER(pn.note_text) LIKE '%strata%' THEN true
            WHEN LOWER(pn.note_text) LIKE '%hakim%' THEN true
            WHEN LOWER(pn.note_text) LIKE '%polaris%' THEN true
            WHEN LOWER(pn.note_text) LIKE '%progav%' THEN true
            WHEN LOWER(pn.note_text) LIKE '%codman%certas%' THEN true
            ELSE false
        END) as mentions_programmable

    FROM fhir_prd_db.procedure_note pn
    WHERE pn.procedure_id IN (SELECT procedure_id FROM shunt_procedures)
    GROUP BY pn.procedure_id
),

-- Shunt devices from procedure_focal_device (programmable valve detection)
shunt_devices AS (
    SELECT
        p.subject_reference as patient_fhir_id,
        pfd.procedure_id,
        pfd.focal_device_manipulated_display as device_name,
        pfd.focal_device_action_text as device_action,

        -- Programmable valve detection
        CASE
            WHEN LOWER(pfd.focal_device_manipulated_display) LIKE '%programmable%' THEN true
            WHEN LOWER(pfd.focal_device_manipulated_display) LIKE '%strata%' THEN true
            WHEN LOWER(pfd.focal_device_manipulated_display) LIKE '%hakim%' THEN true
            WHEN LOWER(pfd.focal_device_manipulated_display) LIKE '%polaris%' THEN true
            WHEN LOWER(pfd.focal_device_manipulated_display) LIKE '%progav%' THEN true
            WHEN LOWER(pfd.focal_device_manipulated_display) LIKE '%medtronic%' THEN true
            WHEN LOWER(pfd.focal_device_manipulated_display) LIKE '%codman%' THEN true
            WHEN LOWER(pfd.focal_device_manipulated_display) LIKE '%sophysa%' THEN true
            WHEN LOWER(pfd.focal_device_manipulated_display) LIKE '%aesculap%' THEN true
            ELSE false
        END as is_programmable

    FROM fhir_prd_db.procedure_focal_device pfd
    INNER JOIN fhir_prd_db.procedure p ON pfd.procedure_id = p.id
    WHERE pfd.procedure_id IN (SELECT procedure_id FROM shunt_procedures)
      AND (
          LOWER(pfd.focal_device_manipulated_display) LIKE '%shunt%'
          OR LOWER(pfd.focal_device_manipulated_display) LIKE '%ventriculo%'
          OR LOWER(pfd.focal_device_manipulated_display) LIKE '%valve%'
      )
),

-- Aggregate devices per patient
patient_devices AS (
    SELECT
        patient_fhir_id,
        COUNT(*) as total_devices,
        MAX(is_programmable) as has_programmable,
        LISTAGG(DISTINCT device_name, ' | ') WITHIN GROUP (ORDER BY device_name) as device_names,
        LISTAGG(DISTINCT device_action, ' | ') WITHIN GROUP (ORDER BY device_action) as device_actions
    FROM shunt_devices
    GROUP BY patient_fhir_id
),

-- Encounter linkage (hospitalization context)
procedure_encounters AS (
    SELECT
        p.id as procedure_id,
        e.id as encounter_id,
        e.class_code as encounter_class,
        e.service_type_text as encounter_type,

        -- Standardized dates
        CASE
            WHEN LENGTH(e.period_start) = 10 THEN e.period_start || 'T00:00:00Z'
            ELSE e.period_start
        END as encounter_start,
        CASE
            WHEN LENGTH(e.period_end) = 10 THEN e.period_end || 'T00:00:00Z'
            ELSE e.period_end
        END as encounter_end,

        -- Hospitalization flags
        CASE WHEN e.class_code = 'IMP' THEN true ELSE false END as was_inpatient,
        CASE WHEN e.class_code = 'EMER' THEN true ELSE false END as was_emergency

    FROM fhir_prd_db.procedure p
    LEFT JOIN fhir_prd_db.encounter e
        ON p.encounter_reference = CONCAT('Encounter/', e.id)
        OR (
            p.subject_reference = e.subject_reference
            AND p.performed_period_start >= e.period_start
            AND p.performed_period_start <= e.period_end
        )
    WHERE p.id IN (SELECT procedure_id FROM shunt_procedures)
),

-- Medications for hydrocephalus
patient_medications AS (
    SELECT
        mr.subject_reference as patient_fhir_id,
        COUNT(DISTINCT mr.id) as total_medications,

        -- Non-surgical management flags
        MAX(CASE WHEN LOWER(mr.medication_codeable_concept_text) LIKE '%acetazol%' THEN true ELSE false END) as has_acetazolamide,
        MAX(CASE
            WHEN LOWER(mr.medication_codeable_concept_text) LIKE '%dexameth%'
                 OR LOWER(mr.medication_codeable_concept_text) LIKE '%prednis%'
                 OR LOWER(mr.medication_codeable_concept_text) LIKE '%methylpred%' THEN true
            ELSE false
        END) as has_steroids,
        MAX(CASE
            WHEN LOWER(mr.medication_codeable_concept_text) LIKE '%furosemide%'
                 OR LOWER(mr.medication_codeable_concept_text) LIKE '%mannitol%' THEN true
            ELSE false
        END) as has_diuretic,

        LISTAGG(DISTINCT mr.medication_codeable_concept_text, ' | ') WITHIN GROUP (ORDER BY mr.medication_codeable_concept_text) as medication_names

    FROM fhir_prd_db.medication_request mr
    INNER JOIN fhir_prd_db.medication_request_reason_code mrc
        ON mr.id = mrc.medication_request_id
    WHERE (
        LOWER(mrc.reason_code_text) LIKE '%hydroceph%'
        OR LOWER(mrc.reason_code_text) LIKE '%intracranial%pressure%'
        OR LOWER(mrc.reason_code_text) LIKE '%ventriculomegaly%'
    )
    GROUP BY mr.subject_reference
)

-- Main SELECT: Combine all procedure data
SELECT
    -- Patient identifier
    sp.patient_fhir_id,

    -- Procedure fields (proc_ prefix)
    sp.procedure_id as proc_id,
    sp.code_text as proc_code_text,
    sp.proc_status,
    sp.proc_performed_datetime,
    sp.proc_period_start,
    sp.proc_period_end,
    sp.proc_category_text,
    sp.proc_outcome_text,
    sp.proc_location,
    sp.proc_encounter_ref,

    -- Shunt classification
    sp.shunt_type as proc_shunt_type,
    sp.procedure_category as proc_category,

    -- Procedure reason codes (prc_ prefix)
    pr.reasons_text as prc_reasons,
    pr.reason_codes as prc_codes,
    pr.confirmed_hydrocephalus as prc_confirmed_hydro,

    -- Body sites (pbs_ prefix)
    pbs.body_sites_text as pbs_sites,
    pbs.body_site_codes as pbs_codes,
    pbs.lateral_ventricle as pbs_lateral_ventricle,
    pbs.third_ventricle as pbs_third_ventricle,
    pbs.fourth_ventricle as pbs_fourth_ventricle,
    pbs.peritoneum as pbs_peritoneum,
    pbs.atrium as pbs_atrium,

    -- Performers (pp_ prefix)
    perf.performers as pp_performers,
    perf.performer_roles as pp_roles,
    perf.has_surgeon as pp_has_surgeon,

    -- Procedure notes (pn_ prefix)
    pn.notes_text as pn_notes,
    pn.mentions_programmable as pn_mentions_programmable,

    -- Device information (dev_ prefix)
    pd.total_devices as dev_total,
    pd.has_programmable as dev_has_programmable,
    pd.device_names as dev_names,
    pd.device_actions as dev_actions,

    -- Encounter linkage (enc_ prefix)
    pe.encounter_id as enc_id,
    pe.encounter_class as enc_class,
    pe.encounter_type as enc_type,
    TRY(CAST(pe.encounter_start AS TIMESTAMP(3))) as enc_start,
    TRY(CAST(pe.encounter_end AS TIMESTAMP(3))) as enc_end,
    pe.was_inpatient as enc_was_inpatient,
    pe.was_emergency as enc_was_emergency,

    -- Medications (med_ prefix)
    pm.total_medications as med_total,
    pm.has_acetazolamide as med_acetazolamide,
    pm.has_steroids as med_steroids,
    pm.has_diuretic as med_diuretic,
    pm.medication_names as med_names,

    -- ============================================================================
    -- CBTN FIELD MAPPINGS
    -- ============================================================================

    -- shunt_required (diagnosis form) - shunt type
    sp.shunt_type as shunt_required,

    -- hydro_surgical_management (hydrocephalus_details form)
    sp.procedure_category as hydro_surgical_management,

    -- hydro_shunt_programmable (from device OR notes)
    COALESCE(pd.has_programmable, pn.mentions_programmable, false) as hydro_shunt_programmable,

    -- hydro_intervention (checkbox: Surgical, Medical, Hospitalization)
    CASE
        WHEN pe.was_inpatient = true THEN 'Hospitalization'
        WHEN sp.procedure_category IN ('Placement', 'Revision', 'ETV', 'Temporary EVD', 'Removal') THEN 'Surgical'
        WHEN sp.procedure_category = 'Reprogramming' THEN 'Medical'
        ELSE 'Surgical'
    END as hydro_intervention_type,

    -- Individual intervention flags
    CASE WHEN sp.procedure_category IN ('Placement', 'Revision', 'ETV', 'Temporary EVD', 'Removal') THEN true ELSE false END as intervention_surgical,
    CASE WHEN sp.procedure_category = 'Reprogramming' THEN true ELSE false END as intervention_medical,
    CASE WHEN pe.was_inpatient = true THEN true ELSE false END as intervention_hospitalization,

    -- hydro_nonsurg_management (checkbox: Acetazolamide, Steroids)
    pm.has_acetazolamide as nonsurg_acetazolamide,
    pm.has_steroids as nonsurg_steroids,
    pm.has_diuretic as nonsurg_diuretic,

    -- hydro_event_date (procedure date)
    TRY(CAST(COALESCE(sp.proc_performed_datetime, sp.proc_period_start) AS TIMESTAMP(3))) as hydro_event_date,

    -- ============================================================================
    -- DATA QUALITY INDICATORS
    -- ============================================================================

    CASE WHEN sp.proc_performed_datetime IS NOT NULL OR sp.proc_period_start IS NOT NULL THEN true ELSE false END as has_procedure_date,
    CASE WHEN pn.notes_text IS NOT NULL THEN true ELSE false END as has_procedure_notes,
    CASE WHEN pd.total_devices > 0 THEN true ELSE false END as has_device_record,
    CASE WHEN sp.proc_status = 'completed' THEN true ELSE false END as is_completed,
    CASE WHEN pr.confirmed_hydrocephalus = true THEN true ELSE false END as validated_by_reason_code,
    CASE WHEN pbs.body_sites_text IS NOT NULL THEN true ELSE false END as has_body_site_documentation,
    CASE WHEN perf.has_surgeon = true THEN true ELSE false END as has_surgeon_documented,
    CASE WHEN pe.encounter_id IS NOT NULL THEN true ELSE false END as linked_to_encounter,
    CASE WHEN pm.total_medications > 0 THEN true ELSE false END as has_nonsurgical_treatment

FROM shunt_procedures sp
LEFT JOIN procedure_reasons pr ON sp.procedure_id = pr.procedure_id
LEFT JOIN procedure_body_sites pbs ON sp.procedure_id = pbs.procedure_id
LEFT JOIN procedure_performers perf ON sp.procedure_id = perf.procedure_id
LEFT JOIN procedure_notes pn ON sp.procedure_id = pn.procedure_id
LEFT JOIN patient_devices pd ON sp.patient_fhir_id = pd.patient_fhir_id
LEFT JOIN procedure_encounters pe ON sp.procedure_id = pe.procedure_id
LEFT JOIN patient_medications pm ON sp.patient_fhir_id = pm.patient_fhir_id

ORDER BY sp.patient_fhir_id, sp.proc_period_start;