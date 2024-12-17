from sqlmesh import macro


@macro()
def generate_source_to_concept_map(evaluator):
    TEMPLATE = """
select
    sourceCode::VARCHAR(MAX),
    sourceName::VARCHAR(MAX),
    conceptId::INT,
    conceptName::VARCHAR(MAX),
    domainId::VARCHAR(50),
    sourceFrequency::INT,
    mappingStatus::VARCHAR(50),
    '{group}' as "group",
    '{source}' as source,
from {schema}.{model}
"""
    schema = "lth_bronze"

    # The seeds variable is an array of tuples of (seed_model, group, source)
    # If there are additional seeds, just add another tuple to this array autogenerate the code
    seeds = [
        ("flex__drugs_mappings", "drugs", "flex"),
        ("flex__job_role", "job_role", "flex"),
        ("flex__medical_specialty", "specialty", "flex"),
        ("flex__radiology_mappings", "radiology", "flex"),
        ("flex__vital_signs_mappings", "result", "flex"),
        ("flex__patient_demographics", "demographics", "flex"),
        ("flex__drug_routes", "drug_routes", "flex"),
        ("flex__blood_mappings", "result", "flex"),
        ("flex__unit_mappings", "units", "flex"),
        ("flex__unit_mappings_bloods", "units", "flex"),
        ("flex__decoded", "decoded", "flex"),
        ("flex__discharge_destination", "discharge_destination", "flex"),
        ("flex__admission_source", "admission_source", "flex"),
        ("swisslab__site_mappings", "specimen_type", "swisslab"),
        ("swisslab__anatomical_site_mappings", "anatomical_site", "swisslab"),
        ("flex__referrals", "result", "bi_referrals"),
        ("flex__referral_priority", "referral_priority", "bi_referrals"),
        ("swisslab__bacteria_presence", "bacteria_presence", "swisslab"),
        ("swisslab__bacteria_observation", "bacteria_observation", "swisslab"),
        ("swisslab__antibiotic_sensitivities", "bacteria_sensitivities", "swisslab"),
        ("swisslab__misc_test", "bacteriology_other_test", "swisslab"),
        ("swisslab__misc_test_result", "bacteriology_other_result", "swisslab"),
        ("flex__devices", "devices", "flex"),
        ("scr__results", "scr_results", "scr"),
        ("scr__field_mappings", "scr_fields", "scr"),
        ("scr__conditions_other", "scr_conditions", "scr"),
    ]

    out = []

    for model, group, source in seeds:
        s = TEMPLATE.format(schema=schema, model=model, group=group, source=source)
        if model == "flex__unit_mappings":
            s += """\n
                where sourceCode not in
                    ('RD/3698/13',
                    'RD/3891/13',
                    'RD/1962/14',
                    'RD/3795/14',
                    'RD/3659/34',
                    'RD/3698/11',
                    'RD/3698/10',
                    'RD/4899/13',
                    'RD/3585/5',
                    'RD/3585/10',
                    'RD/4555/5',
                    'RD/3626/9',
                    'RD/4710/5',
                    'RD/1962/19',
                    'RD/3673/5',
                    'RD/4699/42'
                    )
                """
        out.append(s)

    out = "\nUNION\n".join(out)

    return out
