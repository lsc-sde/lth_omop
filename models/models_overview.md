{% docs __overview__ %}
# OHDSI/OMOP Data Harmonisation project

__Data Science Team, Lancashire Teaching Hospitals NHS Foundation Trust__

## Introduction

Lancashire Teaching Hospitals NHS Foundation Trust (LTH) is a digitally mature secondary care provider, major trauma centre and multi-specialty tertiary referral centre in Lancashire and South Cumbria ICS (LSC).
LTH developed a cloud-native, secure, data science platform on Microsoft Azure that has proven invaluable by enabling data scientists from regional, national, and international organisations to undertake advanced analytics without
transferring data out.
This led to LSC being a partner in a successful bid for £15 million to build LSC Secure Data Environment (LSCSDE) as a federated member of the North-west Secure Data Environment (NWSDE).

We have been awarded [HDRUK Team of the Year 2024](https://www.hdruk.ac.uk/news/winners-announced-2024-hdr-uk-annual-prizes) for our work on this. 

## OHDSI/OMOP

LTH have access to routinely collected healthcare data for over 2.25 million patients spanning 15 years, covering most aspects of secondary care.
This data is stored in multiple disparate databases.

We have invested in a multi-year, large-scale data harmonisation program with the [Observational Medical Outcomes Partnership (OMOP) Common Data Model (CDM)](https://www.ohdsi.org/data-standardization/) as the target model.
We have secured additional external funding from [EHDEN-HDRUK](https://www.ehden.eu/datapartners/) further validating our strategy.

OMOP is supported by the [Observational Health Data Sciences and Informatics (OHDSI)](https://www.ohdsi.org/) program, a multi-stakeholder, global collaborative that aims to deliver value out of health data through large-scale analytics.
Harmonising to OMOP makes our data immediately valuable using standardised, open-source, analytics software maintained by a global community of researchers.

LTH is a member of the [HDRUK Alliance](https://ukhealthdata.org/members/) and will also become member of the global OHDSI federation collaborating on international research studies - both observational as well as clinical trials.

## Benefits

### Federated observational studies

Aggregated analysis across multiple organisations can be made without sharing any patient-level data or requiring complex data sharing agreements allowing rapid translational data science.

### Clinical Trials

Cohort definitions for UK/International clinical trials prepared by any lead site using OMOP can be executed on our database to rapidly establish study feasibility and identify eligible patients, creating opportunities for
a range of portfolio studies and build links with international academic and clinical collaborators.
This is especially important for Lancashire and South Cumbria where participation in clinical trials has been historically low.
It will also allow us to strategically target patient groups for 'at-risk' clinical trials that maybe struggling with recruitment.

LTH are working with TriNetX, a global healthcare data platform, to enhance our capabilities for clinical trial feasibility assessments, cohort discovery and evidence generation using real-world data.
The OMOP/OHDSI data mapping will be a critical enabler for further automated data transformation into TriNetX.

### Lancashire and South Cumbria Secure Data Environment

The LTH OMOP database will become a core component of LSCSDE and is part of the wider LSC Northern Star Intelligence Architecture (NSIA).
NSIA aims to unify secondary care and primary care data in a single ICS cloud data warehouse with the ability to link to social care, local government and environmental data through record-level and fuzzy linkage.
It is anticipated that successful completion of the LTH mapping will allow other regional providers to follow.

## Governance

LTH OMOP database has been approved by the UK Research Ethics Committee for routine ressearch by external researchers.

## Technical Details

This website holds the technical documentation for the Extract-Load-Transform data pipeline that is being developed.
This is the largest data harmonisation exercise involving secondary care data in North West England and will be undertaken in multiple phases.

The principles of development are well encapsulated by the vision described by a core component being used to develop this pipeline - <https://www.getdbt.com/>.
Read more about the benefits of this approach here -> <https://www.getdbt.com/product/what-is-dbt/>

Project code is hosted on GitHub at <https://github.com/LTHTR-DST/dbt_omop>.
We have also developed a [OMOP CDM Entity Relationship Diagram](http://omop-erd.surge.sh/omop_cdm/) to support this work.

## Project Team

- Vishnu V Chandrabalan, Consultant Surgeon and Head of Data Science (@vvcb)
- Quin Davies, Lead Data Scientist and OMOP Analytics Engineer (@quindavies )
- Tim Howcroft, Clinical Scientist (@timhowcroft)
- Dale Kirkwood, Emergency Medicine Registrar, Public Empowerment Lead
- Stephen Dobson, Chief Infomation Officer
- Jo Knight, Professor of Data Science, Lancaster University
- Kina Bennett, Head of Research and Development
- Louise Acheson, Information Governance Lead
- Sally Stewart, Programme Director, NWSDE

{% enddocs %}
