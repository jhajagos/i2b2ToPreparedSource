--source_patient


/*
patient_num                   int64
vital_status_cd              object
birth_date           datetime64[ns]
death_date                   object
sex_cd                       object
age_in_years_num              int64
language_cd                  object
race_cd                      object
marital_status_cd            object
religion_cd                  object
zip_cd                       object
statecityzip_path            object
update_date          datetime64[ns]
download_date        datetime64[ns]
import_date          datetime64[ns]
sourcesystem_cd              object
upload_id                    object
age_in_months_num            object
age_in_days_num              object
age_in_hours_num             object
income_cd                    object
patient_blob                 object

*/

/*
       race_cd
0        other
1        asian
2     nat. am.
3        black
4  mid.eastern
5          api
6        white
7          unk
8     hispanic
9            @
*/

/*
* s_person_id -- Source identifier for patient or person
* s_gender	-- Source gender for person
* m_gender -- {MALE, FEMALE, UNKNOWN}
* s_birth_datetime -- Date of birth can be either a date or a date time
* s_death_datetime -- Date of death can either be either a date or a date time
* s_race -- Source race value for person
* m_race -- {White, Black or African American, American Indian or Alaska Native, . .}
* s_ethnicity -- Source ethnicity for person
* m_ethnicity -- Mapped value {Not Hispanic or Latino, Hispanic or Latino}
* k_location -- Not implemented

*/

 with patient_select as (select disitnct patient_num from i2b2_did.visit_dimension where inout_path
        like '%Inpatient%' and end_date between '2018-01-01' and '2018-06-01')
select
    cast(patient_num as varchar2(16)) as s_person_id,
    sex_cd as s_gender,
    case
        when sex_cd = 'M' then 'MALE'
        when sex_cd = 'F' then 'FEMALE'
        when sex_cd = 'U' then 'KNOWN'
     end as m_gender,
     birth_date as s_birth_datetime,
     death_date as s_death_datetime,
     race_cd as s_race,
     case
        when race_cd = 'white' then 'White'
        when race_cd = 'black' then 'Black or African American'
        when race_cd = 'nat. am.' then 'Ameican Indian or Alaska Native'
        when race_cd = 'mid.eastern' then 'Middle Eastern or North African'
        when race_cd = 'asian' then 'Asian'
     end as m_race,
     case when race_cd = 'hispanic' then 'hispanic' end as s_ethnicity,
     case when race_cd = 'hispanic' then 'Not Hispanic or Latino, Hispanic or Latino' end as m_ethnicity
from i2b2_did.patient_dimension pd join patient_select ps on pd.patient_num = ps.patient_num
where rownum < 10
;

select s_care_site, s_care_site as k_care_site from
    (select distinct coalesce(facility_id, '') || ' - ' || coalesce(location_zip, '') as s_care_site
    from i2b2_did.visit_dimension) t;

/*
* s_encounter_id -- Source identifier for encounter
* s_person_id -- Source identifier for patient or person
* s_visit_start_datetime -- Start date or date time or admission date or time
* s_visit_end_datetime -- End date or date time or discharge date or time
* s_visit_type -- Source of type of visit
* m_visit_type -- Type of visit {Inpatient, Outpatient, . .}
* k_care_site -- Linking key to location
* s_discharge_to -- Source value discharge disposition
* m_discharge_to -- Mapped value {}
* s_admitting_source -- Source value for admitting source
* m_admitting_source -- Mapped value {}
*/

select
    cast(encounter_num as varchar2(16)) as s_encounter_id,
    cast(patient_num as varchar2(16)) as s_person_id,
    start_date as s_visit_start_date_time,
    end_date as s_end_date_time,
    inout_path as s_visit_type,
    case
        when inout_path like '%Inpatient%' then 'Inpatient'
        when inout_path like '%Emergency%' then 'Emergency'
        when inout_path like '%Observation%' then 'Emergency'
        when inout_path like  '%Outpatient%' then 'Outpatient'
    end as m_visit_type,
    admitting_source as s_admitting_source,
    case
        when admitting_source = 'ED' then 'Emergency Department'
        when admitting_source = 'SN' then 'Skilled Nursing Facility'
    end as m_admission_source,
    discharge_disposition as s_discharge_to,
    case
        when discharge_disposition = 'SN' then 'Skilled Nursing Facility'
     end as  m_discharge_to,
     coalesce(facility_id, '') || ' - ' || coalesce(location_zip, '') as k_care_site
from i2b2_did.visit_dimension where rownum < 10
;

/*
   discharge_status
0                SN
1                HS
2                UN
3                OT
4                AM
5                HH
6                NI
7                EX
8                IP
9                NH
10               SH
11               HO
12               RH
*/

/*
  discharge_disposition
0                    UN
1                    OT
2                    NI
3                     A
4                     E
*/

/*
  admitting_source
0               SN
1               HS
2               ED
3               OT
4               NI
5               IP
*/


/*
* s_person_id -- Source identifier for patient or person
* s_encounter_id -- Source identifier for an encounter
* s_start_condition_datetime -- The first
* s_end_condition_datetime --
* s_condition_code
* s_condition_code_type -- {SNOMED, ICD9, ICD10}
* m_condition_code_oid -- {ICD9: 2.16.840.1.113883.6.103, ICD10: 2.16.840.1.113883.6.90}
* s_sequence_id --
* s_rank --
* m_rank -- {Primary, Secondary}
* s_condition_type --
* s_present_on_admission_indicator --
* i_exclude -- exclude from OHDSI mapper
*/

WITH encounter_select AS
  (SELECT DISTINCT encounter_num
   FROM i2b2_did.visit_dimension wvd
   WHERE wvd.inout_path LIKE '%Inpatient%'
     AND wvd.end_date BETWEEN to_date('2018-01-01', 'YYYY-MM-DD') AND to_date('2018-01-02', 'YYYY-MM-DD'))
SELECT cast(vd.patient_num AS varchar2(16)) AS patient_num,
       cast(vd.encounter_num AS varchar2(16)) AS encounter_num,
       code AS s_condition_code,
       vd.start_date AS s_start_condition_datetime,
       vd.end_date AS s_end_condition_datetime,
       coding_system AS s_condition_code_type,
       CASE
           WHEN coding_system = 'ICD9' THEN '2.16.840.1.113883.6.103'
           WHEN coding_system = 'ICD10-CM' THEN '2.16.840.1.113883.6.90'
       END AS m_condition_code_oid,
       md.modifier_cd || '|' || md.name_char AS s_rank,
       CASE
           WHEN md.name_char = 'Principle' THEN 'Primary'
           WHEN md.name_char = 'Secondary' THEN 'Secondary'
           ELSE NULL
       END AS m_rank,
       CASE
           WHEN md.name_char = 'Admitting' THEN 'Admitting'
           WHEN md.name_char = 'Final' THEN 'Final'
           WHEN md.name_char = 'EMR' THEN 'Preliminary'
           ELSE NULL
       END AS s_condition_type
FROM
  (SELECT ttobf.*
   FROM
     (SELECT tobf.*,
             CASE WHEN tobf.n_position = 8
      OR length(concept_cd) = 7)
   AND coding_system = 'ICD9' THEN 1 WHEN tobf.n_position = 0
   AND length(concept_cd) = 7)
AND coding_system = 'ICD10-CM' THEN 1 ELSE 0 END AS is_procedure_code
FROM
  (SELECT iobf.*,
          substr(concept_cd, instr(concept_cd, ':') + 1) AS code,
          substr(concept_cd, 0, instr(concept_cd, ':') - 1) AS coding_system,
          instr(concept_cd, '.') AS n_position
   FROM i2b2_did.observation_fact iobf
   JOIN encounter_select es ON iobf.encounter_num = es.encounter_num
   WHERE concept_cd LIKE 'ICD%') tobf) ttobf
WHERE is_procedure_code = 0 ) obf
  JOIN i2b2_did.visit_dimension vd ON vd.encounter_num = obf.encounter_num
  JOIN i2b2_did.modifier_dimension md ON md.modifier_cd = obf.modifier_cd
;
/*
s_rank        m_rank
DX_SOURCE:AD  Admitting            1              1                 1                           1                         1                      1                     1
DX_SOURCE:FI  Final               32             32                32                          32                        32                     32                    32
LOAD_TYPE:EMR EMR                 33             33                33                          33                        33                     33                    33
PDX:P         Principle           11             11                11                          11                        11                     11                    11
PDX:S         Secondary           22             22                22                          22                        22                     22                    22
*/
--{"Admitting": "52870002", "Final": "89100005", "Preliminary": "148006"}