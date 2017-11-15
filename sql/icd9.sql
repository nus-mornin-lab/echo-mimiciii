with icd9 as (
    select hadm_id,
           max(case when icd9_code in
               ('39891','40201','40291','40491', '40413', '40493',
                '4280', '4281', '42820', '42821', '42822', '42823',
                '42830', '42831', '42832', '42833', '42840', '42841',
                '42842', '42843', '4289', '428', '4282', '4283', '4284')
               then 1 else 0 end) as icd_chf,
           max(case when icd9_code like '4273%' then 1 else 0 end) as icd_afib,
           max(case when icd9_code like '585%' then 1 else 0 end) as icd_renal,
           max(case when icd9_code like '571%' then 1 else 0 end) as icd_liver,
           max(case when icd9_code in
               ('4660', '490', '4910', '4911', '49120',
                '49121', '4918', '4919', '4920', '4928',
                '494', '4940', '4941', '496')
               then 1 else 0 end) as icd_copd,
           max(case when icd9_code like '414%' then 1 else 0 end) as icd_cad,
           max(case when icd9_code like '430%'
               or icd9_code like '431%'
               or icd9_code like '432%'
               or icd9_code like '433%'
               or icd9_code like '434%'
               then 1 else 0 end) as icd_stroke,
           max(case when icd9_code between '140' and '239' then 1 else 0 end) as icd_malignancy
    from diagnoses_icd
    group by hadm_id
)

, icd9_cohort as (
    select *
    from (select hadm_id from cohort) co
    left join icd9 using (hadm_id)
)

select * from icd9_cohort;