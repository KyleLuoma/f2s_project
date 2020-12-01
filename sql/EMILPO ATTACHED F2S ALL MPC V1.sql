select 
STANDARD_HASH(TO_NUMBER(emilpo.soldr_attchmnt_t.SSN) + &&MASK + 4294967295 + &&SEED_NUM, 'MD5') AS SSN_MASK_HASH, 
emilpo.soldr_attchmnt_t.*,
person_status_t.TAPDB_REC_STAT_CD
from emilpo.soldr_attchmnt_t
join emilpo.person_status_t on soldr_attchmnt_t.SSN = person_status_t.SSN
where attach_exp_dt is null and person_status_t.tapdb_rec_stat_cd = 'G'
order by attach_start_dt desc