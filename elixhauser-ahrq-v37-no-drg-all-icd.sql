-- This code uses v37 of Elixhauser comorbidities provided by AHRQ
-- However
--   it does *not* filter based on diagnosis related groups (DRGs)
--   it does *not* filter based on priority of ICD-9 code
-- As such, "comorbidities" identified are more likely to be associated with the primary reason for their hospital stay

-- The code proceeds in two stages
--  (1) convert ICD9_CODE from a VARCHAR to a CHAR(5)
--  (2) use AHRQ published rules to define comorbidities

-- note on (1), we *cannot* skip this step and use a varchar here
-- why? well, for example, VALVE is coded as BETWEEN '4240 ' and '42499'
-- if we used a varchar, then '4240' *is not* between this range
-- but if we use a char(5), then '4240' *is* between this range
-- and we would like the latter behavior
-- it's possible removing the whitespaces would fix this - but I didn't test it.
-- This method is also more consistent with the AHRQ SAS code.

DROP MATERIALIZED VIEW IF EXISTS elixhauser_ahrq_no_drg_all_icd CASCADE;
CREATE MATERIALIZED VIEW elixhauser_ahrq_no_drg_all_icd as
with
icd as
(
  select patient.patienthealthsystemstayid
    , cast(diagnosis.icd9code as char(5)) as icd9code
  from patient, diagnosis
)
,
eliflg as
(
select patienthealthsystemstayid, icd9code
-- note that these codes will seem incomplete at first
-- for example, CHF is missing a lot of codes referenced in the literature (402.11, 402.91, etc)
-- these codes are captured by hypertension flags instead
-- later there are some complicated rules which confirm/reject those codes as CHF
, CASE
  when icd9code = '39891' then 1
  when icd9code between '4280 ' and '4289 ' then 1
		end as CHF       /* Congestive heart failure */

-- cardiac arrhythmias is removed in up to date versions
, case
    when icd9code = '42610' then 1
    when icd9code = '42611' then 1
    when icd9code = '42613' then 1
    when icd9code between '4262 ' and '42653' then 1
    when icd9code between '4266 ' and '42689' then 1
    when icd9code = '4270 ' then 1
    when icd9code = '4272 ' then 1
    when icd9code = '42731' then 1
    when icd9code = '42760' then 1
    when icd9code = '4279 ' then 1
    when icd9code = '7850 ' then 1
    when icd9code between 'V450 ' and 'V4509' then 1
    when icd9code between 'V533 ' and 'V5339' then 1
  end as ARYTHM /* Cardiac arrhythmias */

, CASE
  when icd9code between '09320' and '09324' then 1
  when icd9code between '3940 ' and '3971 ' then 1
  when icd9code = '3979 ' then 1
  when icd9code between '4240 ' and '42499' then 1
  when icd9code between '7463 ' and '7466 ' then 1
  when icd9code = 'V422 ' then 1
  when icd9code = 'V433 ' then 1
		end as VALVE     /* Valvular disease */

, CASE
  when icd9code between '41511' and '41519' then 1
  when icd9code between '4160 ' and '4169 ' then 1
  when icd9code = '4179 ' then 1
		end as PULMCIRC  /* Pulmonary circulation disorder */

, CASE
  when icd9code between '4400 ' and '4409 ' then 1
  when icd9code between '44100' and '4419 ' then 1
  when icd9code between '4420 ' and '4429 ' then 1
  when icd9code between '4431 ' and '4439 ' then 1
  when icd9code between '44421' and '44422' then 1
  when icd9code = '4471 ' then 1
  when icd9code = '449  ' then 1
  when icd9code = '5571 ' then 1
  when icd9code = '5579 ' then 1
  when icd9code = 'V434 ' then 1
		end as PERIVASC  /* Peripheral vascular disorder */

, CASE
  when icd9code = '4011 ' then 1
  when icd9code = '4019 ' then 1
  when icd9code between '64200' and '64204' then 1
		end as HTN       /* Hypertension, uncomplicated */

, CASE
  when icd9code = '4010 ' then 1
  when icd9code = '4372 ' then 1
		end as HTNCX     /* Hypertension, complicated */


      /******************************************************************/
      /* The following are special, temporary formats used in the       */
      /* creation of the hypertension complicated comorbidity when      */
      /* overlapping with congestive heart failure or renal failure     */
      /* occurs. These temporary formats are referenced in the program  */
      /* called comoanaly2009.txt.                                      */
      /******************************************************************/
, CASE
  when icd9code between '64220' and '64224' then 1
		end as HTNPREG   /* Pre-existing hypertension complicating pregnancy */

, CASE
  when icd9code = '40200' then 1
  when icd9code = '40210' then 1
  when icd9code = '40290' then 1
  when icd9code = '40509' then 1
  when icd9code = '40519' then 1
  when icd9code = '40599'         then 1
		end as HTNWOCHF  /* Hypertensive heart disease without heart failure */

, CASE
  when icd9code = '40201' then 1
  when icd9code = '40211' then 1
  when icd9code = '40291'         then 1
		end as HTNWCHF   /* Hypertensive heart disease with heart failure */

, CASE
  when icd9code = '40300' then 1
  when icd9code = '40310' then 1
  when icd9code = '40390' then 1
  when icd9code = '40501' then 1
  when icd9code = '40511' then 1
  when icd9code = '40591' then 1
  when icd9code between '64210' and '64214' then 1
		end as HRENWORF  /* Hypertensive renal disease without renal failure */

, CASE
  when icd9code = '40301' then 1
  when icd9code = '40311' then 1
  when icd9code = '40391'         then 1
		end as HRENWRF   /* Hypertensive renal disease with renal failure */

, CASE
  when icd9code = '40400' then 1
  when icd9code = '40410' then 1
  when icd9code = '40490'         then 1
		end as HHRWOHRF  /* Hypertensive heart and renal disease without heart or renal failure */

, CASE
  when icd9code = '40401' then 1
  when icd9code = '40411' then 1
  when icd9code = '40491'         then 1
		end as HHRWCHF   /* Hypertensive heart and renal disease with heart failure */

, CASE
  when icd9code = '40402' then 1
  when icd9code = '40412' then 1
  when icd9code = '40492'         then 1
		end as HHRWRF    /* Hypertensive heart and renal disease with renal failure */

, CASE
  when icd9code = '40403' then 1
  when icd9code = '40413' then 1
  when icd9code = '40493'         then 1
		end as HHRWHRF   /* Hypertensive heart and renal disease with heart and renal failure */

, CASE
  when icd9code between '64270' and '64274' then 1
  when icd9code between '64290' and '64294' then 1
		end as OHTNPREG  /* Other hypertension in pregnancy */

      /******************** End Temporary Formats ***********************/

, CASE
  when icd9code between '3420 ' and '3449 ' then 1
  when icd9code between '43820' and '43853' then 1
  when icd9code = '78072'         then 1
		end as PARA      /* Paralysis */

, CASE
  when icd9code between '3300 ' and '3319 ' then 1
  when icd9code = '3320 ' then 1
  when icd9code = '3334 ' then 1
  when icd9code = '3335 ' then 1
  when icd9code = '3337 ' then 1
  when icd9code in ('33371','33372','33379','33385','33394') then 1
  when icd9code between '3340 ' and '3359 ' then 1
  when icd9code = '3380 ' then 1
  when icd9code = '340  ' then 1
  when icd9code between '3411 ' and '3419 ' then 1
  when icd9code between '34500' and '34511' then 1
  when icd9code between '3452 ' and '3453 ' then 1
  when icd9code between '34540' and '34591' then 1
  when icd9code between '34700' and '34701' then 1
  when icd9code between '34710' and '34711' then 1
  when icd9code = '3483' then 1 -- discontinued icd-9
  when icd9code between '64940' and '64944' then 1
  when icd9code = '7687 ' then 1
  when icd9code between '76870' and '76873' then 1
  when icd9code = '7803 ' then 1
  when icd9code = '78031' then 1
  when icd9code = '78032' then 1
  when icd9code = '78033' then 1
  when icd9code = '78039' then 1
  when icd9code = '78097' then 1
  when icd9code = '7843 '         then 1
		end as NEURO     /* Other neurological */

, CASE
  when icd9code between '490  ' and '4928 ' then 1
  when icd9code between '49300' and '49392' then 1
  when icd9code between '494  ' and '4941 ' then 1
  when icd9code between '4950 ' and '505  ' then 1
  when icd9code = '5064 '         then 1
		end as CHRNLUNG  /* Chronic pulmonary disease */

, CASE
  when icd9code between '25000' and '25033' then 1
  when icd9code between '64800' and '64804' then 1
  when icd9code between '24900' and '24931' then 1
		end as DM        /* Diabetes w/o chronic complications*/

, CASE
  when icd9code between '25040' and '25093' then 1
  when icd9code = '7751 ' then 1
  when icd9code between '24940' and '24991' then 1
		end as DMCX      /* Diabetes w/ chronic complications */

, CASE
  when icd9code between '243  ' and '2442 ' then 1
  when icd9code = '2448 ' then 1
  when icd9code = '2449 '         then 1
		end as HYPOTHY   /* Hypothyroidism */

, CASE
  when icd9code = '585  ' then 1 -- discontinued code
  when icd9code = '5853 ' then 1
  when icd9code = '5854 ' then 1
  when icd9code = '5855 ' then 1
  when icd9code = '5856 ' then 1
  when icd9code = '5859 ' then 1
  when icd9code = '586  ' then 1
  when icd9code = 'V420 ' then 1
  when icd9code = 'V451 ' then 1
  when icd9code between 'V560 ' and 'V5632' then 1
  when icd9code = 'V568 ' then 1
  when icd9code between 'V4511' and 'V4512' then 1
		end as RENLFAIL  /* Renal failure */

, CASE
  when icd9code = '07022' then 1
  when icd9code = '07023' then 1
  when icd9code = '07032' then 1
  when icd9code = '07033' then 1
  when icd9code = '07044' then 1
  when icd9code = '07054' then 1
  when icd9code = '4560 ' then 1
  when icd9code = '4561 ' then 1
  when icd9code = '45620' then 1
  when icd9code = '45621' then 1
  when icd9code = '5710 ' then 1
  when icd9code = '5712 ' then 1
  when icd9code = '5713 ' then 1
  when icd9code between '57140' and '57149' then 1
  when icd9code = '5715 ' then 1
  when icd9code = '5716 ' then 1
  when icd9code = '5718 ' then 1
  when icd9code = '5719 ' then 1
  when icd9code = '5723 ' then 1
  when icd9code = '5728 ' then 1
  when icd9code = '5735 ' then 1
  when icd9code = 'V427 '         then 1
		end as LIVER     /* Liver disease */

, CASE
  when icd9code = '53141' then 1
  when icd9code = '53151' then 1
  when icd9code = '53161' then 1
  when icd9code = '53170' then 1
  when icd9code = '53171' then 1
  when icd9code = '53191' then 1
  when icd9code = '53241' then 1
  when icd9code = '53251' then 1
  when icd9code = '53261' then 1
  when icd9code = '53270' then 1
  when icd9code = '53271' then 1
  when icd9code = '53291' then 1
  when icd9code = '53341' then 1
  when icd9code = '53351' then 1
  when icd9code = '53361' then 1
  when icd9code = '53370' then 1
  when icd9code = '53371' then 1
  when icd9code = '53391' then 1
  when icd9code = '53441' then 1
  when icd9code = '53451' then 1
  when icd9code = '53461' then 1
  when icd9code = '53470' then 1
  when icd9code = '53471' then 1
  when icd9code = '53491'         then 1
		end as ULCER     /* Chronic Peptic ulcer disease (includes bleeding only if obstruction is also present) */

, CASE
  when icd9code between '042  ' and '0449 ' then 1
		end as AIDS      /* HIV and AIDS */

, CASE
  when icd9code between '20000' and '20238' then 1
  when icd9code between '20250' and '20301' then 1
  when icd9code = '2386 ' then 1
  when icd9code = '2733 ' then 1
  when icd9code between '20302' and '20382' then 1
		end as LYMPH     /* Lymphoma */

, CASE
  when icd9code between '1960 ' and '1991 ' then 1
  when icd9code between '20970' and '20975' then 1
  when icd9code = '20979' then 1
  when icd9code = '78951'         then 1
		end as METS      /* Metastatic cancer */

, CASE
  when icd9code between '1400 ' and '1729 ' then 1
  when icd9code between '1740 ' and '1759 ' then 1
  when icd9code between '179  ' and '1958 ' then 1
  when icd9code between '20900' and '20924' then 1
  when icd9code between '20925' and '2093 ' then 1
  when icd9code between '20930' and '20936' then 1
  when icd9code between '25801' and '25803' then 1
		end as TUMOR     /* Solid tumor without metastasis */

, CASE
  when icd9code = '7010 ' then 1
  when icd9code between '7100 ' and '7109 ' then 1
  when icd9code between '7140 ' and '7149 ' then 1
  when icd9code between '7200 ' and '7209 ' then 1
  when icd9code = '725  ' then 1
		end as ARTH              /* Rheumatoid arthritis/collagen vascular diseases */

, CASE
  when icd9code between '2860 ' and '2869 ' then 1
  when icd9code = '2871 ' then 1
  when icd9code between '2873 ' and '2875 ' then 1
  when icd9code between '64930' and '64934' then 1
  when icd9code = '28984'         then 1
		end as COAG      /* Coagulation deficiency */

, CASE
  when icd9code = '2780 ' then 1
  when icd9code = '27800' then 1
  when icd9code = '27801' then 1
  when icd9code = '27803' then 1
  when icd9code between '64910' and '64914' then 1
  when icd9code between 'V8530' and 'V8539' then 1
  when icd9code = 'V854 ' then 1 -- hierarchy used for AHRQ v3.6 and earlier
  when icd9code between 'V8541' and 'V8545' then 1
  when icd9code = 'V8554' then 1
  when icd9code = '79391'         then 1
		end as OBESE     /* Obesity      */

, CASE
  when icd9code between '260  ' and '2639 ' then 1
  when icd9code between '78321' and '78322' then 1
		end as WGHTLOSS  /* Weight loss */

, CASE
  when icd9code between '2760 ' and '2769 ' then 1
		end as LYTES     /* Fluid and electrolyte disorders - note:
                                      this comorbidity should be dropped when
                                      used with the AHRQ Patient Safety Indicators*/
, CASE
  when icd9code = '2800 ' then 1
  when icd9code between '64820' and '64824' then 1
		end as BLDLOSS   /* Blood loss anemia */

, CASE
  when icd9code between '2801 ' and '2819 ' then 1
  when icd9code between '28521' and '28529' then 1
  when icd9code = '2859 '         then 1
		end as ANEMDEF  /* Deficiency anemias */

, CASE
  when icd9code between '2910 ' and '2913 ' then 1
  when icd9code = '2915 ' then 1
  when icd9code = '2918 ' then 1
  when icd9code = '29181' then 1
  when icd9code = '29182' then 1
  when icd9code = '29189' then 1
  when icd9code = '2919 ' then 1
  when icd9code between '30300' and '30393' then 1
  when icd9code between '30500' and '30503' then 1
		end as ALCOHOL   /* Alcohol abuse */

, CASE
  when icd9code = '2920 ' then 1
  when icd9code between '29282' and '29289' then 1
  when icd9code = '2929 ' then 1
  when icd9code between '30400' and '30493' then 1
  when icd9code between '30520' and '30593' then 1
  when icd9code between '64830' and '64834' then 1
		end as DRUG      /* Drug abuse */

, CASE
  when icd9code between '29500' and '2989 ' then 1
  when icd9code = '29910' then 1
  when icd9code = '29911'         then 1
		end as PSYCH    /* Psychoses */

, CASE
  when icd9code = '3004 ' then 1
  when icd9code = '30112' then 1
  when icd9code = '3090 ' then 1
  when icd9code = '3091 ' then 1
  when icd9code = '311  '         then 1
		end as DEPRESS  /* Depression */
from icd
)
-- collapse the icd9code specific flags into patienthealthsystemstayid specific flags
-- this groups comorbidities together for a single patient admission
, eligrp as
(
  select patienthealthsystemstayid
  , max(chf) as chf
  , max(arythm) as arythm
  , max(valve) as valve
  , max(pulmcirc) as pulmcirc
  , max(perivasc) as perivasc
  , max(htn) as htn
  , max(htncx) as htncx
  , max(htnpreg) as htnpreg
  , max(htnwochf) as htnwochf
  , max(htnwchf) as htnwchf
  , max(hrenworf) as hrenworf
  , max(hrenwrf) as hrenwrf
  , max(hhrwohrf) as hhrwohrf
  , max(hhrwchf) as hhrwchf
  , max(hhrwrf) as hhrwrf
  , max(hhrwhrf) as hhrwhrf
  , max(ohtnpreg) as ohtnpreg
  , max(para) as para
  , max(neuro) as neuro
  , max(chrnlung) as chrnlung
  , max(dm) as dm
  , max(dmcx) as dmcx
  , max(hypothy) as hypothy
  , max(renlfail) as renlfail
  , max(liver) as liver
  , max(ulcer) as ulcer
  , max(aids) as aids
  , max(lymph) as lymph
  , max(mets) as mets
  , max(tumor) as tumor
  , max(arth) as arth
  , max(coag) as coag
  , max(obese) as obese
  , max(wghtloss) as wghtloss
  , max(lytes) as lytes
  , max(bldloss) as bldloss
  , max(anemdef) as anemdef
  , max(alcohol) as alcohol
  , max(drug) as drug
  , max(psych) as psych
  , max(depress) as depress
from eliflg
group by patienthealthsystemstayid
)
-- now merge these flags together to define elixhauser
-- most are straightforward.. but hypertension flags are a bit more complicated

-- NOTE: NOT USING SUBJECT_ID HERE! If an equivalent exists, please use it!
-- select adm.subject_id, adm.patienthealthsystemstayid
select adm.patienthealthsystemstayid
, case
    when chf     = 1 then 1
    when htnwchf = 1 then 1
    when hhrwchf = 1 then 1
    when hhrwhrf = 1 then 1
  else 0 end as CONGESTIVE_HEART_FAILURE
, case
    when arythm = 1 then 1
  else 0 end as CARDIAC_ARRHYTHMIAS
, case when    valve = 1 then 1 else 0 end as VALVULAR_DISEASE
, case when pulmcirc = 1 then 1 else 0 end as PULMONARY_CIRCULATION
, case when perivasc = 1 then 1 else 0 end as PERIPHERAL_VASCULAR

-- we combine "htn" and "htncx" into "HYPERTENSION"
-- note "htn" (hypertension) is only 1 if "htncx" (complicated hypertension) is 0
-- this matters if you filter on DRG but for this query we can just merge them immediately
, case
    when htn = 1 then 1
    when htncx = 1 then 1
    when htnpreg = 1 then 1
    when htnwochf = 1 then 1
    when htnwchf = 1 then 1
    when hrenworf = 1 then 1
    when hrenwrf = 1 then 1
    when hhrwohrf = 1 then 1
    when hhrwchf = 1 then 1
    when hhrwrf = 1 then 1
    when hhrwhrf = 1 then 1
    when ohtnpreg = 1 then 1
  else 0 end as HYPERTENSION

, case when para      = 1 then 1 else 0 end as PARALYSIS
, case when neuro     = 1 then 1 else 0 end as OTHER_NEUROLOGICAL
, case when chrnlung  = 1 then 1 else 0 end as CHRONIC_PULMONARY
, case
    -- only the more severe comorbidity (complicated diabetes) is kept
    when dmcx = 1 then 0
    when dm = 1 then 1
  else 0 end as DIABETES_UNCOMPLICATED
, case when dmcx    = 1 then 1 else 0 end as DIABETES_COMPLICATED
, case when hypothy = 1 then 1 else 0 end as HYPOTHYROIDISM
, case
    when renlfail = 1 then 1
    when hrenwrf  = 1 then 1
    when hhrwrf   = 1 then 1
    when hhrwhrf  = 1 then 1
  else 0 end as RENAL_FAILURE

, case when liver = 1 then 1 else 0 end as LIVER_DISEASE
, case when ulcer = 1 then 1 else 0 end as PEPTIC_ULCER
, case when aids = 1 then 1 else 0 end as AIDS
, case when lymph = 1 then 1 else 0 end as LYMPHOMA
, case when mets = 1 then 1 else 0 end as METASTATIC_CANCER
, case
    -- only the more severe comorbidity (metastatic cancer) is kept
    when mets = 1 then 0
    when tumor = 1 then 1
 else 0 end as SOLID_TUMOR
, case when arth = 1 then 1 else 0 end as RHEUMATOID_ARTHRITIS
, case when coag = 1 then 1 else 0 end as COAGULOPATHY
, case when obese = 1 then 1 else 0 end as OBESITY
, case when wghtloss = 1 then 1 else 0 end as WEIGHT_LOSS
, case when lytes = 1 then 1 else 0 end as FLUID_ELECTROLYTE
, case when bldloss = 1 then 1 else 0 end as BLOOD_LOSS_ANEMIA
, case when anemdef = 1 then 1 else 0 end as DEFICIENCY_ANEMIAS
, case when alcohol = 1 then 1 else 0 end as ALCOHOL_ABUSE
, case when drug = 1 then 1 else 0 end as DRUG_ABUSE
, case when psych = 1 then 1 else 0 end as PSYCHOSES
, case when depress = 1 then 1 else 0 end as DEPRESSION

from admissionsdx adm
left join eligrp eli
  on adm.patienthealthsystemstayid = eli.patienthealthsystemstayid
order by adm.patienthealthsystemstayid;
