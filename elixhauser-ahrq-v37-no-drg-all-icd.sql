-- This code uses v37 of Elixhauser comorbidities provided by AHRQ
-- However
--   it does *not* filter based on diagnosis related groups (DRGs)
--   it does *not* filter based on priority of ICD-9 code
-- As such, "comorbidities" identified are more likely to be associated with the primary reason for their hospital stay

-- The code proceeds in two stages
--  (1) convert ICD9_CODE from a VARCHAR to a CHAR(5)
--  (2) use AHRQ published rules to define comorbidities

-- note on (1), we *cannot* skip this step and use a varchar here
-- why? well, for example, VALVE is coded as BETWEEN '4240 ' and '424.99'
-- if we used a varchar, then '424.0' *is not* between this range
-- but if we use a char(5), then '424.0' *is* between this range
-- and we would like the latter behavior
-- it's possible removing the whitespaces would fix this - but I didn't test it.
-- This method is also more consistent with the AHRQ SAS code.

DROP MATERIALIZED VIEW IF EXISTS elixhauser_ahrq_no_drg_all_icd CASCADE;
CREATE MATERIALIZED VIEW elixhauser_ahrq_no_drg_all_icd as
with
icd as
(
  select patient.patienthealthsystemstayid
  , btrim(cast(diagnosis.icd9code as char(5)), ', ') as icd9code
  from patient inner join diagnosis on patient.patientUnitStayID = diagnosis.patientUnitStayID
  where icd9code <> ''
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
  when icd9code = '398.91' then 1
  when icd9code between '428.0' and '428.9' then 1
		end as CHF       /* Congestive heart failure */

-- cardiac arrhythmias is removed in up to date versions
, case
    when icd9code = '426.10' then 1
    when icd9code = '426.11' then 1
    when icd9code = '426.13' then 1
    when icd9code between '426.2' and '426.53' then 1
    when icd9code between '426.6' and '426.89' then 1
    when icd9code = '427.0' then 1
    when icd9code = '427.2' then 1
    when icd9code = '427.31' then 1
    when icd9code = '427.60' then 1
    when icd9code = '427.9' then 1
    when icd9code = '785.0' then 1
    when icd9code between 'V450' and 'V4509' then 1
    when icd9code between 'V533' and 'V5339' then 1
  end as ARYTHM /* Cardiac arrhythmias */

, CASE
  when icd9code between '093.20' and '093.24' then 1
  when icd9code between '394.0' and '397.1' then 1
  when icd9code = '397.9' then 1
  when icd9code between '424.0' and '424.99' then 1
  when icd9code between '746.3' and '746.6' then 1
  when icd9code = 'V422' then 1
  when icd9code = 'V433' then 1
		end as VALVE     /* Valvular disease */

, CASE
  when icd9code between '415.11' and '415.19' then 1
  when icd9code between '416.0' and '416.9' then 1
  when icd9code = '417.9' then 1
		end as PULMCIRC  /* Pulmonary circulation disorder */

, CASE
  when icd9code between '440.0' and '440.9' then 1
  when icd9code between '441.00' and '441.9' then 1
  when icd9code between '442.0' and '442.9' then 1
  when icd9code between '443.1' and '443.9' then 1
  when icd9code between '444.21' and '444.22' then 1
  when icd9code = '447.1' then 1
  when icd9code = '449' then 1
  when icd9code = '557.1' then 1
  when icd9code = '557.9' then 1
  when icd9code = 'V434' then 1
		end as PERIVASC  /* Peripheral vascular disorder */

, CASE
  when icd9code = '401.1' then 1
  when icd9code = '401.9' then 1
  when icd9code between '642.00' and '642.04' then 1
		end as HTN       /* Hypertension, uncomplicated */

, CASE
  when icd9code = '401.0' then 1
  when icd9code = '437.2' then 1
		end as HTNCX     /* Hypertension, complicated */


      /******************************************************************/
      /* The following are special, temporary formats used in the       */
      /* creation of the hypertension complicated comorbidity when      */
      /* overlapping with congestive heart failure or renal failure     */
      /* occurs. These temporary formats are referenced in the program  */
      /* called comoanaly2009.txt.                                      */
      /******************************************************************/
, CASE
  when icd9code between '642.20' and '642.24' then 1
		end as HTNPREG   /* Pre-existing hypertension complicating pregnancy */

, CASE
  when icd9code = '402.00' then 1
  when icd9code = '402.10' then 1
  when icd9code = '402.90' then 1
  when icd9code = '405.09' then 1
  when icd9code = '405.19' then 1
  when icd9code = '405.99'         then 1
		end as HTNWOCHF  /* Hypertensive heart disease without heart failure */

, CASE
  when icd9code = '402.01' then 1
  when icd9code = '402.11' then 1
  when icd9code = '402.91'         then 1
		end as HTNWCHF   /* Hypertensive heart disease with heart failure */

, CASE
  when icd9code = '403.00' then 1
  when icd9code = '403.10' then 1
  when icd9code = '403.90' then 1
  when icd9code = '405.01' then 1
  when icd9code = '405.11' then 1
  when icd9code = '405.91' then 1
  when icd9code between '642.10' and '642.14' then 1
		end as HRENWORF  /* Hypertensive renal disease without renal failure */

, CASE
  when icd9code = '403.01' then 1
  when icd9code = '403.11' then 1
  when icd9code = '403.91'         then 1
		end as HRENWRF   /* Hypertensive renal disease with renal failure */

, CASE
  when icd9code = '404.00' then 1
  when icd9code = '404.10' then 1
  when icd9code = '404.90'         then 1
		end as HHRWOHRF  /* Hypertensive heart and renal disease without heart or renal failure */

, CASE
  when icd9code = '404.01' then 1
  when icd9code = '404.11' then 1
  when icd9code = '404.91'         then 1
		end as HHRWCHF   /* Hypertensive heart and renal disease with heart failure */

, CASE
  when icd9code = '404.02' then 1
  when icd9code = '404.12' then 1
  when icd9code = '404.92'         then 1
		end as HHRWRF    /* Hypertensive heart and renal disease with renal failure */

, CASE
  when icd9code = '404.03' then 1
  when icd9code = '404.13' then 1
  when icd9code = '404.93'         then 1
		end as HHRWHRF   /* Hypertensive heart and renal disease with heart and renal failure */

, CASE
  when icd9code between '642.70' and '642.74' then 1
  when icd9code between '642.90' and '642.94' then 1
		end as OHTNPREG  /* Other hypertension in pregnancy */

      /******************** End Temporary Formats ***********************/

, CASE
  when icd9code between '342.0' and '344.9' then 1
  when icd9code between '438.20' and '438.53' then 1
  when icd9code = '780.72'         then 1
		end as PARA      /* Paralysis */

, CASE
  when icd9code between '330.0' and '331.9' then 1
  when icd9code = '332.0' then 1
  when icd9code = '333.4' then 1
  when icd9code = '333.5' then 1
  when icd9code = '333.7' then 1
  when icd9code in ('333.71','333.72','333.79','333.85','333.94') then 1
  when icd9code between '334.0' and '335.9' then 1
  when icd9code = '338.0' then 1
  when icd9code = '340' then 1
  when icd9code between '341.1' and '341.9' then 1
  when icd9code between '345.00' and '345.11' then 1
  when icd9code between '345.2' and '345.3' then 1
  when icd9code between '345.40' and '345.91' then 1
  when icd9code between '347.00' and '347.01' then 1
  when icd9code between '347.10' and '347.11' then 1
  when icd9code = '348.3' then 1 -- discontinued icd-9
  when icd9code between '649.40' and '649.44' then 1
  when icd9code = '768.7' then 1
  when icd9code between '768.70' and '768.73' then 1
  when icd9code = '780.3' then 1
  when icd9code = '780.31' then 1
  when icd9code = '780.32' then 1
  when icd9code = '780.33' then 1
  when icd9code = '780.39' then 1
  when icd9code = '780.97' then 1
  when icd9code = '784.3'         then 1
		end as NEURO     /* Other neurological */

, CASE
  when icd9code between '490' and '492.8' then 1
  when icd9code between '493.00' and '493.92' then 1
  when icd9code between '494' and '494.1' then 1
  when icd9code between '495.0' and '505' then 1
  when icd9code = '506.4'         then 1
		end as CHRNLUNG  /* Chronic pulmonary disease */

, CASE
  when icd9code between '250.00' and '250.33' then 1
  when icd9code between '648.00' and '648.04' then 1
  when icd9code between '249.00' and '249.31' then 1
		end as DM        /* Diabetes w/o chronic complications*/

, CASE
  when icd9code between '250.40' and '250.93' then 1
  when icd9code = '775.1' then 1
  when icd9code between '249.40' and '249.91' then 1
		end as DMCX      /* Diabetes w/ chronic complications */

, CASE
  when icd9code between '243' and '244.2' then 1
  when icd9code = '244.8' then 1
  when icd9code = '244.9'         then 1
		end as HYPOTHY   /* Hypothyroidism */

, CASE
  when icd9code = '585' then 1 -- discontinued code
  when icd9code = '585.3' then 1
  when icd9code = '585.4' then 1
  when icd9code = '585.5' then 1
  when icd9code = '585.6' then 1
  when icd9code = '585.9' then 1
  when icd9code = '586' then 1
  when icd9code = 'V420' then 1
  when icd9code = 'V451' then 1
  when icd9code between 'V560' and 'V5632' then 1
  when icd9code = 'V568' then 1
  when icd9code between 'V4511' and 'V4512' then 1
		end as RENLFAIL  /* Renal failure */

, CASE
  when icd9code = '070.22' then 1
  when icd9code = '070.23' then 1
  when icd9code = '070.32' then 1
  when icd9code = '070.33' then 1
  when icd9code = '070.44' then 1
  when icd9code = '070.54' then 1
  when icd9code = '456.0' then 1
  when icd9code = '456.1' then 1
  when icd9code = '456.20' then 1
  when icd9code = '456.21' then 1
  when icd9code = '571.0' then 1
  when icd9code = '571.2' then 1
  when icd9code = '571.3' then 1
  when icd9code between '571.40' and '571.49' then 1
  when icd9code = '571.5' then 1
  when icd9code = '571.6' then 1
  when icd9code = '571.8' then 1
  when icd9code = '571.9' then 1
  when icd9code = '572.3' then 1
  when icd9code = '572.8' then 1
  when icd9code = '573.5' then 1
  when icd9code = 'V427'         then 1
		end as LIVER     /* Liver disease */

, CASE
  when icd9code = '531.41' then 1
  when icd9code = '531.51' then 1
  when icd9code = '531.61' then 1
  when icd9code = '531.70' then 1
  when icd9code = '531.71' then 1
  when icd9code = '531.91' then 1
  when icd9code = '532.41' then 1
  when icd9code = '532.51' then 1
  when icd9code = '532.61' then 1
  when icd9code = '532.70' then 1
  when icd9code = '532.71' then 1
  when icd9code = '532.91' then 1
  when icd9code = '533.41' then 1
  when icd9code = '533.51' then 1
  when icd9code = '533.61' then 1
  when icd9code = '533.70' then 1
  when icd9code = '533.71' then 1
  when icd9code = '533.91' then 1
  when icd9code = '534.41' then 1
  when icd9code = '534.51' then 1
  when icd9code = '534.61' then 1
  when icd9code = '534.70' then 1
  when icd9code = '534.71' then 1
  when icd9code = '534.91'         then 1
		end as ULCER     /* Chronic Peptic ulcer disease (includes bleeding only if obstruction is also present) */

, CASE
  when icd9code between '042' and '044.9' then 1
		end as AIDS      /* HIV and AIDS */

, CASE
  when icd9code between '200.00' and '202.38' then 1
  when icd9code between '202.50' and '203.01' then 1
  when icd9code = '238.6' then 1
  when icd9code = '273.3' then 1
  when icd9code between '203.02' and '203.82' then 1
		end as LYMPH     /* Lymphoma */

, CASE
  when icd9code between '196.0' and '199.1' then 1
  when icd9code between '209.70' and '209.75' then 1
  when icd9code = '209.79' then 1
  when icd9code = '789.51'         then 1
		end as METS      /* Metastatic cancer */

, CASE
  when icd9code between '140.0' and '172.9' then 1
  when icd9code between '174.0' and '175.9' then 1
  when icd9code between '179' and '195.8' then 1
  when icd9code between '209.00' and '209.24' then 1
  when icd9code between '209.25' and '209.3' then 1
  when icd9code between '209.30' and '209.36' then 1
  when icd9code between '258.01' and '258.03' then 1
		end as TUMOR     /* Solid tumor without metastasis */

, CASE
  when icd9code = '701.0' then 1
  when icd9code between '710.0' and '710.9' then 1
  when icd9code between '714.0' and '714.9' then 1
  when icd9code between '720.0' and '720.9' then 1
  when icd9code = '725' then 1
		end as ARTH              /* Rheumatoid arthritis/collagen vascular diseases */

, CASE
  when icd9code between '286.0' and '286.9' then 1
  when icd9code = '287.1' then 1
  when icd9code between '287.3' and '287.5' then 1
  when icd9code between '649.30' and '649.34' then 1
  when icd9code = '289.84'         then 1
		end as COAG      /* Coagulation deficiency */

, CASE
  when icd9code = '278.0' then 1
  when icd9code = '278.00' then 1
  when icd9code = '278.01' then 1
  when icd9code = '278.03' then 1
  when icd9code between '649.10' and '649.14' then 1
  when icd9code between 'V8530' and 'V8539' then 1
  when icd9code = 'V854' then 1 -- hierarchy used for AHRQ v3.6 and earlier
  when icd9code between 'V8541' and 'V8545' then 1
  when icd9code = 'V8554' then 1
  when icd9code = '793.91'         then 1
		end as OBESE     /* Obesity      */

, CASE
  when icd9code between '260' and '263.9' then 1
  when icd9code between '783.21' and '783.22' then 1
		end as WGHTLOSS  /* Weight loss */

, CASE
  when icd9code between '276.0' and '276.9' then 1
		end as LYTES     /* Fluid and electrolyte disorders - note:
                                      this comorbidity should be dropped when
                                      used with the AHRQ Patient Safety Indicators*/
, CASE
  when icd9code = '280.0' then 1
  when icd9code between '648.20' and '648.24' then 1
		end as BLDLOSS   /* Blood loss anemia */

, CASE
  when icd9code between '280.1' and '281.9' then 1
  when icd9code between '285.21' and '285.29' then 1
  when icd9code = '285.9'         then 1
		end as ANEMDEF  /* Deficiency anemias */

, CASE
  when icd9code between '291.0' and '291.3' then 1
  when icd9code = '291.5' then 1
  when icd9code = '291.8' then 1
  when icd9code = '291.81' then 1
  when icd9code = '291.82' then 1
  when icd9code = '291.89' then 1
  when icd9code = '291.9' then 1
  when icd9code between '303.00' and '303.93' then 1
  when icd9code between '305.00' and '305.03' then 1
		end as ALCOHOL   /* Alcohol abuse */

, CASE
  when icd9code = '292.0' then 1
  when icd9code between '292.82' and '292.89' then 1
  when icd9code = '292.9' then 1
  when icd9code between '304.00' and '304.93' then 1
  when icd9code between '305.20' and '305.93' then 1
  when icd9code between '648.30' and '648.34' then 1
		end as DRUG      /* Drug abuse */

, CASE
  when icd9code between '295.00' and '298.9' then 1
  when icd9code = '299.10' then 1
  when icd9code = '299.11'         then 1
		end as PSYCH    /* Psychoses */

, CASE
  when icd9code = '300.4' then 1
  when icd9code = '301.12' then 1
  when icd9code = '309.0' then 1
  when icd9code = '309.1' then 1
  when icd9code = '311'         then 1
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

select pat.uniquepid, pat.patienthealthsystemstayid
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

from patient pat
left join eligrp eli
  on pat.patienthealthsystemstayid = eli.patienthealthsystemstayid
order by pat.patienthealthsystemstayid;
