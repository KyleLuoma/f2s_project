----
-- EMILPO Assignment extraction script for all MPC
-- Filters to only Active component record categories
-- V1.6 Corrects issue where duplicate records returned due to multiple AOCs per member
-- V1.7 Adds Parent UIC Code from the ORGANIZATION_T table snd STRUC CMD CD from UNIT_RECORT_T Table
-- V1.8 Adds ASI and SQI columns and values
-- V1.9 Pivots AOC/MOS into multiple columns
-- v1.11 Adds UIC PARA LN composite key and SSN MASK
-- v1.12 Adds SSN_MASK_HASH
-- v1.13 Updated JOIN and FROM statements to establish emilpo. context
-- v1.14 Updated Hash function to use standard_hash with MD5 to minimize collisions
-- v1.15 Updated primary and secondary MOS ordering in all three MPC sub queries
-- v1.16 Filtering out projected MOS or AOC using WHERE OCC_SPC_DSG_CD <> 'J'
-- v1.17 Removed requirement for CDAT.SLOT_UIC_CD = SAT.UIC_CD and including both UIC_CD and SLOT_UIC_CD
-- AUTHOR: MAJ Kyle Luoma (kyle.r.luoma.mil@mail.mil)
----
--COMMISSIONED OFFICERS (RLO):
SELECT STANDARD_HASH(TO_NUMBER(EMILPO_EXPORT.SSN) + &&MASK + 4294967295 + &&SEED_NUM, 'MD5') AS SSN_MASK_HASH, 
  CONCAT(CONCAT(EMILPO_EXPORT.UIC_CD, EMILPO_EXPORT.PARNO), EMILPO_EXPORT.LN) AS UIC_PAR_LN,
  EMILPO_EXPORT.*
FROM
  ( SELECT DISTINCT SAT.SSN,
    SAT.UIC_CD,
    CDAT.SLOT_UIC_CD,
    ORGT.PARENT_UIC_CD,
    URT.STRUC_CMD_CD,
    CDAT.AUDC_PARA_NR AS PARNO,
    CDAT.AUDC_LINE_NR AS LN,
    CDAT.MIL_POSN_RPT_NR,
    MAX(CDAT.DUTY_ASG_DT) AS DUTY_ASG_DT,
    SRT.RANK_AB,
    CDAT.ASG_RANK,
    CAT.MOS_AOC1,
    CAT.MOS_AOC2,
    CAT.MOS_AOC3,
    CAT.MOS_AOC4,
    CAT.MOS_AOC5,
    CAT.MOS_AOC6,
    CAT.MOS_AOC7,
    CAT.MOS_AOC8,
    CAT.MOS_AOC9,
    CAT.MOS_AOC10,
    CAT.MOS_AOC11,
    CAT.MOS_AOC12,
    CAT.MOS_AOC13,
    SQI.SQI1,
    SQI.SQI2,
    SQI.SQI3,
    SQI.SQI4,
    SQI.SQI5,
    SQI.SQI6,
    SQI.SQI7,
    SQI.SQI8,
    SQI.SQI9,
    SQI.SQI10,
    SQI.SQI11,
    SQI.SQI12,
    SQI.SQI13,
    SQI.SQI14,
    SQI.SQI15,
    SQI.SQI16,
    ASI.ASI1,
    ASI.ASI2,
    ASI.ASI3,
    ASI.ASI4,
    ASI.ASI5,
    ASI.ASI6,
    ASI.ASI7,
    ASI.ASI8,
    ASI.ASI9,
    ASI.ASI10,
    ASI.ASI11,
    ASI.ASI12,
    ASI.ASI13,
    ASI.ASI14
  FROM EMILPO.SOLDR_ASG_T SAT
  JOIN EMILPO.ORGANIZATION_T ORGT
  ON SAT.UIC_CD = ORGT.UIC_CD
  JOIN EMILPO.UNIT_RECORD_T URT
  ON SAT.UIC_CD = URT.UIC_CD
  JOIN
  (SELECT * FROM (SELECT DISTINCT SSN,
      MAX(DUTY_ASG_DT),
      SLOT_UIC_CD,
      AUDC_PARA_NR,
      AUDC_LINE_NR,
      MIL_POSN_RPT_NR,
      DUTY_ASG_DT,
      DENSE_RANK() OVER ( PARTITION BY SSN ORDER BY DUTY_ASG_DT DESC) AS ASG_RANK
    FROM EMILPO.CO_DUTY_ASG_T
    GROUP BY SSN,
      DUTY_ASG_DT,
      SLOT_UIC_CD,
      AUDC_PARA_NR,
      AUDC_LINE_NR,
      MIL_POSN_RPT_NR,
      DUTY_ASG_DT
    ORDER BY SSN,
      DUTY_ASG_DT DESC
    ) WHERE ASG_RANK = 1) CDAT
  ON SAT.SSN           = CDAT.SSN
  --AND CDAT.SLOT_UIC_CD = SAT.UIC_CD
  LEFT OUTER JOIN
    ( SELECT DISTINCT SSN,
      MAX(DECODE(MOS_AOC_RANK, 'MOS_AOC1', CO_AOC_CD))  AS MOS_AOC1,
      MAX(DECODE(MOS_AOC_RANK, 'MOS_AOC2', CO_AOC_CD))  AS MOS_AOC2,
      MAX(DECODE(MOS_AOC_RANK, 'MOS_AOC3', CO_AOC_CD))  AS MOS_AOC3,
      MAX(DECODE(MOS_AOC_RANK, 'MOS_AOC4', CO_AOC_CD))  AS MOS_AOC4,
      MAX(DECODE(MOS_AOC_RANK, 'MOS_AOC5', CO_AOC_CD))  AS MOS_AOC5,
      MAX(DECODE(MOS_AOC_RANK, 'MOS_AOC6', CO_AOC_CD))  AS MOS_AOC6,
      MAX(DECODE(MOS_AOC_RANK, 'MOS_AOC7', CO_AOC_CD))  AS MOS_AOC7,
      MAX(DECODE(MOS_AOC_RANK, 'MOS_AOC8', CO_AOC_CD))  AS MOS_AOC8,
      MAX(DECODE(MOS_AOC_RANK, 'MOS_AOC9', CO_AOC_CD))  AS MOS_AOC9,
      MAX(DECODE(MOS_AOC_RANK, 'MOS_AOC10', CO_AOC_CD)) AS MOS_AOC10,
      MAX(DECODE(MOS_AOC_RANK, 'MOS_AOC11', CO_AOC_CD)) AS MOS_AOC11,
      MAX(DECODE(MOS_AOC_RANK, 'MOS_AOC12', CO_AOC_CD)) AS MOS_AOC12,
      MAX(DECODE(MOS_AOC_RANK, 'MOS_AOC13', CO_AOC_CD)) AS MOS_AOC13
    FROM
      (SELECT SSN,
        CONCAT('MOS_AOC', DENSE_RANK() OVER ( PARTITION BY SSN ORDER BY OCC_SPC_AWD_DT DESC, DECODE(OCC_SPC_DSG_CD, 
        'P', 3, -- Primary specialty
        'G', 4, -- Career field 1 specialty
        'S', 1, -- Secondary specialty
        'H', 2, -- Career field 2 specialty
        'I', 5, -- Career field 3 specialty
        'D', 6, -- Duty specialty
        'A', 7, -- Additional specialty
        'J', 8, -- Projected specialty
        'V', 9)) -- Previous, no specified designator
        ) AS MOS_AOC_RANK,
        CO_AOC_CD,
        OCC_SPC_AWD_DT,
        OCC_SPC_DSG_CD
      FROM EMILPO.CO_AOC_T
      WHERE OCC_SPC_DSG_CD <> 'J'
      ORDER BY SSN,
        MOS_AOC_RANK
      )
    GROUP BY SSN
    ORDER BY SSN
    ) CAT
  ON SAT.SSN = CAT.SSN
  JOIN EMILPO.SOLDR_RANK_T SRT
  ON SAT.SSN = SRT.SSN
  JOIN EMILPO.PERSON_STATUS_T PST
  ON SAT.SSN = PST.SSN
  LEFT OUTER JOIN
    (SELECT '111111111' AS SSN,
      NULL              AS SQI1,
      NULL              AS SQI2,
      NULL              AS SQI3,
      NULL              AS SQI4,
      NULL              AS SQI5,
      NULL              AS SQI6,
      NULL              AS SQI7,
      NULL              AS SQI8,
      NULL              AS SQI9,
      NULL              AS SQI10,
      NULL              AS SQI11,
      NULL              AS SQI12,
      NULL              AS SQI13,
      NULL              AS SQI14,
      NULL              AS SQI15,
      NULL              AS SQI16
    FROM DUAL
    ) SQI
  ON SQI.SSN = SAT.SSN
  LEFT OUTER JOIN
    ( SELECT DISTINCT SSN,
      MAX(DECODE(ASI_RANK, 'ASI1', CO_SKILL_CD))  AS ASI1,
      MAX(DECODE(ASI_RANK, 'ASI2', CO_SKILL_CD))  AS ASI2,
      MAX(DECODE(ASI_RANK, 'ASI3', CO_SKILL_CD))  AS ASI3,
      MAX(DECODE(ASI_RANK, 'ASI4', CO_SKILL_CD))  AS ASI4,
      MAX(DECODE(ASI_RANK, 'ASI5', CO_SKILL_CD))  AS ASI5,
      MAX(DECODE(ASI_RANK, 'ASI6', CO_SKILL_CD))  AS ASI6,
      MAX(DECODE(ASI_RANK, 'ASI7', CO_SKILL_CD))  AS ASI7,
      MAX(DECODE(ASI_RANK, 'ASI8', CO_SKILL_CD))  AS ASI8,
      MAX(DECODE(ASI_RANK, 'ASI9', CO_SKILL_CD))  AS ASI9,
      MAX(DECODE(ASI_RANK, 'ASI10', CO_SKILL_CD)) AS ASI10,
      MAX(DECODE(ASI_RANK, 'ASI11', CO_SKILL_CD)) AS ASI11,
      MAX(DECODE(ASI_RANK, 'ASI12', CO_SKILL_CD)) AS ASI12,
      MAX(DECODE(ASI_RANK, 'ASI13', CO_SKILL_CD)) AS ASI13,
      MAX(DECODE(ASI_RANK, 'ASI14', CO_SKILL_CD)) AS ASI14
    FROM
      (SELECT SSN,
        CONCAT('ASI', DENSE_RANK() OVER (PARTITION BY SSN ORDER BY SKILL_AWD_DT DESC)) AS ASI_RANK,
        CO_SKILL_CD,
        SKILL_AWD_DT
      FROM EMILPO.CO_SKILL_T
      ORDER BY SSN,
        ASI_RANK
      )
    GROUP BY SSN
    ORDER BY SSN
    ) ASI
  ON ASI.SSN                  = SAT.SSN
  WHERE PST.TAPDB_REC_STAT_CD = 'G'
  AND SAT.CURR_ASG_IND        = 'Y'
  AND SRT.CURR_RANK_IND       = 'Y'
  AND SRT.RANK_AB            IN ('2LT', '1LT', 'CPT', 'MAJ', 'LTC', 'COL', 'BG', 'MG', 'LTG', 'GEN')
  AND CDAT.ASG_RANK           = '1'
  GROUP BY 
    SAT.UIC_CD,
    CDAT.SLOT_UIC_CD,
    ORGT.PARENT_UIC_CD,
    URT.STRUC_CMD_CD,
    CDAT.AUDC_PARA_NR,
    CDAT.AUDC_LINE_NR,
    CDAT.MIL_POSN_RPT_NR,
    CDAT.DUTY_ASG_DT,
    SAT.SSN,
    SRT.RANK_AB,
    CDAT.ASG_RANK,
    CAT.MOS_AOC1,
    CAT.MOS_AOC2,
    CAT.MOS_AOC3,
    CAT.MOS_AOC4,
    CAT.MOS_AOC5,
    CAT.MOS_AOC6,
    CAT.MOS_AOC7,
    CAT.MOS_AOC8,
    CAT.MOS_AOC9,
    CAT.MOS_AOC10,
    CAT.MOS_AOC11,
    CAT.MOS_AOC12,
    CAT.MOS_AOC13,
    SQI.SQI1,
    SQI.SQI2,
    SQI.SQI3,
    SQI.SQI4,
    SQI.SQI5,
    SQI.SQI6,
    SQI.SQI7,
    SQI.SQI8,
    SQI.SQI9,
    SQI.SQI10,
    SQI.SQI11,
    SQI.SQI12,
    SQI.SQI13,
    SQI.SQI14,
    SQI.SQI15,
    SQI.SQI16,
    ASI.ASI1,
    ASI.ASI2,
    ASI.ASI3,
    ASI.ASI4,
    ASI.ASI5,
    ASI.ASI6,
    ASI.ASI7,
    ASI.ASI8,
    ASI.ASI9,
    ASI.ASI10,
    ASI.ASI11,
    ASI.ASI12,
    ASI.ASI13,
    ASI.ASI14
  --ORDER BY SSN
  UNION
  --WARRANTS:
  SELECT DISTINCT SAT.SSN,
    SAT.UIC_CD,
    WDAT.SLOT_UIC_CD,
    ORGT.PARENT_UIC_CD,
    URT.STRUC_CMD_CD,
    WDAT.AUDC_PARA_NR AS PARNO,
    WDAT.AUDC_LINE_NR AS LN,
    WDAT.MIL_POSN_RPT_NR,
    MAX(WDAT.DUTY_ASG_DT) AS DUTY_ASG_DT,
    SRT.RANK_AB,
    WDAT.ASG_RANK,
    WMT.MOS_AOC1,
    WMT.MOS_AOC2,
    WMT.MOS_AOC3,
    WMT.MOS_AOC4,
    WMT.MOS_AOC5,
    WMT.MOS_AOC6,
    WMT.MOS_AOC7,
    WMT.MOS_AOC8,
    WMT.MOS_AOC9,
    WMT.MOS_AOC10,
    WMT.MOS_AOC11,
    WMT.MOS_AOC12,
    WMT.MOS_AOC13,
    SQI.SQI1,
    SQI.SQI2,
    SQI.SQI3,
    SQI.SQI4,
    SQI.SQI5,
    SQI.SQI6,
    SQI.SQI7,
    SQI.SQI8,
    SQI.SQI9,
    SQI.SQI10,
    SQI.SQI11,
    SQI.SQI12,
    SQI.SQI13,
    SQI.SQI14,
    SQI.SQI15,
    SQI.SQI16,
    ASI.ASI1,
    ASI.ASI2,
    ASI.ASI3,
    ASI.ASI4,
    ASI.ASI5,
    ASI.ASI6,
    ASI.ASI7,
    ASI.ASI8,
    ASI.ASI9,
    ASI.ASI10,
    ASI.ASI11,
    ASI.ASI12,
    ASI.ASI13,
    ASI.ASI14
  FROM EMILPO.SOLDR_ASG_T SAT
  JOIN EMILPO.ORGANIZATION_T ORGT
  ON SAT.UIC_CD = ORGT.UIC_CD
  JOIN EMILPO.UNIT_RECORD_T URT
  ON SAT.UIC_CD = URT.UIC_CD
  JOIN
    (SELECT * FROM (SELECT DISTINCT SSN,
      MAX(DUTY_ASG_DT),
      SLOT_UIC_CD,
      AUDC_PARA_NR,
      AUDC_LINE_NR,
      MIL_POSN_RPT_NR,
      DUTY_ASG_DT,
      DENSE_RANK() OVER ( PARTITION BY SSN ORDER BY DUTY_ASG_DT DESC) AS ASG_RANK
    FROM EMILPO.WO_DUTY_ASG_T
    GROUP BY SSN,
      DUTY_ASG_DT,
      SLOT_UIC_CD,
      AUDC_PARA_NR,
      AUDC_LINE_NR,
      MIL_POSN_RPT_NR,
      DUTY_ASG_DT
    ORDER BY SSN,
      DUTY_ASG_DT DESC
    ) WHERE ASG_RANK = 1) WDAT
  ON SAT.SSN           = WDAT.SSN
  --AND WDAT.SLOT_UIC_CD = SAT.UIC_CD
  LEFT OUTER JOIN
    ( SELECT DISTINCT SSN,
      MAX(DECODE(MOS_AOC_RANK, 'MOS_AOC1', WO_MOS_CD))  AS MOS_AOC1,
      MAX(DECODE(MOS_AOC_RANK, 'MOS_AOC2', WO_MOS_CD))  AS MOS_AOC2,
      MAX(DECODE(MOS_AOC_RANK, 'MOS_AOC3', WO_MOS_CD))  AS MOS_AOC3,
      MAX(DECODE(MOS_AOC_RANK, 'MOS_AOC4', WO_MOS_CD))  AS MOS_AOC4,
      MAX(DECODE(MOS_AOC_RANK, 'MOS_AOC5', WO_MOS_CD))  AS MOS_AOC5,
      MAX(DECODE(MOS_AOC_RANK, 'MOS_AOC6', WO_MOS_CD))  AS MOS_AOC6,
      MAX(DECODE(MOS_AOC_RANK, 'MOS_AOC7', WO_MOS_CD))  AS MOS_AOC7,
      MAX(DECODE(MOS_AOC_RANK, 'MOS_AOC8', WO_MOS_CD))  AS MOS_AOC8,
      MAX(DECODE(MOS_AOC_RANK, 'MOS_AOC9', WO_MOS_CD))  AS MOS_AOC9,
      MAX(DECODE(MOS_AOC_RANK, 'MOS_AOC10', WO_MOS_CD)) AS MOS_AOC10,
      MAX(DECODE(MOS_AOC_RANK, 'MOS_AOC11', WO_MOS_CD)) AS MOS_AOC11,
      MAX(DECODE(MOS_AOC_RANK, 'MOS_AOC12', WO_MOS_CD)) AS MOS_AOC12,
      MAX(DECODE(MOS_AOC_RANK, 'MOS_AOC13', WO_MOS_CD)) AS MOS_AOC13
    FROM
      (SELECT SSN,
        CONCAT('MOS_AOC', DENSE_RANK() OVER ( PARTITION BY SSN ORDER BY OCC_SPC_AWD_DT DESC, DECODE(OCC_SPC_DSG_CD, 
        'P', 3, -- Primary specialty
        'G', 4, -- Career field 1 specialty
        'S', 1, -- Secondary specialty
        'H', 2, -- Career field 2 specialty
        'I', 5, -- Career field 3 specialty
        'D', 6, -- Duty specialty
        'A', 7, -- Additional specialty
        'J', 8, -- Projected specialty
        'V', 9)) -- Previous, no specified designator
        ) AS MOS_AOC_RANK,
        WO_MOS_CD,
        OCC_SPC_AWD_DT,
        OCC_SPC_DSG_CD
      FROM EMILPO.WO_MOS_T
      WHERE OCC_SPC_DSG_CD <> 'J'
      ORDER BY SSN,
        MOS_AOC_RANK
      )
    GROUP BY SSN
    ORDER BY SSN
    ) WMT
  ON SAT.SSN = WMT.SSN
  JOIN EMILPO.SOLDR_RANK_T SRT
  ON SAT.SSN = SRT.SSN
  JOIN EMILPO.PERSON_STATUS_T PST
  ON SAT.SSN = PST.SSN
  LEFT OUTER JOIN
    ( SELECT DISTINCT SSN,
      MAX(DECODE(SQI_RANK, 'SQI1', WO_SQI_CD))  AS SQI1,
      MAX(DECODE(SQI_RANK, 'SQI2', WO_SQI_CD))  AS SQI2,
      MAX(DECODE(SQI_RANK, 'SQI3', WO_SQI_CD))  AS SQI3,
      MAX(DECODE(SQI_RANK, 'SQI4', WO_SQI_CD))  AS SQI4,
      MAX(DECODE(SQI_RANK, 'SQI5', WO_SQI_CD))  AS SQI5,
      MAX(DECODE(SQI_RANK, 'SQI6', WO_SQI_CD))  AS SQI6,
      MAX(DECODE(SQI_RANK, 'SQI7', WO_SQI_CD))  AS SQI7,
      MAX(DECODE(SQI_RANK, 'SQI8', WO_SQI_CD))  AS SQI8,
      MAX(DECODE(SQI_RANK, 'SQI9', WO_SQI_CD))  AS SQI9,
      MAX(DECODE(SQI_RANK, 'SQI10', WO_SQI_CD)) AS SQI10,
      MAX(DECODE(SQI_RANK, 'SQI11', WO_SQI_CD)) AS SQI11,
      MAX(DECODE(SQI_RANK, 'SQI12', WO_SQI_CD)) AS SQI12,
      MAX(DECODE(SQI_RANK, 'SQI13', WO_SQI_CD)) AS SQI13,
      MAX(DECODE(SQI_RANK, 'SQI14', WO_SQI_CD)) AS SQI14,
      MAX(DECODE(SQI_RANK, 'SQI15', WO_SQI_CD)) AS SQI15,
      MAX(DECODE(SQI_RANK, 'SQI16', WO_SQI_CD)) AS SQI16
    FROM
      (SELECT SSN,
        CONCAT('SQI', DENSE_RANK() OVER (PARTITION BY SSN ORDER BY SKILL_AWD_DT DESC)) AS SQI_RANK,
        WO_SQI_CD,
        SKILL_AWD_DT
      FROM EMILPO.WO_SQI_T
      ORDER BY SSN,
        SQI_RANK
      )
    GROUP BY SSN
    ORDER BY SSN
    ) SQI
  ON SQI.SSN = SAT.SSN
  LEFT OUTER JOIN
    ( SELECT DISTINCT SSN,
      MAX(DECODE(ASI_RANK, 'ASI1', WO_ASI_CD))  AS ASI1,
      MAX(DECODE(ASI_RANK, 'ASI2', WO_ASI_CD))  AS ASI2,
      MAX(DECODE(ASI_RANK, 'ASI3', WO_ASI_CD))  AS ASI3,
      MAX(DECODE(ASI_RANK, 'ASI4', WO_ASI_CD))  AS ASI4,
      MAX(DECODE(ASI_RANK, 'ASI5', WO_ASI_CD))  AS ASI5,
      MAX(DECODE(ASI_RANK, 'ASI6', WO_ASI_CD))  AS ASI6,
      MAX(DECODE(ASI_RANK, 'ASI7', WO_ASI_CD))  AS ASI7,
      MAX(DECODE(ASI_RANK, 'ASI8', WO_ASI_CD))  AS ASI8,
      MAX(DECODE(ASI_RANK, 'ASI9', WO_ASI_CD))  AS ASI9,
      MAX(DECODE(ASI_RANK, 'ASI10', WO_ASI_CD)) AS ASI10,
      MAX(DECODE(ASI_RANK, 'ASI11', WO_ASI_CD)) AS ASI11,
      MAX(DECODE(ASI_RANK, 'ASI12', WO_ASI_CD)) AS ASI12,
      MAX(DECODE(ASI_RANK, 'ASI13', WO_ASI_CD)) AS ASI13,
      MAX(DECODE(ASI_RANK, 'ASI14', WO_ASI_CD)) AS ASI14
    FROM
      (SELECT SSN,
        CONCAT('ASI', DENSE_RANK() OVER (PARTITION BY SSN ORDER BY SKILL_AWD_DT DESC)) AS ASI_RANK,
        WO_ASI_CD,
        SKILL_AWD_DT
      FROM EMILPO.WO_ASI_T
      ORDER BY SSN,
        ASI_RANK
      )
    GROUP BY SSN
    ORDER BY SSN
    ) ASI
  ON ASI.SSN                  = SAT.SSN
  WHERE PST.TAPDB_REC_STAT_CD = 'G'
  AND SAT.CURR_ASG_IND        = 'Y'
  AND SRT.CURR_RANK_IND       = 'Y'
  AND SRT.RANK_AB            IN ('WO1', 'CW2', 'CW3', 'CW4', 'CW5')
  AND WDAT.ASG_RANK           = '1'
  GROUP BY 
    SAT.UIC_CD,
    WDAT.SLOT_UIC_CD,
    ORGT.PARENT_UIC_CD,
    URT.STRUC_CMD_CD,
    WDAT.AUDC_PARA_NR,
    WDAT.AUDC_LINE_NR,
    WDAT.MIL_POSN_RPT_NR,
    WDAT.DUTY_ASG_DT,
    SAT.SSN,
    SRT.RANK_AB,
    WDAT.ASG_RANK,
    WMT.MOS_AOC1,
    WMT.MOS_AOC2,
    WMT.MOS_AOC3,
    WMT.MOS_AOC4,
    WMT.MOS_AOC5,
    WMT.MOS_AOC6,
    WMT.MOS_AOC7,
    WMT.MOS_AOC8,
    WMT.MOS_AOC9,
    WMT.MOS_AOC10,
    WMT.MOS_AOC11,
    WMT.MOS_AOC12,
    WMT.MOS_AOC13,
    SQI.SQI1,
    SQI.SQI2,
    SQI.SQI3,
    SQI.SQI4,
    SQI.SQI5,
    SQI.SQI6,
    SQI.SQI7,
    SQI.SQI8,
    SQI.SQI9,
    SQI.SQI10,
    SQI.SQI11,
    SQI.SQI12,
    SQI.SQI13,
    SQI.SQI14,
    SQI.SQI15,
    SQI.SQI16,
    ASI.ASI1,
    ASI.ASI2,
    ASI.ASI3,
    ASI.ASI4,
    ASI.ASI5,
    ASI.ASI6,
    ASI.ASI7,
    ASI.ASI8,
    ASI.ASI9,
    ASI.ASI10,
    ASI.ASI11,
    ASI.ASI12,
    ASI.ASI13,
    ASI.ASI14
  --ORDER BY SSN
  UNION
  --ENLISTED:
  SELECT DISTINCT SAT.SSN,
    SAT.UIC_CD,
    EDAT.SLOT_UIC_CD,
    ORGT.PARENT_UIC_CD,
    URT.STRUC_CMD_CD,
    EDAT.AUDC_PARA_NR AS PARNO,
    EDAT.AUDC_LINE_NR AS LN,
    EDAT.MIL_POSN_RPT_NR,
    MAX(EDAT.DUTY_ASG_DT) AS DUTY_ASG_DT,
    SRT.RANK_AB,
    EDAT.ASG_RANK,
    EMT.MOS_AOC1,
    EMT.MOS_AOC2,
    EMT.MOS_AOC3,
    EMT.MOS_AOC4,
    EMT.MOS_AOC5,
    EMT.MOS_AOC6,
    EMT.MOS_AOC7,
    EMT.MOS_AOC8,
    EMT.MOS_AOC9,
    EMT.MOS_AOC10,
    EMT.MOS_AOC11,
    EMT.MOS_AOC12,
    EMT.MOS_AOC13,
    SQI.SQI1,
    SQI.SQI2,
    SQI.SQI3,
    SQI.SQI4,
    SQI.SQI5,
    SQI.SQI6,
    SQI.SQI7,
    SQI.SQI8,
    SQI.SQI9,
    SQI.SQI10,
    SQI.SQI11,
    SQI.SQI12,
    SQI.SQI13,
    SQI.SQI14,
    SQI.SQI15,
    SQI.SQI16,
    ASI.ASI1,
    ASI.ASI2,
    ASI.ASI3,
    ASI.ASI4,
    ASI.ASI5,
    ASI.ASI6,
    ASI.ASI7,
    ASI.ASI8,
    ASI.ASI9,
    ASI.ASI10,
    ASI.ASI11,
    ASI.ASI12,
    ASI.ASI13,
    ASI.ASI14
  FROM EMILPO.SOLDR_ASG_T SAT
  JOIN EMILPO.ORGANIZATION_T ORGT
  ON SAT.UIC_CD = ORGT.UIC_CD
  JOIN EMILPO.UNIT_RECORD_T URT
  ON SAT.UIC_CD = URT.UIC_CD
  JOIN
  (SELECT * FROM (SELECT DISTINCT SSN,
      MAX(DUTY_ASG_DT),
      SLOT_UIC_CD,
      AUDC_PARA_NR,
      AUDC_LINE_NR,
      MIL_POSN_RPT_NR,
      DUTY_ASG_DT,
      DENSE_RANK() OVER ( PARTITION BY SSN ORDER BY DUTY_ASG_DT DESC) AS ASG_RANK
    FROM EMILPO.ENL_DUTY_ASG_T
    GROUP BY SSN,
      DUTY_ASG_DT,
      SLOT_UIC_CD,
      AUDC_PARA_NR,
      AUDC_LINE_NR,
      MIL_POSN_RPT_NR,
      DUTY_ASG_DT
    ORDER BY SSN,
      DUTY_ASG_DT DESC
    ) WHERE ASG_RANK = 1) EDAT
  ON SAT.SSN           = EDAT.SSN
  --AND EDAT.SLOT_UIC_CD = SAT.UIC_CD
  LEFT OUTER JOIN
    ( SELECT DISTINCT SSN,
      MAX(DECODE(MOS_AOC_RANK, 'MOS_AOC1', ENL_MOS_CD))  AS MOS_AOC1,
      MAX(DECODE(MOS_AOC_RANK, 'MOS_AOC2', ENL_MOS_CD))  AS MOS_AOC2,
      MAX(DECODE(MOS_AOC_RANK, 'MOS_AOC3', ENL_MOS_CD))  AS MOS_AOC3,
      MAX(DECODE(MOS_AOC_RANK, 'MOS_AOC4', ENL_MOS_CD))  AS MOS_AOC4,
      MAX(DECODE(MOS_AOC_RANK, 'MOS_AOC5', ENL_MOS_CD))  AS MOS_AOC5,
      MAX(DECODE(MOS_AOC_RANK, 'MOS_AOC6', ENL_MOS_CD))  AS MOS_AOC6,
      MAX(DECODE(MOS_AOC_RANK, 'MOS_AOC7', ENL_MOS_CD))  AS MOS_AOC7,
      MAX(DECODE(MOS_AOC_RANK, 'MOS_AOC8', ENL_MOS_CD))  AS MOS_AOC8,
      MAX(DECODE(MOS_AOC_RANK, 'MOS_AOC9', ENL_MOS_CD))  AS MOS_AOC9,
      MAX(DECODE(MOS_AOC_RANK, 'MOS_AOC10', ENL_MOS_CD)) AS MOS_AOC10,
      MAX(DECODE(MOS_AOC_RANK, 'MOS_AOC11', ENL_MOS_CD)) AS MOS_AOC11,
      MAX(DECODE(MOS_AOC_RANK, 'MOS_AOC12', ENL_MOS_CD)) AS MOS_AOC12,
      MAX(DECODE(MOS_AOC_RANK, 'MOS_AOC13', ENL_MOS_CD)) AS MOS_AOC13
    FROM
      (SELECT SSN,
        CONCAT('MOS_AOC', DENSE_RANK() OVER ( PARTITION BY SSN ORDER BY OCC_SPC_AWD_DT DESC, DECODE(OCC_SPC_DSG_CD, 
        'P', 3, -- Primary specialty
        'G', 4, -- Career field 1 specialty
        'S', 1, -- Secondary specialty
        'H', 2, -- Career field 2 specialty
        'I', 5, -- Career field 3 specialty
        'D', 6, -- Duty specialty
        'A', 7, -- Additional specialty
        'J', 8, -- Projected specialty
        'V', 9)) -- Previous, no specified designator
        ) AS MOS_AOC_RANK,
        ENL_MOS_CD,
        OCC_SPC_AWD_DT,
        OCC_SPC_DSG_CD
      FROM EMILPO.ENL_SOLDR_MOS_T
      WHERE OCC_SPC_DSG_CD <> 'J'
      ORDER BY SSN,
        MOS_AOC_RANK
      )
    GROUP BY SSN
    ORDER BY SSN
    ) EMT
  ON SAT.SSN = EMT.SSN
  JOIN EMILPO.SOLDR_RANK_T SRT
  ON SAT.SSN = SRT.SSN
  JOIN EMILPO.PERSON_STATUS_T PST
  ON SAT.SSN = PST.SSN
  LEFT OUTER JOIN
    ( SELECT DISTINCT SSN,
      MAX(DECODE(SQI_RANK, 'SQI1', ENL_SQI_CD))  AS SQI1,
      MAX(DECODE(SQI_RANK, 'SQI2', ENL_SQI_CD))  AS SQI2,
      MAX(DECODE(SQI_RANK, 'SQI3', ENL_SQI_CD))  AS SQI3,
      MAX(DECODE(SQI_RANK, 'SQI4', ENL_SQI_CD))  AS SQI4,
      MAX(DECODE(SQI_RANK, 'SQI5', ENL_SQI_CD))  AS SQI5,
      MAX(DECODE(SQI_RANK, 'SQI6', ENL_SQI_CD))  AS SQI6,
      MAX(DECODE(SQI_RANK, 'SQI7', ENL_SQI_CD))  AS SQI7,
      MAX(DECODE(SQI_RANK, 'SQI8', ENL_SQI_CD))  AS SQI8,
      MAX(DECODE(SQI_RANK, 'SQI9', ENL_SQI_CD))  AS SQI9,
      MAX(DECODE(SQI_RANK, 'SQI10', ENL_SQI_CD)) AS SQI10,
      MAX(DECODE(SQI_RANK, 'SQI11', ENL_SQI_CD)) AS SQI11,
      MAX(DECODE(SQI_RANK, 'SQI12', ENL_SQI_CD)) AS SQI12,
      MAX(DECODE(SQI_RANK, 'SQI13', ENL_SQI_CD)) AS SQI13,
      MAX(DECODE(SQI_RANK, 'SQI14', ENL_SQI_CD)) AS SQI14,
      MAX(DECODE(SQI_RANK, 'SQI15', ENL_SQI_CD)) AS SQI15,
      MAX(DECODE(SQI_RANK, 'SQI16', ENL_SQI_CD)) AS SQI16
    FROM
      (SELECT SSN,
        CONCAT('SQI', DENSE_RANK() OVER (PARTITION BY SSN ORDER BY SKILL_AWD_DT DESC)) AS SQI_RANK,
        ENL_SQI_CD,
        SKILL_AWD_DT
      FROM EMILPO.ENL_SOLDR_SQI_T
      ORDER BY SSN,
        SQI_RANK
      )
    GROUP BY SSN
    ORDER BY SSN
    ) SQI
  ON SQI.SSN = SAT.SSN
  LEFT OUTER JOIN
    ( SELECT DISTINCT SSN,
      MAX(DECODE(ASI_RANK, 'ASI1', ENL_ASI_CD))  AS ASI1,
      MAX(DECODE(ASI_RANK, 'ASI2', ENL_ASI_CD))  AS ASI2,
      MAX(DECODE(ASI_RANK, 'ASI3', ENL_ASI_CD))  AS ASI3,
      MAX(DECODE(ASI_RANK, 'ASI4', ENL_ASI_CD))  AS ASI4,
      MAX(DECODE(ASI_RANK, 'ASI5', ENL_ASI_CD))  AS ASI5,
      MAX(DECODE(ASI_RANK, 'ASI6', ENL_ASI_CD))  AS ASI6,
      MAX(DECODE(ASI_RANK, 'ASI7', ENL_ASI_CD))  AS ASI7,
      MAX(DECODE(ASI_RANK, 'ASI8', ENL_ASI_CD))  AS ASI8,
      MAX(DECODE(ASI_RANK, 'ASI9', ENL_ASI_CD))  AS ASI9,
      MAX(DECODE(ASI_RANK, 'ASI10', ENL_ASI_CD)) AS ASI10,
      MAX(DECODE(ASI_RANK, 'ASI11', ENL_ASI_CD)) AS ASI11,
      MAX(DECODE(ASI_RANK, 'ASI12', ENL_ASI_CD)) AS ASI12,
      MAX(DECODE(ASI_RANK, 'ASI13', ENL_ASI_CD)) AS ASI13,
      MAX(DECODE(ASI_RANK, 'ASI14', ENL_ASI_CD)) AS ASI14
    FROM
      (SELECT SSN,
        CONCAT('ASI', DENSE_RANK() OVER (PARTITION BY SSN ORDER BY SKILL_AWD_DT DESC)) AS ASI_RANK,
        ENL_ASI_CD,
        SKILL_AWD_DT
      FROM EMILPO.ENL_SOLDR_ASI_T
      ORDER BY SSN,
        ASI_RANK
      )
    GROUP BY SSN
    ORDER BY SSN
    ) ASI
  ON ASI.SSN                  = SAT.SSN
  WHERE PST.TAPDB_REC_STAT_CD = 'G'
  AND SAT.CURR_ASG_IND        = 'Y'
  AND SRT.CURR_RANK_IND       = 'Y'
  AND SRT.RANK_AB            IN ('PV1', 'PV2', 'PFC', 'SPC', 'CPL', 'SGT', 'SSG', 'SFC', 'MSG', '1SG', 'SGM', 'CSM')
  AND EDAT.ASG_RANK           = '1'
  GROUP BY 
    SAT.UIC_CD,
    EDAT.SLOT_UIC_CD,
    ORGT.PARENT_UIC_CD,
    URT.STRUC_CMD_CD,
    EDAT.AUDC_PARA_NR,
    EDAT.AUDC_LINE_NR,
    EDAT.MIL_POSN_RPT_NR,
    EDAT.DUTY_ASG_DT,
    SAT.SSN,
    SRT.RANK_AB,
    EDAT.ASG_RANK,
    EMT.MOS_AOC1,
    EMT.MOS_AOC2,
    EMT.MOS_AOC3,
    EMT.MOS_AOC4,
    EMT.MOS_AOC5,
    EMT.MOS_AOC6,
    EMT.MOS_AOC7,
    EMT.MOS_AOC8,
    EMT.MOS_AOC9,
    EMT.MOS_AOC10,
    EMT.MOS_AOC11,
    EMT.MOS_AOC12,
    EMT.MOS_AOC13,
    SQI.SQI1,
    SQI.SQI2,
    SQI.SQI3,
    SQI.SQI4,
    SQI.SQI5,
    SQI.SQI6,
    SQI.SQI7,
    SQI.SQI8,
    SQI.SQI9,
    SQI.SQI10,
    SQI.SQI11,
    SQI.SQI12,
    SQI.SQI13,
    SQI.SQI14,
    SQI.SQI15,
    SQI.SQI16,
    ASI.ASI1,
    ASI.ASI2,
    ASI.ASI3,
    ASI.ASI4,
    ASI.ASI5,
    ASI.ASI6,
    ASI.ASI7,
    ASI.ASI8,
    ASI.ASI9,
    ASI.ASI10,
    ASI.ASI11,
    ASI.ASI12,
    ASI.ASI13,
    ASI.ASI14
  ORDER BY SSN
  ) EMILPO_EXPORT;
