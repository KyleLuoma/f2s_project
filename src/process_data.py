# -*- coding: utf-8 -*-
"""
Created on Mon Feb 9 14:22:39 2020
@author: LuomaKR
Module to process data files loaded for F2S matching
"""

import pandas as pd

"""Process the AOS spaces billet export Pandas DF(s)"""
def process_aos_billet_export(aos_billet_export):
    aos_billet_export["stage_matched"] = 0
    aos_billet_export.rename(columns = {
                        "PARENT_UIC" : "UIC",
                        "PARENT_PARNO" : "PARNO",
                        "PERLN" : "LN"
            })
    return aos_billet_export

"""Process the EMILPO faces assignment file Pandas DF"""
def process_emilpo_assignments(emilpo_assignments, rank_grade_xwalk):
    print("Mapping grade to rank in the eMILPO assignments file")
    emilpo_assignments["GRADE"] = emilpo_assignments.apply(
            lambda row: rank_grade_xwalk.loc[row.RANK_AB].GRADE, axis = 1
            )
    
    print("Consolidating ASIs into ASI_LIST column in the eMILPO assignments file")
    emilpo_assignments["ASI_LIST"] = emilpo_assignments.apply(
            lambda row: [
                    row.ASI1,
                    row.ASI2,
                    row.ASI3,
                    row.ASI4,
                    row.ASI5,
                    row.ASI6,
                    row.ASI7,
                    row.ASI8,
                    row.ASI9,
                    row.ASI10,
                    row.ASI11,
                    row.ASI12,
                    row.ASI13,
                    row.ASI14
                    ],
                    axis = 1
            )
    
    print("Consolidating SQIs into SQI_LIST column in the eMILPO assignments file")
    emilpo_assignments["SQI_LIST"] = emilpo_assignments.apply(
            lambda row: [
                    row.SQI1,
                    row.SQI2,
                    row.SQI3,
                    row.SQI4,
                    row.SQI5,
                    row.SQI6,
                    row.SQI7,
                    row.SQI8,
                    row.SQI9,
                    row.SQI10,
                    row.SQI11,
                    row.SQI12,
                    row.SQI13,
                    row.SQI14,
                    row.SQI15,
                    row.SQI16
                    ],
                    axis = 1
            )
    
    print("Consolidating MOS/AOC into MOS_AOC_LIST column in the eMILPO assignments file")
    emilpo_assignments["MOS_AOC_LIST"] = emilpo_assignments.apply(
            lambda row: [
                    row.MOS_AOC1,
                    row.MOS_AOC2,
                    row.MOS_AOC3,
                    row.MOS_AOC4,
                    row.MOS_AOC5,
                    row.MOS_AOC6,
                    row.MOS_AOC7,
                    row.MOS_AOC8,
                    row.MOS_AOC9,
                    row.MOS_AOC10,
                    row.MOS_AOC11,
                    row.MOS_AOC12,
                    row.MOS_AOC13
                    ],
                    axis = 1
            )
    
    return emilpo_assignments
