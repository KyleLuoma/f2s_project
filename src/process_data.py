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
    return emilpo_assignments
