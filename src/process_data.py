# -*- coding: utf-8 -*-
"""
Created on Mon Feb 9 14:22:39 2020
@author: LuomaKR
Module to process data files loaded for F2S matching
"""

import pandas as pd
from pandas.api.types import CategoricalDtype

"""Match phase headers
" STAGE,UIC,PARNO,LN,GRADE,PRI_MOS,ALT_MOS,SQI,ASI,
" GRADE_VAR_UP,GRADE_VAR_DN,TEMPLET
"""

MIL_GRADES = ["E1", "E2", "E3", "E4", "E5", "E6", "E7", "E8", "E9", 
              "W1", "W2", "W3", "W4", "W5",
              "O1", "O2", "O3", "O4", "O5", "O6", "O7", "O8", "O9", "O10"]

CIV_GRADES = ["00", "01", "02", "03", "04", "05", "06", "07", "08",
              "09", "10", "11", "12", "13", "14", "15"]

NON_ADD_RMKS = ['49','83','85','87','88','90','91','89','92']

TEMPLET_PARNOS = ["999", "9GO", "9AD", "9RF","9ER", 
                  "9TM", "9AI", "9PF", "9SA", "9ST"]


""" uic_cod_update should include UIC and CODE series"""
def convert_cmd_code_for_uic_in_faces(
    faces_target, uic_code_file, 
    uic_col_name = "UICOD", 
    cmd_col_name = "MACOM",
    new_cmd_cd = "AF"
):
    print("Converting command codes in faces file")
    uic_cmd_codes = uic_code_file[[uic_col_name, cmd_col_name]].set_index(uic_col_name) 
    ssn_mask_updates = faces_target.where(
        faces_target.UIC.isin(uic_cmd_codes.index)
    ).dropna(how = "all")[[
        "SSN_MASK", "STRUC_CMD_CD"
    ]].set_index("SSN_MASK")
    ssn_mask_updates = ssn_mask_updates.where(
        ssn_mask_updates.STRUC_CMD_CD != "AR"
    ).dropna(how = "all").copy()
    ssn_mask_updates.STRUC_CMD_CD = new_cmd_cd
    faces_target.set_index("SSN_MASK", inplace = True)
    faces_target.update(ssn_mask_updates)
    faces_target.reset_index(inplace = True)
    return faces_target

def calculate_age(target, today, time_column, age_column_prefix = ""):
    print("Calculating target column age with prefix", age_column_prefix)
    target_column = age_column_prefix + "_AGE"
    target[target_column] = 0
    target[target_column] = target.apply(
        lambda row: (today - row[time_column]).days, 
        axis = 1
    )
    return target

def add_match_phase_description(target, match_phases):
    print("Joining match phase description to target dataframe")
    match_phases = match_phases.reset_index().set_index("STAGE")
    target.stage_matched = target.stage_matched.fillna(0).astype("int64")
    target = target.join(match_phases["DESCRIPTION"], on = "stage_matched").rename(
                columns = {"DESCRIPTION" : "MATCH_DESCRIPTION"}
            )
    return target

def add_command_title(target, cmd_description_xwalk, cmd_col_name = "STRUC_CMD_CD"):
    print("Mapping command titles to target data frame")
    target = target.join(
        cmd_description_xwalk,
        on = cmd_col_name
    )
    return target

def add_drrsa_data(target, drrsa):
    drrsa = drrsa.reset_index().set_index("UIC")
    print("Mapping DRRSA data to target data frame")    
    column_check_list = [
        "DRRSA_ADCON", "DRRSA_HOGEO", "DRRSA_ARLOC", "DRRSA_GEOLOCATIONNAME",
        "DRRSA_ASGMT", "DRRSA_UNPRSNTLOCZIP", "DRRSA_PPA"
    ]
    for column in column_check_list:
        if column in target.columns:
            target = target.drop(column, axis = 1)
    target = target.join(
        drrsa[["ADCON", "HOGEO", "ARLOC", "GEOLOCATIONNAME", "ASGMT", "PPA", "UNPRSNTLOCZIP"]],
        on = "UIC",
        lsuffix = "_AOS",
        rsuffix = "_DRRSA"
    ).rename(columns = {
        "ADCON" : "DRRSA_ADCON",
        "HOGEO" : "DRRSA_HOGEO",
        "ARLOC" : "DRRSA_ARLOC",
        "GEOLOCATIONNAME" : "DRRSA_GEOLOCATIONNAME",
        "ASGMT" : "DRRSA_ASGMT",
        "UNPRSNTLOCZIP" : "DRRSA_UNPRSNTLOCZIP",
        "PPA_DRRSA" : "DRRSA_PPA"
        }
    )
    target.DRRSA_UNPRSNTLOCZIP = target.apply(
        lambda row: str(row.DRRSA_UNPRSNTLOCZIP)[0:5],
        axis = 1      
    )
    return target

def check_uic_in_aos(target, uic_ouid_xwalk, uic_index_title):
    print("Checking if", uic_index_title, "exists in AOS")
    if(uic_ouid_xwalk.index.name != "UIC"): uic_ouid_xwalk.set_index("UIC", inplace = True)
    target[(uic_index_title + "_IN_AOS")] = target[uic_index_title].isin(uic_ouid_xwalk.index)
    return target

def add_expected_hsduic(target, UIC_HD_map, NA_value = "NA"):
    print("Adding expected HSDUIC to target file")
    UIC_primary_code_list = UIC_HD_map.UIC.to_list()
    UIC_HD_map = UIC_HD_map.set_index("UIC")
    target["HSDUIC"] = target.apply(
            lambda row: ((row.UIC[0:4] + UIC_HD_map.HDUIC.loc[row.UIC[4:6]]) 
                            if (row.UIC[4:6] in UIC_primary_code_list) else "NA"),
            axis = 1
            )
    UIC_HD_map.reset_index()
    return target

def add_templet_columns(target, parno = "999E", ln = "99"):
    print("Adding templet PARA and LN to target data frame")
    target["TMP_PARNO"] = parno
    target["TMP_LN"] = ln
    return target

def add_is_templet_column(spaces):
    spaces["IS_TEMPLET"] = spaces.apply(
        lambda row: row.PARNO[0:3] in TEMPLET_PARNOS,
        axis = 1
    )
    return spaces        

"""Converts faces columns to categorical values for indexing"""
def categorical_faces(faces):
        print(" - UIC: creating categorical UIC index in faces file")
        faces["UIC"] = faces["UIC"].astype(CategoricalDtype(faces.UIC.unique()))
        
        print(" - PARENT_UIC_CD: creating categorical PARENT_UIC_CD index in faces file")
        faces["PARENT_UIC_CD"] = faces["PARENT_UIC_CD"].astype(CategoricalDtype(faces.UIC.unique()))
            
        print(" - PARNO: creating categorical PARNO index in faces file")
        faces["PARNO"] = faces.PARNO.fillna("NONE")
        faces["PARNO"] = faces["PARNO"].astype(CategoricalDtype(faces.PARNO.unique()))
        
        print(" - LN: creating categorical LN index in faces file")
        faces["LN"] = faces.LN.fillna("NONE")
        faces["LN"] = faces["LN"].astype(CategoricalDtype(faces.LN.unique()))
        
        print(" - GRADE: creating categorical GRADE index in faces file")
        faces["GRADE"] = faces["GRADE"].astype(CategoricalDtype(faces.GRADE.unique()))
        
        print(" - Primary MOS_AOC: creating categorical Primary MOS_AOC index in faces file")
        faces["MOS_AOC1"] = faces.MOS_AOC1.fillna("NONE")
        faces["MOS_AOC1"] = faces["MOS_AOC1"].astype(CategoricalDtype(faces.MOS_AOC1.unique()))
        
        return faces
        
"""Converts spaces columns to categorical values for indexing"""
def categorical_spaces(spaces):
        print(" - UIC: creating categorical UIC index in spaces file")
        spaces["UIC"] = spaces["UIC"].astype(CategoricalDtype(spaces.UIC.unique()))
        
        print(" - PARNO: creating categorical PARNO index in spaces file")
        spaces["PARNO"] = spaces.PARNO.fillna("NOPAR")
        spaces["PARNO"] = spaces["PARNO"].astype(CategoricalDtype(spaces.PARNO.unique()))

        print(" - LN: creating categorical LN index in spaces file")
        spaces["LN"] = spaces.LN.fillna("NOLN")
        spaces["LN"] = spaces["LN"].astype(CategoricalDtype(spaces.LN.unique()))

        print(" - GRADE: creating categorical GRADE index in spaces file")
        spaces["GRADE"] = spaces.GRADE.fillna("NOGR")
        spaces["GRADE"] = spaces["GRADE"].astype(CategoricalDtype(spaces.GRADE.unique()))

        print(" - Primary MOS_AOC: creating categorical POSCO index in spaces file")
        spaces["POSCO"] = spaces.POSCO.fillna("NOGR")
        spaces["POSCO"] = spaces["POSCO"].astype(CategoricalDtype(spaces.POSCO.unique()))
        
        print(" - SQI: creating categorical SQI index in spaces file")
        spaces["SQI1"] = spaces.SQI1.fillna("NOSQ")
        spaces["SQI1"] = spaces["SQI1"].astype(CategoricalDtype(spaces.SQI1.unique()))

        return spaces        


"""Process the AOS spaces billet export Pandas DF(s)
" AOS headers: PARENT_UIC,PARENT_PARNO,PARENT_TITLE,FMID,PERLN,GRADE,POSCO,TITLE,
" ASI1,ASI2,ASI3,ASI4,SQI1,RMK1,RMK2,RMK3,RMK4,AMSCO,MDEP,BRANCH,IDENT,PPSST,PSIRQ,
" PPSRQ,LDUIC,MDUIC,LICCO,LPIND,CTYPE,CIVCC,SUPV,MMC,CAFC,FSC,REPORTS_TO_FMID,
" REPORTS_TO_TITLE,REQ,AUTH,AGR,S_DATE,T_DATE
"""
def process_aos_billet_export(aos_billet_export):
    print("Processing AOS billet export file")
    print("  - Dropping non military grade positions")
    aos_billet_export = aos_billet_export.where(~aos_billet_export.GRADE.isin(CIV_GRADES)).dropna(how = "all")
    print("  - Renaming columns")
    aos_billet_export["stage_matched"] = 0
    aos_billet_export["SSN_MASK"] = 0
    
    aos_billet_export.GRADE.fillna("TMP")
    aos_billet_export.GRADE = aos_billet_export.GRADE.astype("str")
    aos_billet_export.POSCO.fillna("TMP")   
    aos_billet_export.POSCO = aos_billet_export.POSCO.astype("str")
    
    print("  - Truncating POSCO to match eMILPO MOS_AOC field")
    aos_billet_export["POSCO"] = aos_billet_export.apply(
        lambda row: row.POSCO[0:4] if row.GRADE[0] == "W" else row.POSCO[0:3],
        axis = 1
    )
    
    aos_billet_export = aos_billet_export.rename(
        columns = {
            "PARENT_UIC" : "UIC",
            "PARENT_PARNO" : "PARNO",
            "PERLN" : "LN"
        }
    )
    
    aos_billet_export.PARNO = aos_billet_export.PARNO.astype("str")
    aos_billet_export.LN = aos_billet_export.LN.astype("str")
    
    print("  - Creating a 3 Charactar PARNO in the spaces file")
    aos_billet_export["PARNO_3_CHAR"] = aos_billet_export.apply(
        lambda row: row.PARNO[0:3],
        axis = 1
    )
    
    print("  - Consolidating AOS ASIs into ASI_LIST column")
    aos_billet_export["ASI_LIST"] = aos_billet_export.apply(
        lambda row: pd.Series(
            data = [row.ASI1, row.ASI2, row.ASI3, row.ASI4]
        ).dropna().to_list(),
        axis = 1
    )
    
    print("  - Consolidating AOS RMKs into RMK_LIST column")
    aos_billet_export["RMK_LIST"] = aos_billet_export.apply(
        lambda row: pd.Series(
            data = [row.RMK1, row.RMK2, row.RMK3, row.RMK4]
        ).dropna().to_list(),
        axis = 1
    )
            
    print("  - Creating a concatenation of UIC PARA LN")
    aos_billet_export["UIC_PAR_LN"] = aos_billet_export.apply(
        lambda row: row.UIC + row.PARNO + row.LN,
        axis = 1
    )
    
    print("  - Converting S_Date and T_Date to date time format")
    aos_billet_export["T_DATE"] = pd.to_datetime(
        aos_billet_export.T_DATE, infer_datetime_format = True, errors = "ignore"        
    )
    aos_billet_export["S_DATE"] = pd.to_datetime(
        aos_billet_export.S_DATE, infer_datetime_format = True, errors = "ignore"        
    )
            
    """
    fms_file['LOWEST_UIC'] = fms_file.apply(
                lambda row: row["UIC"] if pd.isna(row["FULLSUBCO"]) else row["FULLSUBCO"],
                axis = 1                
                )   
    """
    return aos_billet_export

"""Process the EMILPO faces assignment file Pandas DF
" EMILPO Headers: SSN_MASK,UIC_PAR_LN,UIC_CD,PARENT_UIC_CD,STRUC_CMD_CD,PARNO,
" LN,MIL_POSN_RPT_NR,DUTY_ASG_DT,RANK_AB,ASG_RANK,MOS_AOC1,MOS_AOC2,MOS_AOC3,
" MOS_AOC4,MOS_AOC5,MOS_AOC6,MOS_AOC7,MOS_AOC8,MOS_AOC9,MOS_AOC10,MOS_AOC11,
" MOS_AOC12,MOS_AOC13,SQI1,SQI2,SQI3,SQI4,SQI5,SQI6,SQI7,SQI8,SQI9,SQI10,SQI11,
" SQI12,SQI13,SQI14,SQI15,SQI16,ASI1,ASI2,ASI3,ASI4,ASI5,ASI6,ASI7,ASI8,ASI9,
" ASI10,ASI11,ASI12,ASI13,ASI14
"
"""
def process_emilpo_assignments(
    emilpo_assignments, 
    rank_grade_xwalk, 
    grade_mismatch_xwalk, 
    consolidate = True
   ):
    print("Processing EMILPO assignments file")
    emilpo_assignments["stage_matched"] = 0    
    print("  - Renaming columns")
    emilpo_assignments = emilpo_assignments.rename(columns = {
                        "UIC_CD" : "UIC"
            })
        
    print("  - Creating 3 Char PARNO Column")    
    emilpo_assignments.PARNO = emilpo_assignments.PARNO.astype("str")
    emilpo_assignments["PARNO_3_CHAR"] = emilpo_assignments.apply(
        lambda row: row.PARNO[0:3],
        axis = 1
    )
    
    print("  - Mapping grade to rank in the eMILPO assignments file")
    emilpo_assignments["GRADE"] = emilpo_assignments.apply(
            lambda row: rank_grade_xwalk.loc[row.RANK_AB].GRADE, 
            axis = 1
            )
    
    print("  - Mapping grade to one-up grade in the eMILPO assignments file")
    emilpo_assignments["ONE_UP"] = emilpo_assignments.apply(
            lambda row: grade_mismatch_xwalk.loc[row.GRADE].ONE_UP, 
            axis = 1
            )
    
    print("  - Mapping grade to two-up grade in the eMILPO assignments file")
    emilpo_assignments["TWO_UP"] = emilpo_assignments.apply(
            lambda row: grade_mismatch_xwalk.loc[row.GRADE].TWO_UP, 
            axis = 1
            )
    
    print("  - Mapping grade to one-down grade in the eMILPO assignments file")
    emilpo_assignments["ONE_DN"] = emilpo_assignments.apply(
            lambda row: grade_mismatch_xwalk.loc[row.GRADE].ONE_DN, 
            axis = 1
            )    
    
    if(consolidate):
        print("  - Consolidating ASIs into ASI_LIST column in the eMILPO assignments file")
        emilpo_assignments["ASI_LIST"] = emilpo_assignments.apply(
                lambda row: pd.Series(data = [
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
                        ]).dropna().to_list(),
                        axis = 1
                )
        
        print("  - Consolidating SQIs into SQI_LIST column in the eMILPO assignments file")
        emilpo_assignments["SQI_LIST"] = emilpo_assignments.apply(
                lambda row: pd.Series(data = [
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
                        ]).dropna().to_list(),
                        axis = 1
                )
        
        print("  - Consolidating MOS/AOC into MOS_AOC_LIST column in the eMILPO assignments file")
        emilpo_assignments["MOS_AOC_LIST"] = emilpo_assignments.apply(
                lambda row: pd.Series(data = [
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
                        ]).dropna().to_list(),
                        axis = 1
                )
    
    print("  - Converting duty assignment date to date_time format")
    emilpo_assignments["DUTY_ASG_DT"] = pd.to_datetime(
        emilpo_assignments.DUTY_ASG_DT, infer_datetime_format = True, errors = "ignore"
    )
    
    print("  - Adding leading 0 to LN for single digit LNs")
    emilpo_assignments.LN = emilpo_assignments.LN.astype("str")
    emilpo_assignments.LN = emilpo_assignments.apply(
        lambda row: "0" + row.LN if len(row.LN) == 1 else row.LN, 
        axis = 1        
    )
        
    return emilpo_assignments
