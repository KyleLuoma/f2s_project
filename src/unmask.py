# -*- coding: utf-8 -*-
import pandas as pd

def unmask_and_export(
        all_faces_to_matched_spaces, 
        attached_faces_to_matched_spaces,
        timestamp, 
        emilpo_key_date,
        cmd_labels = ""
):
    all_faces_to_matched_spaces["ASGN_TYPE"] = "PERM"
    attached_faces_to_matched_spaces = attached_faces_to_matched_spaces.reset_index()
    attached_faces_to_matched_spaces["ASGN_TYPE"] = "TEMP"
    all_faces_to_matched_spaces = all_faces_to_matched_spaces.drop_duplicates(
        subset = "SSN_MASK"
    )
    all_faces_to_matched_spaces = all_faces_to_matched_spaces.append(
        attached_faces_to_matched_spaces
    )
    key_file = pd.read_csv(
        "C:/Users/KYLE/Documents/f2s_unmask/emilpo assignments map " + emilpo_key_date + ".csv",
        dtype = {"SSN_MASK_HASH" : "str", "SSN" : "str"}
    ).set_index("SSN_MASK_HASH")
    key_file = key_file.append(
        pd.read_csv(
            "C:/Users/KYLE/Documents/f2s_unmask/rcms assignments map " + emilpo_key_date + ".csv",
            dtype = {"SSN_MASK_HASH" : "str", "SSN" : "str"}            
        ).set_index("SSN_MASK_HASH")
    )   
    all_faces_to_matched_spaces = all_faces_to_matched_spaces.join(
        key_file,
        on = "SSN_MASK"        
    )
    all_faces_to_matched_spaces.SSN = all_faces_to_matched_spaces.apply(
        lambda row: str(row.SSN).zfill(9),
        axis = 1
    )
    del all_faces_to_matched_spaces['SSN_MASK']
    all_faces_to_matched_spaces.dropna(
        subset = ["SSN"]     
    ).rename(columns = {"GFC1" : "GFC"})[[
       'SSN', 'FMID', 'DRRSA_ASGMT', 'STRUC_CMD_CD', 'GFC', 'GFC 1 Name', 'RCC',
       'PARENT_UIC_CD', 'UIC', 'UIC_facesfile', 'UIC_aos', 'UNITNAME',
       'PARNO_facesfile', 'LN_facesfile', 'MIL_POSN_RPT_NR', 'PARENT_TITLE',
       'PARNO_aos', 'LN_aos', 'RMK_LIST', 'RMK1', 'RMK2', 'RMK3', 'RMK4',
       'RANK_AB', 'GRADE_facesfile', 'MOS_AOC1', 'MOS_AOC2',
       'GRADE_aos', 'POSCO', 'stage_matched', 'MATCH_DESCRIPTION',
       'DUTY_ASG_DT', 'S_DATE', 'T_DATE', 'ASSIGNMENT_AGE', 'POSITION_AGE',
       'ASG_OLDER_THAN_POS', 'ADD_UIC_TO_AOS', 'CREATE_TEMPLET',
       'AOS_FILE_DATE', 'EMILPO_FILE_DATE', 'RCMS_FILE', 'DRRSA_ADCON',
       'DRRSA_ADCON_IN_AOS', 'DRRSA_ARLOC', 'DRRSA_GEOLOCATIONNAME',
       'DRRSA_HOGEO', 'PPA', 'APART_POSN_KEY', 'ASGN_TYPE'
    ]].to_csv(
        "C:/Users/KYLE/Documents/f2s_unmask/f2s_file_export/" + 
        cmd_labels 
        + "all_faces_matched_spaces_"
        + timestamp + ".csv",
        sep = "\t"
    )
