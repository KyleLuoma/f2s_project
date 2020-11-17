# -*- coding: utf-8 -*-
import pandas as pd

def unmask_and_export(
        all_faces_to_matched_spaces, 
        timestamp, 
        emilpo_key_date,
        cmd_labels = ""
):
    key_file = pd.read_csv(
        "F:/aos/master_files/emilpo/emilpo_maps/emilpo assignments map " + emilpo_key_date + ".csv",
        dtype = {"SSN_MASK_HASH" : "str", "SSN" : "str"}
    ).set_index("SSN_MASK_HASH")
    all_faces_to_matched_spaces = all_faces_to_matched_spaces.join(
        key_file,
        on = "SSN_MASK"        
    )
    del all_faces_to_matched_spaces['SSN_MASK']
    all_faces_to_matched_spaces.dropna(
        subset = ["SSN"]     
    )[[
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
       'DRRSA_HOGEO', 'PPA', 'APART_POSN_KEY'
    ]].drop_duplicates(subset = "SSN").to_csv(
        "F:/aos/f2s_project/export/f2s_unmasked/" + 
        cmd_labels 
        + "all_faces_matched_spaces_"
        + timestamp + ".csv",
        sep = "\t"
    )
