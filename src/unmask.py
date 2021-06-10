# -*- coding: utf-8 -*-
import pandas as pd

def unmask_and_return(
        file_config,
        all_faces_to_matched_spaces,
        cmd_labels = ""
):
    emilpo_key = file_config['AC_KEY_FILE']
    tapdbr_key = file_config['AR_KEY_FILE']
    print(" - Unmasking and returning unmasked data to calling function.")
    key_file = pd.read_csv(
        emilpo_key,
        sep = "|",
        converters = {"SSN_MASK_HASH" : str, "SSN" : str}
    ).set_index("SSN_MASK_HASH")
    key_file = key_file.append(
        pd.read_csv(
            tapdbr_key,
            sep = '|',
            converters = {"SSN_MASK_HASH" : str, "SSN" : str}            
        ).set_index("SSN_MASK_HASH")
    )
    key_file = key_file.append(
        pd.read_csv(
            file_config['AR_KEY_FILE'],
            sep = '|',
            converters = {"SSN_MASK_HASH" : str, "SSN" : str}            
        ).set_index("SSN_MASK_HASH")
    )  
    key_file.dropna(subset = ["SSN"], inplace = True)
    all_faces_to_matched_spaces = all_faces_to_matched_spaces.join(
        key_file,
        on = "SSN_MASK"        
    )
    all_faces_to_matched_spaces.SSN = all_faces_to_matched_spaces.apply(
        lambda row: str(row.SSN).zfill(9),
        axis = 1
    )
    del all_faces_to_matched_spaces['SSN_MASK']
    return all_faces_to_matched_spaces
        

def unmask_and_export(
        all_faces_to_matched_spaces, 
        attached_faces_to_matched_spaces,
        timestamp, 
        file_config,
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
        file_config['AC_KEY_FILE'],
        sep = '|',
        converters = {"SSN_MASK_HASH" : str, "SSN" : str}
    ).set_index("SSN_MASK_HASH")
    key_file = key_file.append(
        pd.read_csv(
            file_config['AR_KEY_FILE'],
            sep = '|',
            converters = {"SSN_MASK_HASH" : str, "SSN" : str}            
        ).set_index("SSN_MASK_HASH")
    )  
    key_file.dropna(subset = ["SSN"], inplace = True)
    for column in key_file.columns:
        try:
            key_file[column] = key_file[column].str.strip()
        except AttributeError:
            pass
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
    ]].drop_duplicates(subset = ['SSN', 'FMID']).to_csv(
        file_config['KEY_PATH'] + 
        cmd_labels 
        + "all_faces_matched_spaces_"
        + timestamp + ".csv",
        sep = "\t"
    )
