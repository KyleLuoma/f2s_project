import process_data


def run_face_match_diagnostics(
    faces,
    face_space_match,
    spaces,
    last_templet_stage,
    match_phases,
    all_uics,
    add_vacant_position_rows = True
):
    all_faces_to_matched_spaces = face_space_match_analysis(
        faces, face_space_match, spaces
    )
    all_faces_to_matched_spaces = process_data.add_match_phase_description(
        all_faces_to_matched_spaces, match_phases
    )
    all_faces_to_matched_spaces = diagnose_mismatch_in_target(
        all_faces_to_matched_spaces, 
        all_uics, 
        last_templet_stage
    )
    if(add_vacant_position_rows):
        all_faces_to_matched_spaces = add_vacant_positions(
            all_faces_to_matched_spaces,
            spaces        
        )    
        all_faces_to_matched_spaces = reorder_all_faces_to_matched_spaces_columns(
            all_faces_to_matched_spaces        
        )
    return all_faces_to_matched_spaces



def face_space_match_analysis(faces, face_space_match, spaces):
    #Export a join of eMILPO and AOS using face_space_match to connect
    all_faces_to_matched_spaces = faces[[
        "SSN_MASK", "UIC", "PARENT_UIC_CD", "STRUC_CMD_CD", "GFC", "GFC 1 Name",
        "PARNO", "LN", "MIL_POSN_RPT_NR", "APART_POSN_KEY", "DUTY_ASG_DT", "MOS_AOC1", "MOS_AOC2", 
        "RANK_AB", "GRADE",
        "DRRSA_ADCON", "DRRSA_HOGEO", "DRRSA_ARLOC", "DRRSA_GEOLOCATIONNAME",
        "DRRSA_ASGMT", "PPA", "DRRSA_ADCON_IN_AOS", "ASSIGNMENT_AGE", "RCC",
        "UNITNAME", "EMILPO_FILE_DATE", "RCMS_FILE"
    ]].set_index("SSN_MASK", drop = True)
    all_faces_to_matched_spaces = all_faces_to_matched_spaces.join(
        face_space_match.reset_index(
            drop = True
        ).set_index(
            "SSN_MASK"
        )[["stage_matched", "ENCUMBERED", "FMID"]],
        lsuffix = "_facesfile",
        rsuffix = "_f2s"
    )
    all_faces_to_matched_spaces = all_faces_to_matched_spaces.reset_index(
    ).set_index(
        "FMID"
    ).join(
        spaces.reset_index(
            drop = True
        ).set_index(
            "FMID"
        )[[
            "UIC", "PARNO", "LN", "RMK_LIST", "RMK1", "RMK2", "RMK3", "RMK4",
            "PARENT_TITLE", "GRADE", "POSCO", 
            "S_DATE", "T_DATE", "POSITION_AGE", "AOS_FILE_DATE"
        ]],
        lsuffix = "_facesfile",
        rsuffix = "_aos"
    )
    all_faces_to_matched_spaces["ASG_OLDER_THAN_POS"] = (
        all_faces_to_matched_spaces.S_DATE > all_faces_to_matched_spaces.DUTY_ASG_DT
    )
    return all_faces_to_matched_spaces

def add_vacant_positions(
        all_faces_to_matched_spaces,
        spaces        
    ):
    import pandas as pd
    print(" - Adding vacant positions to all_faces_to_matched_spaces")
    vacant_spaces = spaces.where(
        ~spaces.FMID.isin(all_faces_to_matched_spaces.reset_index().FMID)
    ).dropna(how = "all")
    all_faces_to_matched_spaces = all_faces_to_matched_spaces.reset_index().append(
        vacant_spaces.rename(columns = {       
            "UIC" : "UIC_aos",
            "PARNO" : "PARNO_aos",
            "LN" : "LN_aos",
            "GRADE" : "GRADE_aos"
        })[[
            "FMID", "UIC_aos", "PARNO_aos", "LN_aos", 
            "RMK_LIST", "RMK1", "RMK2", "RMK3", "RMK4",
            "PARENT_TITLE",
            "GRADE_aos", "POSCO", "S_DATE", "T_DATE", "POSITION_AGE", 
            "AOS_FILE_DATE", "DRRSA_ADCON", "DRRSA_ARLOC", "DRRSA_ASGMT",
            "DRRSA_GEOLOCATIONNAME", "DRRSA_HOGEO"
        ]]
    )
    print("  - Merging UICs from faces and spaces into one column")
    all_faces_to_matched_spaces["UIC"] = all_faces_to_matched_spaces.apply(
        lambda row: row.UIC_facesfile if not pd.isna(row.UIC_facesfile)    
        else row.UIC_aos,
        axis = 1
    )
    return all_faces_to_matched_spaces
    
    
def diagnose_mismatch_in_target(target, all_uics, last_templet_stage):
    import pandas as pd
    print("Analyzing unmatched faces")
    target["ADD_UIC_TO_AOS"] = False
    target["CREATE_TEMPLET"] = False
    print(" - Checking if UICs are in AOS")
    target.ADD_UIC_TO_AOS = (~target.UIC_facesfile.isin(all_uics))
    
    print(" - Checking if templets are needed")
    target.CREATE_TEMPLET = target.apply(
        lambda row: True if (
            not row.ADD_UIC_TO_AOS and (
                row.stage_matched == 0 or row.stage_matched > last_templet_stage
            )
        ) else False,
        axis = 1
    )
    return target# -*- coding: utf-8 -*-

def space_available_analysis(faces, face_space_match, spaces):
    #Return a DF with information about all spaces
    all_spaces_to_matched_faces = spaces.copy()
    all_spaces_to_matched_faces = all_spaces_to_matched_faces.reset_index(
        ).set_index(
            "FMID"
        ).join(
            face_space_match.reset_index(drop = True).set_index("FMID"),
            lsuffix = "_spaces",
            rsuffix = "_face_space_match"
        )
    faces_subset = faces[[
        "GFC", "GFC 1 Name", "GRADE", "MOS_AOC_LIST", "SQI_LIST", 
        "STRUC_CMD_CD", "UIC", "PARNO", "LN", "MIL_POSN_RPT_NR", "SSN_MASK"
    ]].set_index("SSN_MASK")
    all_spaces_to_matched_faces = all_spaces_to_matched_faces.join(
        faces_subset,
        on = "SSN_MASK_face_space_match",
        lsuffix = "_spaces",
        rsuffix = "_faces"
    )
    return all_spaces_to_matched_faces
    
def reorder_all_faces_to_matched_spaces_columns(all_faces_to_matched_spaces):
    return all_faces_to_matched_spaces[[
        'DRRSA_ASGMT', 'STRUC_CMD_CD',
        'GFC', 'GFC 1 Name', 'RCC', 'PARENT_UIC_CD', 
        'UIC', 'UIC_facesfile', 'UIC_aos', 'UNITNAME', 
        'PARNO_facesfile', 'LN_facesfile', 'MIL_POSN_RPT_NR',  
        'PARENT_TITLE', 'PARNO_aos', 'LN_aos', 
        'RMK_LIST', "RMK1", "RMK2", "RMK3", "RMK4", 'FMID',
        'RANK_AB', 'GRADE_facesfile', 'MOS_AOC1', 'MOS_AOC2',
        'GRADE_aos', 'POSCO', 'SSN_MASK', 
        'stage_matched', 'MATCH_DESCRIPTION', 
        'DUTY_ASG_DT', 'S_DATE', 'T_DATE', 
        'ASSIGNMENT_AGE', 'POSITION_AGE', 'ASG_OLDER_THAN_POS',
        'ADD_UIC_TO_AOS', 'CREATE_TEMPLET',
        'AOS_FILE_DATE', 'EMILPO_FILE_DATE', 'RCMS_FILE',
        'DRRSA_ADCON', 'DRRSA_ADCON_IN_AOS',
        'DRRSA_ARLOC',  'DRRSA_GEOLOCATIONNAME', 'DRRSA_HOGEO',
        'PPA', 'APART_POSN_KEY'
    ]]
    
    
    
    
    
    

