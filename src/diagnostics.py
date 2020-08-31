def face_space_match_analysis(faces, face_space_match, spaces):
    #Export a join of eMILPO and AOS using face_space_match to connect
    all_faces_to_matched_spaces = faces[[
        "SSN_MASK", "UIC", "PARENT_UIC_CD", "STRUC_CMD_CD", "GFC", "GFC 1 Name",
        "PARNO", "LN", "MIL_POSN_RPT_NR", "DUTY_ASG_DT","RANK_AB", "GRADE",
        "DRRSA_ADCON", "DRRSA_HOGEO", "DRRSA_ARLOC", "DRRSA_GEOLOCATIONNAME",
        "DRRSA_ASGMT", "PPA", "DRRSA_ADCON_IN_AOS", "ASSIGNMENT_AGE", "RCC",
        "UNITNAME", "EMILPO_FILE_DATE", "RCMS_FILE"
    ]].set_index("SSN_MASK", drop = True)
    all_faces_to_matched_spaces = all_faces_to_matched_spaces.join(
        face_space_match.reset_index(
            drop = True
        ).set_index(
            "SSN_MASK"
        )[["stage_matched", "FMID"]],
        lsuffix = "_emilpo",
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
            "UIC", "PARNO", "LN", "PARENT_TITLE", "GRADE", "POSCO", 
            "S_DATE", "T_DATE", "POSITION_AGE", "AOS_FILE_DATE"
        ]],
        lsuffix = "_emilpo",
        rsuffix = "_aos"
    )
    all_faces_to_matched_spaces["ASG_OLDER_THAN_POS"] = (
        all_faces_to_matched_spaces.S_DATE > all_faces_to_matched_spaces.DUTY_ASG_DT
    )
    return all_faces_to_matched_spaces


def diagnose_mismatch_in_target(target, all_uics, last_templet_stage):
    print("Analyzing unmatched faces")
    target["ADD_UIC_TO_AOS"] = False
    target["CREATE_TEMPLET"] = False
    print(" - Checking if UICs are in AOS")
    target.ADD_UIC_TO_AOS = (~target.UIC_emilpo.isin(all_uics))
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
    
    
    
    
    
    
    
    
    

