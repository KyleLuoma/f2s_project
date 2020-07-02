# =============================================================================
# Core matching function that iterates through available spaces and aligns
# faces based on matching criteria passed in the criteria list
# =============================================================================
def match(criteria, faces, spaces, stage, face_space_match, verbose):
    print(
        " - Stage ", str(stage), "Faces File Shape:", faces.shape, 
        "Spaces File Shape:", spaces.shape
    )
    faces_index_labels = []
    spaces_index_labels = []
    
    face_space_match.set_index(
        face_space_match.FMID, drop = False, inplace = True
    )
    face_space_match.sort_index()
    
    #Analyze match criteria and set multi index array for spaces and faces files
    #Order is critical here. Face and Space indices need to be symetrical for matching to work
    if(criteria.UIC.loc[stage]):
        faces_index_labels.append("UIC")
        spaces_index_labels.append("UIC")
        
    if(criteria.PARENT_UIC.loc[stage]):
        faces_index_labels.append("PARENT_UIC_CD")
        spaces_index_labels.append("UIC")
        
    if(criteria.LDUIC.loc[stage]):
        faces_index_labels.append("UIC")
        spaces_index_labels.append("LDUIC")
        
    if(criteria.HSDUIC.loc[stage]):
        faces_index_labels.append("HSDUIC")
        spaces_index_labels.append("UIC")
        
    if(criteria.PARNO.loc[stage]):
        faces_index_labels.append("PARNO")
        spaces_index_labels.append("PARNO")
        
    if(criteria.PARNO_3_CHAR.loc[stage]):
        faces_index_labels.append("PARNO_3_CHAR")
        spaces_index_labels.append("PARNO_3_CHAR")
        
    if(criteria.LN.loc[stage]):
        faces_index_labels.append("LN")
        spaces_index_labels.append("LN")
    
    #Match on grade without variance
    if(criteria.GRADE.loc[stage]):
        if(criteria.GRADE_VAR.loc[stage] == 0):        
            faces_index_labels.append("GRADE")
            spaces_index_labels.append("GRADE")

        #Match on one up
        elif(criteria.GRADE_VAR.loc[stage] == 1):
            faces_index_labels.append("ONE_UP")
            spaces_index_labels.append("GRADE")
            
        elif(criteria.GRADE_VAR.loc[stage] >= 2):
            faces_index_labels.append("TWO_UP")
            spaces_index_labels.append("GRADE")
            
        elif(criteria.GRADE_VAR.loc[stage] < 0):
            faces_index_labels.append("ONE_DN")
            spaces_index_labels.append("GRADE")
        
    if(criteria.PRI_MOS.loc[stage] and not criteria.ALT_MOS.loc[stage]):
        faces_index_labels.append("MOS_AOC1")
        spaces_index_labels.append("POSCO")
        
    if(criteria.ALT_MOS.loc[stage] and not criteria.PRI_MOS.loc[stage]):
        faces_index_labels.append("MOS_AOC2")
        spaces_index_labels.append("POSCO")
        
    if(criteria.SQI.loc[stage]):
        spaces_index_labels.append("SQI1")  
        
    if(criteria.TEMPLET.loc[stage]):
        faces_index_labels.append("TMP_PARNO")
        faces_index_labels.append("TMP_LN")
        spaces_index_labels.append("PARNO_3_CHAR")
        spaces_index_labels.append("LN")
        
    counter = 0
    stage_matched = 0 #Increase if match found
    exception_count = 0 #Increase if exception thrown
    asgn_age_reject_count = 0
    f_ix = 0 #Faces index
    s_ix = 0 #Spaces index
    faces = faces.reset_index(drop = True)
    spaces = spaces.reset_index(drop = True)
    spaces_index_labels.append("POSITION_AGE")
    faces_index_labels.append("ASSIGNMENT_AGE")
    spaces_index_labels.append("FMID") #This will be the last column in the list
    faces_index_labels.append("SSN_MASK") #This will be the last column in the list
    
    if(stage == 1): #Overwrite the index labels for perfect matching stage 1
        
        face_list = faces_index_labels = [
            "UIC_PAR_LN", "ASSIGNMENT_AGE", "SSN_MASK"
        ]
        space_list = spaces_index_labels = [
            "UIC_PAR_LN", "POSITION_AGE", "FMID"
        ]
    
    print(" - Matching stage ", str(stage))
    #Force type conversion before sorting and matching:
    faces["UIC_PAR_LN"] = faces["UIC_PAR_LN"].astype("str")
    faces["PARNO"] = faces["PARNO"].astype("str")
    faces["LN"] = faces["LN"].astype("str")
    faces["TMP_PARNO"] = faces["TMP_PARNO"].astype("str")
    faces["TMP_LN"] = faces["TMP_LN"].astype("str")
    faces["PARENT_UIC_CD"] = faces["PARENT_UIC_CD"].astype("str")
    faces["SSN_MASK"] = faces["SSN_MASK"].astype("str")
    faces["ASSIGNMENT_AGE"] = faces["ASSIGNMENT_AGE"].astype("int64")
    faces["GRADE"] = faces["GRADE"].astype("str")
    spaces["LDUIC"] = spaces["LDUIC"].astype("str")
    spaces["PARNO"] = spaces["PARNO"].astype("str")
    spaces["PARNO_3_CHAR"] = spaces["PARNO_3_CHAR"].astype("str")
    spaces["LN"] = spaces["LN"].astype("str")

    face_list = sorted(faces[faces_index_labels].values.tolist())
    space_list = sorted(spaces[spaces_index_labels].values.tolist())
    
    f_total = len(face_list)
    s_total = len(space_list)
    
    compare_ix = len(faces_index_labels) - 2   #Number of columns for comparison
    age_ix = len(faces_index_labels) - 2       #Column index # for AGE
    fmid_ix = len(spaces_index_labels) - 1     #Column index # for FMID
    mask_ix = len(faces_index_labels)  - 1     #Column index # for SSN MASK
    
    print("  - Comparing faces", faces_index_labels[0:compare_ix], 
          "to", spaces_index_labels[0:compare_ix])            
    
# =============================================================================
#     This is the matching algorithm!!!
# =============================================================================
    comparison_count = 0
    while(f_ix < f_total and s_ix < s_total): 
        comparison_count += 1
        if verbose and comparison_count % 2000 == 0: print("    Compared", str(comparison_count), "face_ix:", str(f_ix), "space_ix:", str(s_ix), "Match:", str(stage_matched))
        if(face_list[f_ix][0:compare_ix] == space_list[s_ix][0:compare_ix]):
            if(int(face_list[f_ix][age_ix]) <= int(space_list[s_ix][age_ix]) or ~criteria.AGE_MATTERS.loc[stage]):
                face_space_match.at[space_list[s_ix][fmid_ix], "SSN_MASK"] = face_list[f_ix][mask_ix]
                face_space_match.at[space_list[s_ix][fmid_ix], "stage_matched"] = stage
                f_ix += 1
                s_ix += 1
                stage_matched += 1
            else:
                f_ix += 1
                exception_count += 1
                asgn_age_reject_count += 1
            counter += 1
        elif(face_list[f_ix][0:compare_ix] < space_list[s_ix][0:compare_ix]):
            f_ix += 1
            exception_count += 1
            counter += 1
        elif(face_list[f_ix][0:compare_ix] > space_list[s_ix][0:compare_ix]):
            s_ix += 1
            
    print(" - STAGE", str(stage), "Total records reviewed:", str(counter), 
        " Matched:", str(stage_matched),
        " Exceptions:", str(exception_count),
        " Assignment Age Rejects:", str(asgn_age_reject_count)
    )
    
    return faces.where(
        ~faces.SSN_MASK.isin(face_space_match.SSN_MASK)
    ).dropna(how = "all"), spaces, face_space_match# -*- coding: utf-8 -*-

# =============================================================================
# Iterates through all rows of match phases and calls the core match function
# for each combination. Eliminates matched spaces and faces with each call to 
# match().
# =============================================================================
def full_run(
    criteria, faces, spaces, verbose = False,
    include_only_cmds = [], exclude_cmds = [], exclude_rmks = []
):
    print("Executing full matching run")
    face_space_match = spaces.copy()[["FMID", "SSN_MASK", "stage_matched"]]
    face_space_match.SSN_MASK = face_space_match.SSN_MASK.astype("str")
    rmks_excluded = False
    
    if(len(include_only_cmds) > 0):
        print(" - for commands:", include_only_cmds)
        spaces = spaces.where(
            spaces.DRRSA_ASGMT.isin(include_only_cmds)
        ).dropna(how = "all")
        faces = faces.where(
            faces.STRUC_CMD_CD.isin(include_only_cmds)
        ).dropna(how = "all")
        
    if(len(exclude_cmds) > 0):
        print(" - excluding commands:", exclude_cmds)
        spaces = spaces.where(
            ~spaces.DRRSA_ASGMT.isin(exclude_cmds)
        ).dropna(how = "all")
        faces = faces.where(
            ~faces.STRUC_CMD_CD.isin(exclude_cmds)
        ).dropna(how = "all")
    
    for i in range(1, criteria.shape[0] + 1):
        print(" *** Calling match() for stage ", str(i), "***")
        faces, spaces, face_space_match = match(
            criteria,  
            faces.where(
                ~faces.SSN_MASK.isin(face_space_match.SSN_MASK)
            ).dropna(how = "all"),
            spaces.where(
                spaces.FMID.isin(
                    face_space_match.where(
                        face_space_match.stage_matched == 0
                    ).dropna(how = "all").FMID
                )
            ).dropna(how = "all"), 
            i,
            face_space_match,
            verbose
        )
        print(" - match() returned", 
            str(face_space_match.where(
                face_space_match.stage_matched == i
            ).dropna(how = "all").shape[0]), 
            " matches."
        )
        #Runs after phase 1 to enable perfect match on all positions regardless of RMK code
        if(not rmks_excluded and len(exclude_rmks) > 0):
            print(" - excluding billets with remarks:", exclude_rmks)
            spaces = spaces.where(
                ~spaces.RMK1.isin(exclude_rmks)
            ).dropna(how = "all")
            spaces = spaces.where(
                ~spaces.RMK2.isin(exclude_rmks)
            ).dropna(how = "all")
            spaces = spaces.where(
                ~spaces.RMK3.isin(exclude_rmks)
            ).dropna(how = "all")
            spaces = spaces.where(
                ~spaces.RMK4.isin(exclude_rmks)
            ).dropna(how = "all")
            rmks_excluded = True
    return faces, spaces, face_space_match