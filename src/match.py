import pandas as pd
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
        if(criteria.TEMP_ASSIGNMENT.loc[stage]):
            faces_index_labels.append("ATTACH_UIC")
        else:
            faces_index_labels.append("UIC")
        spaces_index_labels.append("UIC")

        
    if(criteria.PARENT_UIC.loc[stage]):
        faces_index_labels.append("PARENT_UIC_CD")
        spaces_index_labels.append("UIC")
        
    if(criteria.LDUIC.loc[stage]):
        faces_index_labels.append("UIC")
        spaces_index_labels.append("LDUIC")
        
    if(criteria.HSDUIC.loc[stage]):
        if(criteria.TEMP_ASSIGNMENT.loc[stage]):
            faces_index_labels.append("HSDUIC_TEMP")
        else:
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
        #faces_index_labels.append("TMP_PARNO")
        #faces_index_labels.append("TMP_LN")
        #spaces_index_labels.append("PARNO_3_CHAR")
        #spaces_index_labels.append("LN")
        spaces_index_labels.append("IS_TEMPLET")
        faces["TRUE"] = True
        faces_index_labels.append("TRUE")
        
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
    
    if(stage == -1): #Overwrite the index labels for perfect matching stage 1
        
        face_list = faces_index_labels = [
            "UIC_PAR_LN", "ASSIGNMENT_AGE", "SSN_MASK"
        ]
        space_list = spaces_index_labels = [
            "UIC_PAR_LN", "POSITION_AGE", "FMID"
        ]
    
    print(" - Matching stage ", str(stage))
    #Force type conversion before sorting and matching:
    try:
        faces["UIC_PAR_LN"] = faces["UIC_PAR_LN"].astype("str")
        faces["PARNO"] = faces["PARNO"].astype("str")
        faces["LN"] = faces["LN"].astype("str")
        faces["TMP_PARNO"] = faces["TMP_PARNO"].astype("str")
        faces["TMP_LN"] = faces["TMP_LN"].astype("str")
        faces["PARENT_UIC_CD"] = faces["PARENT_UIC_CD"].astype("str")
        faces["SSN_MASK"] = faces["SSN_MASK"].astype("str")
        faces["ASSIGNMENT_AGE"] = faces["ASSIGNMENT_AGE"].astype("int64")
        faces["GRADE"] = faces["GRADE"].astype("str")
        faces["MOS_AOC1"] = faces["MOS_AOC1"].astype("str")
        spaces["LDUIC"] = spaces["LDUIC"].astype("str")
        spaces["PARNO"] = spaces["PARNO"].astype("str")
        spaces["PARNO_3_CHAR"] = spaces["PARNO_3_CHAR"].astype("str")
        spaces["LN"] = spaces["LN"].astype("str")
        spaces["GRADE"] = spaces["GRADE"].astype("str")
        spaces["POSCO"] = spaces["POSCO"].astype("str")
    except KeyError:
        pass

    face_list = sorted(faces[faces_index_labels].values.tolist())
    space_list = sorted(spaces[spaces_index_labels].values.tolist())
    
    
    f_ix = len(face_list) - 1
    s_ix = len(space_list) - 1
    
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
    while(f_ix >= 0 and s_ix >= 0): 
        comparison_count += 1
        if verbose and comparison_count % 2000 == 0: print("    Compared", str(comparison_count), "face_ix:", str(f_ix), "space_ix:", str(s_ix), "Match:", str(stage_matched))
        if(face_list[f_ix][0:compare_ix] == space_list[s_ix][0:compare_ix]):
            if(int(face_list[f_ix][age_ix]) <= int(space_list[s_ix][age_ix]) or ~criteria.AGE_MATTERS.loc[stage]):
                face_space_match.at[space_list[s_ix][fmid_ix], "SSN_MASK"] = face_list[f_ix][mask_ix]
                if(not criteria.TEMP_ASSIGNMENT.loc[stage]):
                    face_space_match.at[space_list[s_ix][fmid_ix], "stage_matched"] = stage
                else:
                    face_space_match.at[space_list[s_ix][fmid_ix], "TEMP_STAGE_MATCHED"] = stage
                face_space_match.at[space_list[s_ix][fmid_ix], "ENCUMBERED"] = True
                f_ix -= 1
                s_ix -= 1
                stage_matched += 1
            else:
                f_ix -= 1
                exception_count += 1
                asgn_age_reject_count += 1
            counter += 1
        elif(face_list[f_ix][0:compare_ix] > space_list[s_ix][0:compare_ix]):
            f_ix -= 1
            exception_count += 1
            counter += 1
        elif(face_list[f_ix][0:compare_ix] < space_list[s_ix][0:compare_ix]):
            s_ix -= 1
            
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
    face_space_match["TEMP_STAGE_MATCHED"] = 0
    face_space_match["ENCUMBERED"] = False
    rmks_excluded = False
    
    if(len(include_only_cmds) > 0):
        print(" - for commands:", include_only_cmds)
        faces = faces.where(
            faces.STRUC_CMD_CD.isin(include_only_cmds)
        ).dropna(how = "all")
        
    if(len(exclude_cmds) > 0):
        print(" - excluding commands:", exclude_cmds)
        faces = faces.where(
            ~faces.STRUC_CMD_CD.isin(exclude_cmds)
        ).dropna(how = "all")
    
    for i in range(1, criteria.shape[0] + 1):
        print(" *** Calling match() for stage ", str(i), "***")
        faces_with_matches = pd.Series()
        faces_to_match = pd.DataFrame()
        
        #Check if this is a permanent vs temporary assignment run
        #If temporary, we only care if TEMP_STAGE_MATCHED is not 0, otherwise
        #we only care if stage_matched is 0.
        print(" - Modifying faces_to_match DF based on assignment vs attachment matching phase")
        if(criteria.TEMP_ASSIGNMENT.loc[i]):
            faces_with_matches = face_space_match.where(
                face_space_match.TEMP_STAGE_MATCHED != 0
            ).dropna(how = "all").SSN_MASK
            faces_to_match = faces.dropna(subset = ["ATTACH_UIC"])
        else:
            faces_with_matches = face_space_match.where(
                face_space_match.stage_matched != 0
            ).dropna(how = "all").SSN_MASK
            faces_to_match = faces
            
        faces_to_match = faces_to_match.where( 
            ~faces_to_match.SSN_MASK.isin(faces_with_matches)
        ).dropna(how = "all")
        
        spaces_to_match = spaces.where(
            spaces.FMID.isin(
                face_space_match.where(
                    ~face_space_match.ENCUMBERED
                ).dropna(how = "all").FMID
            )
        ).dropna(how = "all")
        
        faces, spaces, face_space_match = match(
            criteria,  
            faces_to_match,
            spaces_to_match, 
            i,
            face_space_match,
            verbose
        )
        print(" - match() returned", 
            str(face_space_match.where(
                face_space_match.stage_matched == i
            ).dropna(how = "all").shape[0]), 
            " assignment matches and",
            str(face_space_match.where(
                face_space_match.TEMP_STAGE_MATCHED == i
            ).dropna(how = "all").shape[0]),
            " attachment matches."
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


# =============================================================================
# Calls full_run() for different AR and AC populations, merges the results 
# and returns three files used as input into the analytics process
# =============================================================================
def split_population_full_runs(
        faces,
        spaces,
        match_phases,
        rmk_codes,
    ):
    # Full run for AR AGR ABL faces and spaces
    print("### Performing split population full runs ###")
    print("#-- AGR Above the line                    --#")
    agr_faces = faces.where(faces.RCC == "AGR").dropna(how = "all")
    agr_spaces = spaces.where(spaces.RMK1 == "92").dropna(how = "all")
    for i in range(2, 5):
        agr_spaces = agr_spaces.append(
            spaces.where(spaces["RMK" + str(i)] == "92").dropna(how = "all")        
        )
    agr_spaces = agr_spaces.append(
        spaces.where(spaces.IS_TEMPLET).dropna(how = "all")        
    )
    
    agr_match_phases = match_phases
    
    print("ALERT: BYPASSING AGE MATTERS RESTRICTION FOR AGR MATCHING!!!")
    agr_match_phases.AGE_MATTERS = False #For FY22 runs only
    
    unmatched_agr_faces, remaining_agr_spaces, agr_face_space_match = full_run(
        match_phases,
        agr_faces,
        agr_spaces,
        include_only_cmds = [],
        exclude_cmds = [],
        exclude_rmks = []
    )
    
    #Full run for AR IMA ABL faces and spaces
    print("#-- IMA Above the line                    --#")
    ima_faces = faces.where(faces.RCC == "IMA").dropna(how = "all")

    ima_spaces = spaces.where(spaces.IS_TEMPLET).dropna(how = "all")
    ima_spaces = ima_spaces.where(
        ~ima_spaces.FMID.isin(
            agr_face_space_match.where(
                agr_face_space_match.ENCUMBERED == 1
            ).dropna(how = "all").FMID
        )        
    ).dropna(how = "all")
    #347596
    ima_spaces = ima_spaces.append(spaces.where(spaces.RMK1 == "MD").dropna(how = "all"))
    ima_spaces = ima_spaces.append(spaces.where(spaces.RMK1 == "DM").dropna(how = "all"))
    ima_spaces = ima_spaces.append(spaces.where(spaces.RMK1 == "MQ").dropna(how = "all"))
    for i in range(2, 5):
        ima_spaces = ima_spaces.append(
            spaces.where(spaces["RMK" + str(i)] == "MD").dropna(how = "all")        
        )
        ima_spaces = ima_spaces.append(
            spaces.where(spaces["RMK" + str(i)] == "DM").dropna(how = "all")        
        )
        ima_spaces = ima_spaces.append(
            spaces.where(spaces["RMK" + str(i)] == "MQ").dropna(how = "all")        
        )
    #349296
    ima_match_phases = match_phases
    
    print("ALERT: BYPASSING AGE MATTERS RESTRICTION FOR IMA MATCHING!!!")
    ima_match_phases.AGE_MATTERS = False #For FY22 runs only
    unmatched_ima_faces, remaining_ima_spaces, ima_face_space_match = full_run(
        ima_match_phases,
        ima_faces,
        ima_spaces,
        include_only_cmds = [],
        exclude_cmds = [],
        exclude_rmks = []
    )
    
    #Merge IMA matches into AGR matches
    agr_ima_face_space_match = ima_face_space_match.where(
        ima_face_space_match.ENCUMBERED
    ).dropna(how = "all").append(
        agr_face_space_match.where(
            agr_face_space_match.ENCUMBERED        
        ).dropna(how = "all")
    )
    
    print("#-- All Active Component                    --#")
    # Full run for AC faces and spaces:
    
    #Drop spaces encumbered by IMA and AGR:
    ac_run_spaces = spaces.where(
        ~spaces.FMID.isin(agr_ima_face_space_match.FMID)
    ).dropna(how = "all")
    
    unmatched_faces, remaining_spaces, ac_face_space_match = full_run(
        match_phases, 
        faces, 
        ac_run_spaces,
        include_only_cmds = [],
        exclude_cmds = ["AR"],
        exclude_rmks = rmk_codes.where(rmk_codes.NO_AC)
            .dropna(how = "all")
            .index.to_list()
    )
                
    #Merge AGR and IMA matches into ac matches
    ac_agr_ima_face_space_match = ac_face_space_match.append(
        agr_ima_face_space_match        
    )
    
    usar_spaces = spaces.where(spaces.COMPO == 3).dropna(how = "all") 
    usar_spaces = usar_spaces.where(
        ~usar_spaces.FMID.isin(ac_agr_ima_face_space_match.where(
                ac_agr_ima_face_space_match.ENCUMBERED == 1
            ).dropna(how = "all").FMID
        )
    ).dropna(how = "all") 
    # Full run for AR faces and spaces:
    print("#-- All Remaining Reserve Component        --#")
    unmatched_faces, remaining_spaces, ar_face_space_match = full_run(
        match_phases, 
        faces.where(
            ~faces.SSN_MASK.isin(ac_agr_ima_face_space_match.SSN_MASK)        
        ).dropna(how = "all"), 
        usar_spaces,
        include_only_cmds = ["AR"],
        exclude_cmds = [],
        exclude_rmks = ["89"],
        verbose = False
    )
    
    #Merge AR matches into AC and AGR matches
    face_space_match = ac_agr_ima_face_space_match.where(
        ~ac_agr_ima_face_space_match.FMID.isin(ar_face_space_match.FMID)
    ).dropna(how = "all").append(ar_face_space_match) 
    
    attach_face_space_match = face_space_match.where(
        face_space_match.TEMP_STAGE_MATCHED > 0
    ).dropna(how = "all")[["FMID", "SSN_MASK", "TEMP_STAGE_MATCHED", "ENCUMBERED"]]
    attach_face_space_match = attach_face_space_match.rename(columns = {
        "TEMP_STAGE_MATCHED" : "stage_matched"        
    })
    
    return unmatched_faces, remaining_spaces, face_space_match, attach_face_space_match 