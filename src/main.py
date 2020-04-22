# -*- coding: utf-8 -*-
"""
Created on Mon Feb 10 13:45:26 2020

@author: LuomaKR
"""
import pandas as pd
from pandas.api.types import CategoricalDtype
import numpy as np
#import math
import load_data
import process_data
import utility
import pyodbc as db
import analytics.cmd_match_metrics_table
#import sqlalchemy

LOAD_MATCH_PHASES = False
LOAD_AND_PROCESS = False
VERBOSE = False
EXPORT_F2S = False
EXPORT_UNMATCHED = False
UPDATE_CONNECTIONS = True

def main():
    global drrsa, spaces, faces, match_phases, rank_grade_xwalk, test_faces 
    global test_spaces, face_space_match, unmatched_faces, unmatched_analysis
    global grade_mismatch_xwalk, all_faces_to_matched_spaces, aos_ouid_uic_xwalk 
    global rmk_codes, uic_hd_map, cmd_description_xwalk, cmd_match_metrics_table
        
    if(LOAD_MATCH_PHASES):
        match_phases = load_data.load_match_phases()
    if(LOAD_AND_PROCESS):
        uic_hd_map = load_data.load_uic_hd_map()
        rank_grade_xwalk = load_data.load_rank_grade_xwalk()
        grade_mismatch_xwalk = load_data.load_grade_mismatch_xwalk()
        aos_ouid_uic_xwalk = load_data.load_ouid_uic_xwalk()
        rmk_codes = load_data.load_rmk_codes()
        cmd_description_xwalk = load_data.load_cmd_description_xwalk()
        
        drrsa = load_data.load_drrsa_file()
        
        spaces = load_data.load_army_command_aos_billets()
        spaces = process_data.process_aos_billet_export(spaces)
        spaces = process_data.add_expected_hsduic(spaces, uic_hd_map, "NA")
        spaces = process_data.add_drrsa_data(spaces, drrsa)
        spaces = process_data.categorical_spaces(spaces)

        faces = process_data.process_emilpo_assignments(
                load_data.load_emilpo(), 
                rank_grade_xwalk,
                grade_mismatch_xwalk)
        faces = process_data.add_drrsa_data(faces, drrsa)
        faces = process_data.check_uic_in_aos(faces, aos_ouid_uic_xwalk, "DRRSA_ADCON")
        faces = process_data.add_templet_columns(faces)
        faces = process_data.add_expected_hsduic(faces, uic_hd_map, "None")
        faces = process_data.categorical_faces(faces)
            
    # Full run for AC faces and spaces:
    unmatched_faces, remaining_spaces, face_space_match = full_run(
            match_phases, 
            faces, 
            spaces[[
                "UIC_PAR_LN","UIC", "LDUIC", "PARNO", "FMID", "LN", "GRADE", 
                "POSCO", "SQI1", "stage_matched", "SSN_MASK",
                "ASI_LIST", "RMK_LIST", "RMK1", "RMK2", "RMK3", "RMK4", 
                "DRRSA_ASGMT", "S_DATE", "T_DATE"
            ]],
            include_only_cmds = [],
            exclude_cmds = ["AR"],
            exclude_rmks = rmk_codes.where(rmk_codes.NO_AC)
                .dropna(how = "all")
                .index.to_list()
            )
    
    all_faces_to_matched_spaces = face_space_match_analysis(faces, face_space_match, spaces)
    all_faces_to_matched_spaces = process_data.add_match_phase_description(all_faces_to_matched_spaces, match_phases)
    all_faces_to_matched_spaces = diagnose_mismatch_in_target(all_faces_to_matched_spaces, unmatched_faces, spaces)
    
    cmd_metrics = analytics.cmd_match_metrics_table.make_cmd_f2s_metric_df(all_faces_to_matched_spaces)
    
    if(EXPORT_F2S): 
        face_space_match.to_csv("..\export\\face_space_matches" + utility.get_file_timestamp() + ".csv")
        all_faces_to_matched_spaces.to_csv("..\export\\all_faces_to_matched_spaces" + utility.get_file_timestamp() + ".csv")                
    if(EXPORT_UNMATCHED): 
        unmatched_faces.to_csv("..\export\\unmatched_faces" + utility.get_file_timestamp() + ".csv")
    if(UPDATE_CONNECTIONS):
        all_faces_to_matched_spaces.to_csv("..\export\\for_connections\\all_faces_to_matched_space_latest.csv")
        cmd_metrics.to_csv("..\export\\for_connections\\cmd_metrics.csv")
        
def reload_spaces():
    spaces = load_army_command_aos_billets()
    spaces = process_aos_billet_export(spaces)
    spaces = add_expected_hsduic(spaces, uic_hd_map, "NA")
    spaces = add_drrsa_data(spaces, drrsa)
    spaces = categorical_spaces(spaces)
    return spaces

def face_space_match_analysis(faces, face_space_match, spaces):
    #Export a join of eMILPO and AOS using face_space_match to connect
    all_faces_to_matched_spaces = faces[["SSN_MASK", "UIC", "PARENT_UIC_CD", "STRUC_CMD_CD",
                           "PARNO", "LN", "MIL_POSN_RPT_NR", "DUTY_ASG_DT","RANK_AB", "GRADE",
                           "DRRSA_ADCON", "DRRSA_HOGEO", "DRRSA_ARLOC", "DRRSA_GEOLOCATIONNAME",
                           "DRRSA_ASGMT", "PPA", "DRRSA_ADCON_IN_AOS"
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
    all_faces_to_matched_spaces = all_faces_to_matched_spaces.reset_index().set_index("FMID").join(
        spaces.reset_index(
            drop = True
        ).set_index(
            "FMID"
        )[["UIC", "PARNO", "LN", "PARENT_TITLE", "GRADE", "POSCO", "S_DATE", "T_DATE"]],
        lsuffix = "_emilpo",
        rsuffix = "_aos"
        )
    return all_faces_to_matched_spaces


def diagnose_mismatch_in_target(target, unmatched_faces, spaces):
    print("Analyzing unmatched faces")
    unmatched_analysis = unmatched_faces[[
             "SSN_MASK", "UIC", "PARENT_UIC_CD", "STRUC_CMD_CD", "PARNO", "LN", 
             "MIL_POSN_RPT_NR", "RANK_AB", "GRADE", "ASI_LIST", "SQI_LIST", "MOS_AOC_LIST"
        ]
    ]
    target["ADD_UIC_TO_AOS"] = False
    target["CREATE_TEMPLET"] = False
    print(" - Checking if UICs are in AOS")
    target.ADD_UIC_TO_AOS = (~target.UIC_emilpo.isin(spaces.UIC))
    
    print(" - Checking if templets are needed")
    target.CREATE_TEMPLET = target.apply(
        lambda row: True if (not row.ADD_UIC_TO_AOS and row.stage_matched == 0) else False,
        axis = 1
    )
    return target

"""
" Iterates through all rows of match phases and calls the core match function
" for each combination. Eliminates matched spaces and faces with each call to 
" match().
" Parameters: criteria - DF of matching criteria
"             faces - DF eMILPO faces file
"             spaces - DF AOS billet detail file
" Returns: faces - unmatched faces after all runs complete
"          spaces - remaining available spaces
"          face_space_match - DF of FMID-SSN_MASK pairing with stage matched indicator
"""
def full_run(criteria, faces, spaces, include_only_cmds = [], exclude_cmds = [], exclude_rmks = []):
    print("Executing full matching run")
    face_space_match = spaces.copy()[["FMID", "SSN_MASK", "stage_matched"]]
    face_space_match.SSN_MASK = face_space_match.SSN_MASK.astype("str")
    rmks_excluded = False
    
    if(len(include_only_cmds) > 0):
        print(" - for commands:", include_only_cmds)
        spaces = spaces.where(spaces.DRRSA_ASGMT.isin(include_only_cmds)).dropna(how = "all")
        faces = faces.where(faces.STRUC_CMD_CD.isin(include_only_cmds)).dropna(how = "all")
        
    if(len(exclude_cmds) > 0):
        print(" - excluding commands:", exclude_cmds)
        spaces = spaces.where(~spaces.DRRSA_ASGMT.isin(exclude_cmds)).dropna(how = "all")
        faces = faces.where(~faces.STRUC_CMD_CD.isin(exclude_cmds)).dropna(how = "all")
    
    for i in range(1, match_phases.shape[0] + 1):
        print(" - Calling match() for stage", str(i))
        faces, spaces, face_space_match = match(
            match_phases,  
            faces.where(~faces.SSN_MASK.isin(face_space_match.SSN_MASK)).dropna(how = "all"),
            spaces.where(
                spaces.FMID.isin(
                    face_space_match.where(face_space_match.stage_matched == 0).dropna(how = "all").FMID
                )
            ).dropna(how = "all"), 
            i,
            face_space_match
        )
        print(" - match() returned", 
            str(face_space_match.where(face_space_match.stage_matched == i).dropna(how = "all").shape[0]), 
            " matches."
        )
        #Runs after phase 1 to enable perfect match on all positions regardless of RMK code
        if(not rmks_excluded and len(exclude_rmks) > 0):
            print(" - excluding billets with remarks:", exclude_rmks)
            spaces = spaces.where(~spaces.RMK1.isin(exclude_rmks)).dropna(how = "all")
            spaces = spaces.where(~spaces.RMK2.isin(exclude_rmks)).dropna(how = "all")
            spaces = spaces.where(~spaces.RMK3.isin(exclude_rmks)).dropna(how = "all")
            spaces = spaces.where(~spaces.RMK4.isin(exclude_rmks)).dropna(how = "all")
            rmks_excluded = True
    return faces, spaces, face_space_match
    
"""
" Core matching function that iterates through available spaces and aligns
" faces based on matching criteria passed in the criteria list
" Parameters: Pandas series of matching criteria, 
"             Pandas DF of available spaces, 
"             Pandas DF of available faces,
"             step integer indicating how many previous match runs have occured
" Returns: updated spaces and faces DFs with matched faces removed from faces
" and matching SSN mask added to spaces DF
"""
def match(criteria, faces, spaces, stage, face_space_match):
    print(" - Stage ", str(stage), "Faces File Shape:", faces.shape, "Spaces File Shape:", spaces.shape)
    faces_index_labels = []
    spaces_index_labels = []
    
    face_space_match.set_index(face_space_match.FMID, drop = False, inplace = True)
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
        spaces_index_labels.append("PARNO")
        spaces_index_labels.append("LN")
        
    counter = 0
    stage_matched = 0 #Increase if match found
    exception_count = 0 #Increase if exception thrown
    f_ix = 0 #Faces index
    s_ix = 0 #Spaces index
    faces = faces.reset_index(drop = True)
    spaces = spaces.reset_index(drop = True)
    spaces_index_labels.append("FMID") #This will be the last column in the list
    faces_index_labels.append("SSN_MASK") #This will be the last column in the list
    
    if(stage == 1): #Overwrite the index labels for perfect matching stage 1
        face_list = faces_index_labels = ["UIC_PAR_LN", "SSN_MASK"]
        space_list = spaces_index_labels = ["UIC_PAR_LN", "FMID"]
    
    print(" - Matching stage ", str(stage))
                
    faces["PARNO"] = faces["PARNO"].astype("str")
    faces["LN"] = faces["LN"].astype("str")
    faces["TMP_PARNO"] = faces["TMP_PARNO"].astype("str")
    faces["TMP_LN"] = faces["TMP_LN"].astype("str")
    faces["PARENT_UIC_CD"] = faces["PARENT_UIC_CD"].astype("str")
    spaces["LDUIC"] = spaces["LDUIC"].astype("str")
    spaces["PARNO"] = spaces["PARNO"].astype("str")
    spaces["LN"] = spaces["LN"].astype("str")

    face_list = sorted(faces[faces_index_labels].values.tolist())
    space_list = sorted(spaces[spaces_index_labels].values.tolist())
    
    f_total = len(face_list)
    s_total = len(space_list)
    
    compare_ix = len(faces_index_labels) - 1   #Number of columns for comparison
    fmid_ix = len(spaces_index_labels) - 1     #Column index # for FMID
    mask_ix = len(faces_index_labels)  - 1     #Column index # for SSN MASK
    
    print("  - Comparing faces", faces_index_labels[0:compare_ix], 
          "to", spaces_index_labels[0:compare_ix])            
    
    comparison_count = 0
    while(f_ix < f_total and s_ix < s_total): 
        comparison_count += 1
        if VERBOSE and comparison_count % 2000 == 0: print("    Compared", str(comparison_count), "face_ix:", str(f_ix), "space_ix:", str(s_ix), "Match:", str(stage_matched))
        if(face_list[f_ix][0:compare_ix] == space_list[s_ix][0:compare_ix]):
            face_space_match.at[space_list[s_ix][fmid_ix], "SSN_MASK"] = face_list[f_ix][mask_ix]
            face_space_match.at[space_list[s_ix][fmid_ix], "stage_matched"] = stage
            f_ix += 1
            s_ix += 1
            stage_matched += 1
            counter += 1
        elif(face_list[f_ix][0:compare_ix] < space_list[s_ix][0:compare_ix]):
            f_ix += 1
            exception_count += 1
            counter += 1
        elif(face_list[f_ix][0:compare_ix] > space_list[s_ix][0:compare_ix]):
            s_ix += 1
            
    print(" - STAGE", str(stage), "Total records reviewed:", str(counter), 
                  " Matched:", str(stage_matched),
                  " Exceptions:", str(exception_count))
    
    return faces.where(~faces.SSN_MASK.isin(face_space_match.SSN_MASK)).dropna(how = "all"), spaces, face_space_match

if (__name__ == "__main__"): main()
