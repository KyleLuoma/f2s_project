# -*- coding: utf-8 -*-
"""
Created on Mon Feb 10 13:45:26 2020

@author: LuomaKR
"""
import pandas as pd
from pandas.api.types import CategoricalDtype
import numpy as np
import math
import load_data
import process_data
import utility

LOAD_MATCH_PHASES = False
LOAD_AND_PROCESS = False
VERBOSE = False
EXPORT_F2S = True
EXPORT_UNMATCHED = True

def main():
    global drrsa, acom_spaces, faces, match_phases, rank_grade_xwalk, test_faces 
    global test_spaces, face_space_match, unmatched_faces, unmatched_analysis
    global grade_mismatch_xwalk, faces_matches, aos_ouid_uic_xwalk, uic_hd_map
    
    if(LOAD_MATCH_PHASES):
        match_phases = load_data.load_match_phases()
    
    if(LOAD_AND_PROCESS):
        uic_hd_map = load_data.load_uic_hd_map()
        rank_grade_xwalk = load_data.load_rank_grade_xwalk()
        grade_mismatch_xwalk = load_data.load_grade_mismatch_xwalk()
        aos_ouid_uic_xwalk = load_data.load_ouid_uic_xwalk()
        
        drrsa = load_data.load_drrsa_file()
        
        acom_spaces = load_data.load_army_command_aos_billets()
        acom_spaces = process_data.process_aos_billet_export(acom_spaces)
        acom_spaces = process_data.add_expected_hsduic(acom_spaces, uic_hd_map)
        
        faces = process_data.process_emilpo_assignments(
                load_data.load_emilpo(), 
                rank_grade_xwalk,
                grade_mismatch_xwalk)
        
        faces = process_data.add_drrsa_data(faces, drrsa)
        faces = process_data.check_uic_in_aos(faces, aos_ouid_uic_xwalk, "DRRSA_ADCON")
        
        acom_spaces = process_data.add_drrsa_data(acom_spaces, drrsa)
        
        acom_spaces = process_data.categorical_spaces(acom_spaces)
        faces = process_data.categorical_faces(faces)
            
    unmatched_faces, remaining_spaces, face_space_match = full_run(
            match_phases, 
            faces, 
            acom_spaces[["UIC_PAR_LN","UIC", "LDUIC", "PARNO", "FMID", "LN", "GRADE", 
                       "POSCO", "SQI1", "stage_matched", "SSN_MASK",
                       "ASI_LIST", "RMK_LIST"]])
    
    unmatched_analysis = analyze_unmatched_faces(face_space_match, 
                                                 unmatched_faces, 
                                                 acom_spaces)
    
    faces_matches = face_space_match_analysis(faces, face_space_match, acom_spaces)
    
    
    if(EXPORT_F2S): 
        face_space_match.to_csv("..\export\\faces_spaces_match" + utility.get_file_timestamp() + ".csv")
        faces_matches.to_csv("..\export\\faces_matches" + utility.get_file_timestamp() + ".csv")
        
    if(EXPORT_UNMATCHED): 
        unmatched_faces.to_csv("..\export\\unmatched_faces" + utility.get_file_timestamp() + ".csv")
        unmatched_analysis.to_csv("..\export\\unmatched_analysis" + utility.get_file_timestamp() + ".csv")


def face_space_match_analysis(faces, face_space_match, acom_spaces):
    #Export a join of eMILPO and AOS using face_space_match to connect
    faces_matches = faces[["SSN_MASK", "UIC", "PARENT_UIC_CD", "STRUC_CMD_CD",
                           "PARNO", "LN", "MIL_POSN_RPT_NR", "RANK_AB", "GRADE",
                           "DRRSA_ADCON", "DRRSA_HOGEO", "DRRSA_ARLOC", "DRRSA_GEOLOCATIONNAME",
                           "DRRSA_ASGMT", "PPA", "DRRSA_ADCON_IN_AOS"
                           ]].set_index("SSN_MASK", drop = True)
    faces_matches = faces_matches.join(
            face_space_match.reset_index(drop = True).set_index("SSN_MASK")[["stage_matched", "FMID"]],
            lsuffix = "_emilpo",
            rsuffix = "_f2s"
            )
    faces_matches = faces_matches.reset_index().set_index("FMID").join(
            acom_spaces.reset_index(drop = True).set_index("FMID")[["UIC", "PARNO", "LN", "PARENT_TITLE", "GRADE", "POSCO"]],
            lsuffix = "_emilpo",
            rsuffix = "_aos"
            )
    return faces_matches


def analyze_unmatched_faces(face_space_match, unmatched_faces, spaces):
    print("Analyzing unmatched faces")
    unmatched_analysis = unmatched_faces[["SSN_MASK", "UIC", "PARENT_UIC_CD",
                                          "STRUC_CMD_CD", "PARNO", "LN", 
                                          "MIL_POSN_RPT_NR", "RANK_AB", 
                                          "GRADE", "ASI_LIST", "SQI_LIST",
                                          "MOS_AOC_LIST"]]

    unmatched_analysis["UIC_IN_AOS"] = False
    unmatched_analysis["PARENT_UIC_IN_AOS"] = False
    unmatched_analysis["NEED_TEMPLET"] = False
    
    print(" - Checking if UICs are in AOS")
    unmatched_analysis.UIC_IN_AOS = unmatched_analysis.UIC.isin(spaces.UIC)
    
    print(" - Checking if PARENT_UICs are in AOS")
    unmatched_analysis.PARENT_UIC_IN_AOS = unmatched_analysis.PARENT_UIC_CD.isin(spaces.UIC)
    
    print(" - Checking if templets are needed")
    unmatched_analysis.NEED_TEMPLET = unmatched_analysis.apply(
            lambda row: True if (row.UIC_IN_AOS or row.PARENT_UIC_IN_AOS) else False,
            axis = 1
            )
    
    return unmatched_analysis

    

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
def full_run(criteria, faces, spaces):
    print("Executing full matching run")
    face_space_match = spaces[["FMID", "SSN_MASK", "stage_matched"]]
    face_space_match.SSN_MASK = face_space_match.SSN_MASK.astype("str")
    face_space_match["F_PLN"] = ""
    face_space_match["S_PLN"] = ""
    
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
              " matches.")
        
    
    return faces, spaces, face_space_match

"""
" Use to test a single matching stage on match()
"""
def test_stage(criteria, faces, spaces, stage):
    return match(match_phases,  
              faces.where(faces.stage_matched == 0).dropna(how = "all"),
              spaces.where(acom_spaces.stage_matched == 0).dropna(how = "all"), stage)
    
    
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
    print("Matching stage ", str(stage), "Faces File Shape:", faces.shape, "Spaces File Shape:", spaces.shape)
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
        pass
    if(criteria.HSDUIC.loc[stage]):
        pass
    
    counter = 0
    stage_matched = 0 #Increase if match found
    exception_count = 0 #Increase if exception thrown
    f_ix = 0 #Faces index
    s_ix = 0 #Spaces index
    faces = faces.reset_index(drop = True)
    spaces = spaces.reset_index(drop = True)
    spaces_index_labels.append("FMID") #This will be the last column in the list
    faces_index_labels.append("SSN_MASK") #This will be the last column in the list
    
    print("Matching stage ", str(stage))
    #Iterate through every person in faces file
    if(stage == 1): #UIC, PARNO, LN Matching custom implementation. Customized because of volumne
                
        face_list = sorted(faces[["UIC_PAR_LN", "SSN_MASK"]].values.tolist())
        space_list = sorted(spaces[["UIC_PAR_LN", "FMID"]].values.tolist())
        
        f_total = len(face_list)
        s_total = len(space_list)
        
        while(f_ix < f_total and s_ix < s_total):
            #If UIC, PARNO and LN match, log match and advance both cursors
            #print(face_list[f_ix][0], space_list[s_ix][0])
            if(face_list[f_ix][0]   == space_list[s_ix][0]):
                #Log the match in the face_space_match file
                face_space_match.at[space_list[s_ix][1], "SSN_MASK"] = face_list[f_ix][1]
                face_space_match.at[space_list[s_ix][1], "stage_matched"] = stage
                face_space_match.at[space_list[s_ix][1], "F_PLN"] = face_list[f_ix][0]
                face_space_match.at[space_list[s_ix][1], "S_PLN"] = space_list[s_ix][0]
                #print(face_list[f_ix][1], space_list[s_ix][1])
                #Advance both cursors
                f_ix += 1
                s_ix += 1
                stage_matched += 1
                counter += 1
            #If faces < spaces position
            elif(face_list[f_ix][0] < space_list[s_ix][0]):
                #print(face_list[f_ix][1])
                #Advance faces cursor
                f_ix += 1
                #This indicaets an unmatched face, so advance the exception_count:
                exception_count += 1
                counter += 1
            #If spaces < faces position
            elif(face_list[f_ix][0] > space_list[s_ix][0]):
                #print("           ", space_list[s_ix][1])
                #Advance spaces cursor
                s_ix += 1
                
            #if(f_ix % 100 == 0): print(".", end = "")
            #if(f_ix % 1000== 0): print("Faces index:", str(f_ix), "Matched:", str(stage_matched))
            
    if(stage > 1): #All the rest of the stages happen here escept templet matching
        faces["PARNO"] = faces["PARNO"].astype("str")
        faces["LN"] = faces["LN"].astype("str")
        faces["PARENT_UIC_CD"] = faces["PARENT_UIC_CD"].astype("str")
        spaces["LDUIC"] = spaces["LDUIC"].astype("str")
        
        face_list = sorted(faces[faces_index_labels].values.tolist())
        space_list = sorted(spaces[spaces_index_labels].values.tolist())
        
        f_total = len(face_list)
        s_total = len(space_list)
        
        compare_ix = len(faces_index_labels) - 1   #Number of columns for comparison
        fmid_ix = len(spaces_index_labels) - 1     #Column index # for FMID
        mask_ix = len(faces_index_labels)  - 1     #Column index # for SSN MASK
        
        print("Comparing faces", faces_index_labels[0:compare_ix], 
              "to", spaces_index_labels[0:compare_ix])            
        
        while(f_ix < f_total and s_ix < s_total):
            
            #print(face_list[f_ix][0:compare_ix], space_list[s_ix][0:compare_ix])
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

            
            
            #if(f_ix % 100 == 0): print(".", end = "")
            #if(f_ix % 1000== 0): print("Faces index:", str(f_ix), "Matched:", str(stage_matched))
            
    print("STAGE", str(stage), "Total records reviewed:", str(counter), 
                  " Matched:", str(stage_matched),
                  " Exceptions:", str(exception_count))
    
    return faces.where(~faces.SSN_MASK.isin(face_space_match.SSN_MASK)).dropna(how = "all"), spaces, face_space_match

            
        
    #Attempt to locate a matching vacant position based on provided criteria
    #If match: pop row from faces, add SSN mask to space match
    #If no match: iterate to next faces row
    print("Match", str(stage))

if (__name__ == "__main__"): main()
