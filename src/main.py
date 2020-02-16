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

LOAD_AND_PROCESS = False
VERBOSE = False
EXPORT_F2S = True
EXPORT_UNMATCHED = True

def main():
    global drrsa, acom_spaces, faces, match_phases, rank_grade_xwalk, test_faces, test_spaces, face_space_match
    
    if(LOAD_AND_PROCESS):
        rank_grade_xwalk = load_data.load_rank_grade_xwalk()
        
        #drrsa = load_data.load_drrsa_file()
        
        acom_spaces = process_data.process_aos_billet_export(
                load_data.load_army_command_aos_billets()
                )
        
        faces = process_data.process_emilpo_assignments(
                load_data.load_emilpo(), 
                rank_grade_xwalk)
        
        match_phases = load_data.load_match_phases()
        acom_spaces = process_data.categorical_spaces(acom_spaces)
        faces = process_data.categorical_faces(faces)
            
    unmatched_faces, remaining_spaces, face_space_match = full_run(
            match_phases, 
            faces, 
            acom_spaces[["UIC_PAR_LN","UIC", "PARNO", "FMID", "LN", "GRADE", 
                       "POSCO", "SQI1", "stage_matched", "SSN_MASK",
                       "ASI_LIST", "RMK_LIST"]])
    
    if(EXPORT_F2S): face_space_match.to_csv("..\export\\faces_spaces_match.csv")
    if(EXPORT_UNMATCHED): unmatched_faces.co_csv("..\export\unmatched_faces.csv")
    
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
    
    """
    face_space_match = spaces[["FMID", "SSN_MASK", "stage_matched"]]
    face_space_match.SSN_MASK = face_space_match.SSN_MASK.astype("str")
    face_space_match["F_PLN"] = ""
    face_space_match["S_PLN"] = ""
    """
    
    face_space_match.set_index(face_space_match.FMID, drop = False, inplace = True)
    face_space_match.sort_index()
    
    #Analyze match criteria and set multi index array for spaces and faces files
    if(criteria.UIC.loc[stage]):
        faces_index_labels.append("UIC")
        spaces_index_labels.append("UIC")
        
    if(criteria.PARENT_UIC.loc[stage]):
        faces_index_labels.append("PARENT_UIC_CD")
        spaces_index_labels.append("UIC")
        
    if(criteria.PARNO.loc[stage]):
        faces_index_labels.append("PARNO")
        spaces_index_labels.append("PARNO")
        
    if(criteria.LN.loc[stage]):
        faces_index_labels.append("LN")
        spaces_index_labels.append("LN")
    
    if(criteria.GRADE.loc[stage] & 
       (criteria.GRADE_VAR_UP.loc[stage] == 0 & criteria.GRADE_VAR_DN.loc[stage] == 0)):
        faces_index_labels.append("GRADE")
        spaces_index_labels.append("GRADE")
        
    if(criteria.PRI_MOS.loc[stage] and not criteria.ALT_MOS.loc[stage]):
        faces_index_labels.append("MOS_AOC1")
        spaces_index_labels.append("POSCO")
        
    if(criteria.ALT_MOS.loc[stage] and not criteria.PRI_MOS.loc[stage]):
        faces_index_labels.append("MOS_AOC2")
        spaces_index_labels.append("POSCO")
        
    if(criteria.SQI.loc[stage]):
        spaces_index_labels.append("SQI1")  
        
    
    
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
    if(stage == 1): #UIC, PARNO, LN Matching custom implementation
                
        face_list = sorted(faces[["UIC_PAR_LN", "SSN_MASK"]].values.tolist())
        space_list = sorted(spaces[["UIC_PAR_LN", "FMID"]].values.tolist())
        
        f_total = len(face_list)
        
        while(f_ix < f_total):
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
            
    if(stage in [2, 3, 4, 5, 6, 7, 8]): #Perfect MOS, GRADE match in PARA (2) and UIC (3)        
        face_list = sorted(faces[faces_index_labels].values.tolist())
        space_list = sorted(spaces[spaces_index_labels].values.tolist())
        
        f_total = len(face_list)
        s_total = len(space_list)
        
        compare_ix = len(faces_index_labels) - 1   #Number of columns for comparison
        fmid_ix = len(spaces_index_labels) - 1     #Column index # for FMID
        mask_ix = len(faces_index_labels)  - 1     #Column index # for SSN MASK
        
        print("Comparing faces", faces_index_labels[0:compare_ix], 
              "to", spaces_index_labels[0:compare_ix])
        
        if(stage == 8): 
            print(faces.columns)
            print("compare_ix:", str(compare_ix))
            print("fmid_ix:", str(fmid_ix))
            print("mask_ix:", str(mask_ix))
            
        
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
