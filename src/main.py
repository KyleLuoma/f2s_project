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

def main():
    global drrsa, acom_spaces, faces, match_phases, rank_grade_xwalk, test_faces, test_spaces, face_space_match
    
    if(LOAD_AND_PROCESS):
        rank_grade_xwalk = load_data.load_rank_grade_xwalk()
        
        drrsa = load_data.load_drrsa_file()
        
        acom_spaces = process_data.process_aos_billet_export(
                load_data.load_army_command_aos_billets()
                )
        
        faces = process_data.process_emilpo_assignments(
                load_data.load_emilpo(), 
                rank_grade_xwalk)
        
        match_phases = load_data.load_match_phases()
        acom_spaces = process_data.categorical_spaces(acom_spaces)
        faces = process_data.categorical_faces(faces)
        
    test_faces = faces.head(500).copy()
    
    test_faces, test_spaces, face_space_match = test_stage(match_phases, test_faces, acom_spaces, 1)

    
def full_run(criteria, faces, spaces):
    print("Executing full matching run")
    for i in range(1, match_phases.shape[0] + 1):
        print(" - Calling match() for stage", str(i))
        faces, spaces, face_space_match = match(match_phases,  
              faces.where(faces.stage_matched == 0).dropna(how = "all"),
              spaces.where(acom_spaces.stage_matched == 0).dropna(how = "all"), i)
        print(" - match() returned," str(face_space_match.shape[0], " matches."))
        
        print(" - indexing spaces on FMID and sorting")
        spaces.set_index(spaces.FMID, inplace = True, drop = False)
        spaces.sort_index()
        
        print(" - indexing faces on FMID and sorting")
        faces.set_index(faces.SSN_MASK, inplace = True, drop = False)
        faces.sort_index()
        
        print(" - updating spaces and faces SSN MASK and stage matched columns")
    
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
def match(criteria, faces, spaces, stage):
    print("Matching stage ", str(stage), "Faces File Shape:", faces.shape)
    faces_index_labels = []
    spaces_index_labels = []
    
    face_space_match = spaces[["FMID", "SSN_MASK", "stage_matched"]]
    face_space_match.set_index(face_space_match.FMID, drop = False, inplace = True)
    face_space_match.sort_index()
    
    #Analyze match criteria and set multi index array for spaces and faces files
    if(criteria.UIC.loc[stage]):
        faces_index_labels.append("UIC")
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
        spaces_index_labels.append("POSCO")
        
    if(criteria.SQI.loc[stage]):
        spaces_index_labels.append("SQI1")
        
    spaces_index_labels.append("FMID")
        
    #Set the multi index for spaces and faces files
    faces = faces.reset_index(drop = True)
    spaces = spaces.reset_index(drop = True)    

    faces_index = faces[faces_index_labels]
    faces = faces.set_index(pd.MultiIndex.from_frame(faces_index))
    
    spaces_index = spaces[spaces_index_labels]
    spaces = spaces.set_index(pd.MultiIndex.from_frame(spaces_index))
    
    print("Spaces multi-index set to: ", end = "")
    print(spaces.index)

    print("\nFaces multi-index set to: ", end = "")
    print(faces.index)
    
    counter = 0
    stage_matched = 0
    exception_count = 0
    
    print("Matching stage ", str(stage))
    #Iterate through every person in faces file
    for row in faces.itertuples():
        counter += 1
        try:
            print(spaces.loc[row.Index].iloc[0].FMID, row.SSN_MASK)
            print(row)
            face_space_match.at[spaces.loc[row.Index].iloc[0].FMID, "stage_matched"] = stage
            face_space_match.at[spaces.loc[row.Index].iloc[0].FMID, "SSN_MASK"] = row.SSN_MASK
            stage_matched += 1
            
        except KeyError:
            ##print(KeyError)
            exception_count += 1
        except Exception:
            print("Unknown Exception")
            exception_count += 1
        
        if(counter % 100 == 0):
            print(".", end = "")
        if(counter % 5000 == 0):
            print("\nrecords reviewed:", str(counter), 
                  " Matched:", str(stage_matched),
                  " Exceptions:", str(exception_count))
            
    print("STAGE", str(stage), "Total records reviewed:", str(counter), 
                  " Matched:", str(stage_matched),
                  " Exceptions:", str(exception_count))
    
    return faces, spaces, face_space_match

            
        
    #Attempt to locate a matching vacant position based on provided criteria
    #If match: pop row from faces, add SSN mask to space match
    #If no match: iterate to next faces row
    print("Match", str(stage))

if (__name__ == "__main__"): main()
