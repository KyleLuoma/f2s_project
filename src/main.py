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
    global drrsa, acom_spaces, faces, match_phases, rank_grade_xwalk
    
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
    
    for i in range(1, match_phases.shape[0] + 1):
        match(match_phases, 
              acom_spaces.where(acom_spaces.UIC == "WJKCAA").dropna(how = "all"), 
              faces.where(faces.UIC == "WJKCAA").dropna(how = "all"), i)
    
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
def match(criteria, spaces, faces, stage):
    print("Matching stage ", str(stage))
    faces_index_labels = []
    spaces_index_labels = []
    
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
            #Index matching:
            sub_spaces = spaces.loc[row.Index]
            #Drop already matched positions:
            sub_spaces = sub_spaces.where(sub_spaces.SSN_MASK == 0)
            
            if(VERBOSE):
                print("Faces index", row.Index, "type:", type(row.Index))
                print("Joined tuple:", row.Index + (sub_spaces.iloc[0].FMID,))
                
            spaces.at[row.Index + (sub_spaces.iloc[0].FMID,), "SSN_MASK"] = row.SSN_MASK
            spaces.at[row.Index + (sub_spaces.iloc[0].FMID,), "stage_matched"] = stage
            stage_matched += 1
            
        except Exception:
            if(VERBOSE):
                print(Exception.with_traceback)
            exception_count += 1
        
        if(counter % 100 == 0):
            print(".", end = "")
        if(counter % 5000 == 0):
            print("\nrecords reviewed:", str(counter), 
                  " Matched:", str(stage_matched),
                  " Exceptions:", str(exception_count))
            
        
    #Attempt to locate a matching vacant position based on provided criteria
    #If match: pop row from faces, add SSN mask to space match
    #If no match: iterate to next faces row
    print("Match", str(stage))

if (__name__ == "__main__"): main()
