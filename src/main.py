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
    
    
    for i in range(1, match_phases.shape[0] + 1):
        match(match_phases, acom_spaces, faces, i)
    
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
    faces_index = []
    spaces_index = []
    
    #Analyze match criteria and set multi index for spaces and faces files
    if(criteria.UIC.loc[stage]):
        print(" - UIC: creating categorical UIC index in faces file")
        faces["UIC"] = faces["UIC"].astype(CategoricalDtype(faces.UIC.unique()))
        faces_index.append("UIC")
        #faces = faces.set_index("UIC", drop = False)
            
        print(" - UIC: creating categorical UIC index in spaces file")
        spaces["UIC"] = spaces["UIC"].astype(CategoricalDtype(spaces.UIC.unique()))
        spaces_index.append("UIC")
        #spaces = spaces.set_index("UIC", drop = False)
        
    if(criteria.PARNO.loc[stage]):
        print(" - PARNO: creating categorical PARNO index in faces file")
        faces["PARNO"] = faces.PARNO.fillna("NONE")
        faces["PARNO"] = faces["PARNO"].astype(CategoricalDtype(faces.PARNO.unique()))
        faces_index.append("PARNO")
        
        print(" - PARNO: creating categorical PARNO index in spaces file")
        spaces["PARNO"] = spaces.PARNO.fillna("NOPAR")
        spaces["PARNO"] = spaces["PARNO"].astype(CategoricalDtype(spaces.PARNO.unique()))
        spaces_index.append("PARNO")
        
    if(criteria.LN.loc[stage]):
        print(" - LN: creating categorical LN index in faces file")
        faces["LN"] = faces.LN.fillna("NONE")
        faces["LN"] = faces["LN"].astype(CategoricalDtype(faces.LN.unique()))
        faces_index.append("LN")
        
        print(" - LN: creating categorical LN index in spaces file")
        spaces["LN"] = spaces.LN.fillna("NOLN")
        spaces["LN"] = spaces["LN"].astype(CategoricalDtype(spaces.LN.unique()))
        spaces_index.append("LN")
    
    if(criteria.GRADE.loc[stage] & 
       (criteria.GRADE_VAR_UP.loc[stage] == 0 & criteria.GRADE_VAR_DN.loc[stage] == 0)):
        print(" - GRADE: creating categorical GRADE index in faces file")
        faces["GRADE"] = faces["GRADE"].astype(CategoricalDtype(faces.GRADE.unique()))
        faces_index.append("GRADE")
        
        print(" - GRADE: creating categorical GRADE index in spaces file")
        spaces["GRADE"] = spaces.GRADE.fillna("NOGR")
        spaces["GRADE"] = spaces["GRADE"].astype(CategoricalDtype(spaces.GRADE.unique()))
        spaces_index.append("GRADE")
        
    if(criteria.PRI_MOS.loc[stage]):
        print(" - Primary MOS_AOC: creating categorical Primary MOS_AOC index in faces file")
        faces["MOS_AOC1"] = faces.MOS_AOC1.fillna("NONE")
        faces["MOS_AOC1"] = faces["MOS_AOC1"].astype(CategoricalDtype(faces.MOS_AOC1.unique()))
        faces_index.append("MOS_AOC1")
        
        print(" - Primary MOS_AOC: creating categorical POSCO index in spaces file")
        spaces["POSCO"] = spaces.POSCO.fillna("NOGR")
        spaces["POSCO"] = spaces["POSCO"].astype(CategoricalDtype(spaces.POSCO.unique()))
        spaces_index.append("POSCO")
    
    #Iterate through every person in faces file
    
            
        
    #Attempt to locate a matching vacant position based on provided criteria
    #If match: pop row from faces, add SSN mask to space match
    #If no match: iterate to next faces row
    print("Match", str(stage))

if (__name__ == "__main__"): main()
