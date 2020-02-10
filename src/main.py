# -*- coding: utf-8 -*-
"""
Created on Mon Feb 10 13:45:26 2020

@author: LuomaKR
"""
import pandas as pd
import numpy as np
import math
import load_data
import process_data
import utility

def main():
    global drrsa, acom_spaces, faces, match_phases, rank_grade_xwalk
    
    rank_grade_xwalk = load_data.load_rank_grade_xwalk()
    drrsa = load_data.load_drrsa_file()
    acom_spaces = load_data.load_army_command_aos_billets()
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
    #Iterate through every person in faces file
    #Attempt to locate a matching vacant position based on provided criteria
    #If match: pop row from faces, add SSN mask to space match
    #If no match: iterate to next faces row
    print("Match", str(stage))

if (__name__ == "__main__"): main()
