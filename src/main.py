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
    print("You need to write some code here")
    
    drrsa = load_data.load_drrsa_file()
    acom_spaces = load_data.load_army_command_aos_billets()
    faces = load_data.load_emilpo()
    match_phases = load_data.load_match_phases()
    rank_grade_xwalk = load_data.load_rank_grade_xwalk()
    
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

if (__name__ == "__main__"): main()
