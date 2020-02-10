# -*- coding: utf-8 -*-
"""
Created on Mon Feb 10 13:45:26 2020

@author: LuomaKR
"""
import pandas as pd
import numpy as np
import math
import load_data
import utility

def main():
    print("You need to write some code here")
    criteria = pd.Series(data = ["PARA", "LN"])
    
    drrsa = load_drrsa_file()
    acom_spaces = load_army_command_aos_billets()
    faces = load_emilpo()
    
"""
" Core matching function that iterates through available spaces and aligns
" faces based on matching criteria passed in the criteria list
" Parameters: Pandas series of matching criteria, 
              Pandas DF of available spaces, 
              Pandas DF of available faces
"""
def match(criteria, spaces, faces):
    
    

if (__name__ == "__main__"): main()
