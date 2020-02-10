# -*- coding: utf-8 -*-
"""
Created on Tue Nov  5 12:47:39 2019
@author: LuomaKR
Module to load data as Pandas objects 
Sources include DRRSA UIC file and AOS tree export
"""

import pandas as pd

""" Retrieve the DRRSA UIC / Location file """
def load_drrsa_file():
    return pd.read_csv("../data/DRRSA_Data_20200114.csv")

""" Retrieve AOS Billet Export for Army Commands """
def load_army_command_aos_billets():
    return pd.read_csv("../data/AOS_ARMY_COMMANDS_FY21.csv")

""" Retrieve EMILPO position level assignment file """
def load_emilpo():
    return pd.read_csv("../data/EMILPO_ASI_SQI_MOSAOC_ASSIGNMENTS_9FEB2020_WITH_KEY_MASK_NO_SSN.csv")

""" Retreive match phases file """
def load_match_phases():
    return pd.read_csv("../data/match_phases.csv").set_index("STAGE")

""" Retrieve rank grade crosswalk file """
def load_rank_grade_xwalk():
    return pd.read_csv("../data/rank_grade_xwalk.csv").set_index("RANK")