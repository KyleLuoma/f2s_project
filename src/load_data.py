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
    return pd.read_csv("./data/DRRSA_Data_20200114.csv")

""" Retrieve AOS Billet Export for Army Commands """
def load_army_command_aos_billets():
    return pd.read_csv()

""" Retrieve EMILPO position level assignment file """
def load_emilpo():
    return pd.read_csv("./data/emilpo_assigned_excess_01142020.csv").append(
            pd.read_csv("./data/rcms_assigned_excess_01132020.csv")
            )

