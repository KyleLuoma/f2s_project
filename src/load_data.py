# -*- coding: utf-8 -*-
"""
Created on Tue Nov  5 12:47:39 2019
@author: LuomaKR
Module to load data as Pandas objects 
Sources include DRRSA UIC file and AOS tree export
"""

import pandas as pd
import numpy as np

def load_uic_hd_map():
    return pd.read_csv("../data/uic_hd_map/UIC_HD_MAP.csv")

""" Retrieve the DRRSA UIC / Location file """
def load_drrsa_file():
    return pd.read_csv("../data/DRRSA_Data_20200114.csv")

""" Retrieve partitioned command tree and return a single DF of all Army Commands"""
def load_and_append_warcff_billet_export(num_partitions):
    cmd_uic = "WARCF"
    file_path = "../data/aos_billet_export/WARCFF/"
    file_name_end = " C2 BILLET EXPORT 4-6-2021.xlsx"
    
    cmd_billet_export = pd.read_excel(
            file_path + cmd_uic + str(1) + file_name_end,
            header = 2,
            dtype = {
                'PARENT_PARNO': str,    
                'FMID': str,
                'PERLN': str,
                'GRADE': str,
                'POSCO': str,
                'ASI1': str,
                'ASI2': str,
                'ASI3': str,
                'ASI4': str,
                'SQI1': str,
                'RMK1': str,
                'RMK2': str,
                'RMK3': str,
                'RMK4': str,
                'AMSCO': str,
                'MDEP': str,
                'BRANCH': str,
                'CTYPE': str
                },
            skipfooter = 1
            )
    
    for i in range (2, num_partitions + 1):
        cmd_billet_export = cmd_billet_export.append(
                pd.read_excel(
                        file_path + cmd_uic + str(i) + file_name_end,
                        header = 2,
                        dtype = {
                            'PARENT_PARNO': str,    
                            'FMID': str,
                            'PERLN': str,
                            'GRADE': str,
                            'POSCO': str,
                            'ASI1': str,
                            'ASI2': str,
                            'ASI3': str,
                            'ASI4': str,
                            'SQI1': str,
                            'RMK1': str,
                            'RMK2': str,
                            'RMK3': str,
                            'RMK4': str,
                            'AMSCO': str,
                            'MDEP': str,
                            'BRANCH': str,
                            'CTYPE': str
                            },
                        skipfooter = 1
                        )
                )
    return cmd_billet_export

""" Retrieve AOS Billet Export for USAR and AC """
def load_army_command_aos_billets():
    return load_and_append_warcff_billet_export(4).append(
                pd.read_excel(
                    "../data/aos_billet_export/W00EFF/W00EFF C2 BILLET EXPORT 4-6-2021.xlsx",
                    header = 2,
                    dtype = {
                        'PARENT_PARNO': str,    
                        'FMID': str,
                        'PERLN': str,
                        'GRADE': str,
                        'POSCO': str,
                        'ASI1': str,
                        'ASI2': str,
                        'ASI3': str,
                        'ASI4': str,
                        'SQI1': str,
                        'RMK1': str,
                        'RMK2': str,
                        'RMK3': str,
                        'RMK4': str,
                        'AMSCO': str,
                        'MDEP': str,
                        'BRANCH': str,
                        'CTYPE': str
                        },
                    skipfooter = 1
                )
            ).append(
                pd.read_excel(
                    "../data/aos_billet_export/WSTAFF/WSTAFF C2 BILLET EXPORT 4-6-2021.xlsx",
                    header = 2,
                    dtype = {
                        'PARENT_PARNO': str,    
                        'FMID': str,
                        'PERLN': str,
                        'GRADE': str,
                        'POSCO': str,
                        'ASI1': str,
                        'ASI2': str,
                        'ASI3': str,
                        'ASI4': str,
                        'SQI1': str,
                        'RMK1': str,
                        'RMK2': str,
                        'RMK3': str,
                        'RMK4': str,
                        'AMSCO': str,
                        'MDEP': str,
                        'BRANCH': str,
                        'CTYPE': str
                        },
                    skipfooter = 1
                )
            )

""" Retrieve EMILPO position level assignment file """
def load_emilpo():
    return pd.read_csv(
            "../data/emilpo/EMILPO_ASSIGNMENTS_4-6-20.csv",
            dtype = {
                    "PARNO": str,
                    "LN": str,
                    "SSN_MASK_HASH": str,
                    "MIL_POSN_RPT_NR": str,
                    "MOS_AOC1": str,
                    "MOS_AOC2": str,
                    "MOS_AOC3": str,
                    "MOS_AOC4": str,
                    "MOS_AOC5": str,
                    "MOS_AOC6": str,
                    "MOS_AOC7": str,
                    "MOS_AOC8": str,
                    "MOS_AOC9": str,
                    "MOS_AOC10": str,
                    "MOS_AOC11": str,
                    "MOS_AOC12": str,
                    "MOS_AOC13": str,
                    "SQI1": str,
                    "SQI2": str,
                    "SQI3": str,
                    "SQI4": str,
                    "SQI5": str,
                    "SQI6": str,
                    "SQI7": str,
                    "SQI8": str,
                    "SQI9": str,
                    "SQI10": str,
                    "SQI11": str,
                    "SQI12": str,
                    "SQI13": str,
                    "SQI14": str,
                    "SQI15": str,
                    "SQI16": str,
                    "ASI1": str,
                    "ASI2": str,
                    "ASI3": str,
                    "ASI4": str,
                    "ASI5": str,
                    "ASI6": str,
                    "ASI7": str,
                    "ASI8": str,
                    "ASI9": str,
                    "ASI10": str,
                    "ASI11": str,
                    "ASI12": str,
                    "ASI13": str,
                    "ASI14": str
                    }
            ).rename(columns = {"SSN_MASK_HASH" : "SSN_MASK"})

""" Retreive match phases file """
def load_match_phases():
    return pd.read_csv("../data/match_phases.csv").set_index("STAGE")

""" Retrieve rank grade crosswalk file """
def load_rank_grade_xwalk():
    return pd.read_csv("../data/rank_grade_xwalk.csv").set_index("RANK")

""" Retrieve grade mismatch crosswalk file """
def load_grade_mismatch_xwalk():
    return pd.read_csv("../data/grade_mismatch_xwalk.csv").set_index("GRADE")

""" Retrieve OUID to UIC mapping file """
def load_ouid_uic_xwalk():
    return pd.read_csv("../data/OUID_UIC_FY21.CSV")

#emilpo: (17,29,30,31,32,46,47,48,49,50,51)
    
