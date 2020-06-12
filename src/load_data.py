# -*- coding: utf-8 -*-
"""
Created on Tue Nov  5 12:47:39 2019
@author: LuomaKR
Module to load data as Pandas objects 
Sources include DRRSA UIC file and AOS tree export
"""

import pandas as pd
import numpy as np

WARCFF_PARTITION_COUNT = 5
DATA_PATH = "X:/AOS/master_files"

RCMS_FILE = "USAR_Faces_28May.xlsx"
AOS_FILE_DATE = "6-10-2021"
UIC_TREE_DATE = "6-10-2021"
EMILPO_FILE_DATE = "6-10-20"

def load_uic_hd_map():
    return pd.read_csv(DATA_PATH + "/uic_hd_map/UIC_HD_MAP.csv")

""" Retrieve the DRRSA UIC / Location file """
def load_drrsa_file():
    return pd.read_csv(DATA_PATH + "/drrsa/DRRSA_Data_20200114.csv")

""" Retrieve AF UIC list from Ed """
def load_af_uics():
    return pd.read_excel("../data/command_considerations/AFC_MASTER_UIC_LISTING.xlsx")

""" Retrieve UIC tree files """
def load_uics_from_uic_trees():
    uics = pd.read_excel(
        DATA_PATH + "/aos/uic_tree/WARCFF C2 UIC TREE " + UIC_TREE_DATE + ".xlsx",
        header = 2,
        skipfooter = 1
    )
    uics = uics.append(
        pd.read_excel(
            DATA_PATH + "/aos/uic_tree/WSTAFF C2 UIC TREE " + UIC_TREE_DATE + ".xlsx",
            header = 2,
            skipfooter = 1 
        )
    )
    uics = uics.append(
        pd.read_excel(
            DATA_PATH + "/aos/uic_tree/W00EFF C2 UIC TREE " + UIC_TREE_DATE + ".xlsx",
            header = 2,
            skipfooter = 1    
        )
    )
    uics = uics.drop_duplicates("UIC")["UIC"]
    return uics

""" Retrieve partitioned command tree and return a single DF of all Army Commands"""
def load_and_append_warcff_billet_export(num_partitions):
    cmd_uic = "WARCF"
    file_path = DATA_PATH + "/aos/billet_tree/WARCFF/"
    file_name_end = " C2 BILLET EXPORT " + AOS_FILE_DATE + ".xlsx"
    
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
    return cmd_billet_export.drop_duplicates("FMID")

""" Retrieve AOS Billet Export for USAR and AC """
def load_army_command_aos_billets():
    return load_and_append_warcff_billet_export(WARCFF_PARTITION_COUNT).append(
                pd.read_excel(
                    DATA_PATH + "/aos/billet_tree/W00EFF/W00EFF C2 BILLET EXPORT " + AOS_FILE_DATE + ".xlsx",
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
                    DATA_PATH + "/aos/billet_tree/WSTAFF/WSTAFF C2 BILLET EXPORT " + AOS_FILE_DATE + ".xlsx",
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
                    DATA_PATH + "/aos/billet_tree/WUSAFF/WUSAFF C2 BILLET EXPORT " + AOS_FILE_DATE + ".xlsx",
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
            ).drop_duplicates("FMID")
                
def load_rcms():
    rcms = pd.read_excel(
        DATA_PATH + "/rcmsr/assignments/" + RCMS_FILE,
        dtype = {
            "GFC" : str,
            "GFC 1 Name" : str,
            "Hash" : str,
            "Paragraph" : str,
            "Line Number" : str,
            "Rank" : str,
            "PMOS" : str,
            "SMOS" : str,
            "AMOS" : str,
            "Primary ASI" : str,
            "Secondary ASI" : str,
            "Additional ASI" : str
        }
    ).rename(columns = {
        "Hash" : "SSN_MASK",
        "UPC" : "UIC",
        "Paragraph" : "PARNO",
        "Line Number" : "LN",
        "Position Assigned Date" : "DUTY_ASG_DT",
        "Rank" : "RANK_AB",
        "PMOS" : "MOS_AOC1",
        "SMOS" : "MOS_AOC2",
        "AMOS" : "MOS_AOC3",
        "Primary ASI" : "ASI1",
        "Secondary ASI" : "ASI2",
        "Additional ASI" : "ASI3",
        "Unit Name" : "UNITNAME"
    })
# =============================================================================
#     rcms = rcms.drop(
#         ["UPC", "UNITNAME", "Position", "OVERSTRENGTH", "POSN MOS", "RCC"],
#         axis = 1, 
#         errors = "ignore"
#     )
# =============================================================================
    rcms["UIC_PAR_LN"] = rcms.fillna("").apply(
        lambda row: row.UIC + row.PARNO + row.LN,
        axis = 1
    )
    rcms = rcms.where(~rcms.DUTY_ASG_DT.isna()).dropna(how = "all")
    return rcms.where(~rcms.RANK_AB.isna()).dropna(how = "all")

""" Retrieve EMILPO position level assignment file """
def load_emilpo():
    return pd.read_csv(
            DATA_PATH + "/emilpo/EMILPO_ASSIGNMENTS_" + EMILPO_FILE_DATE + ".csv",
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
    return pd.read_csv(DATA_PATH + "/phases/match_phases.csv").set_index("STAGE")

""" Retrieve rank grade crosswalk file """
def load_rank_grade_xwalk():
    return pd.read_csv("../data/xwalks/rank_grade_xwalk.csv").set_index("RANK")

""" Retrieve grade mismatch crosswalk file """
def load_grade_mismatch_xwalk():
    return pd.read_csv(DATA_PATH + "/xwalks/grade_mismatch_xwalk.csv").set_index("GRADE")

""" Retrieve OUID to UIC mapping file """
def load_ouid_uic_xwalk():
    return pd.read_csv("../data/xwalks/OUID_UIC_FY21.CSV")

""" Retrieve RMK Domain Codes """
def load_rmk_codes():
    rmk_codes = pd.read_excel(
        DATA_PATH + "/domain_codes/Standard Personnel RMK Codes 4-14-2020.xlsx"
    )[["PERMK", "NO_AC", "SPTXT"]].set_index("PERMK")
    return rmk_codes
    
""" Retrieve CMD Codes and Titles Crosswalk """
def load_cmd_description_xwalk():
    cmd_description_xwalk = pd.read_excel(
        DATA_PATH + "/xwalks/CMD_code_CMD_title_xwalk.xlsx"
    )[["CMDCD", "CMDTITLE"]].set_index("CMDCD")
    return cmd_description_xwalk

#emilpo: (17,29,30,31,32,46,47,48,49,50,51)
    
