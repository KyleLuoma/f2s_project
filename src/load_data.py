# -*- coding: utf-8 -*-
"""
Created on Tue Nov  5 12:47:39 2019
@author: LuomaKR
Module to load data as Pandas objects 
Sources include DRRSA UIC file and AOS tree export
"""

import pandas as pd
import numpy as np
import process_data
import aos_unzipper
import utility
import os.path
from os import path

WARCFF_PARTITION_COUNT = 5
DATA_PATH = "F:/aos/master_files"
RCMS_FILE = "USAR_BDE_SELRES_F2S_15MAR_FINAL.xlsx"
APART_FILE = "USAR_AGR_F2S_15MAR_FINAL.XLSX"
RCMS_IMA_FILE = "IMA_hoy96_all_20200505_Hash.xlsx"
AOS_FILE_DATE = "3-10-2021"
UIC_TREE_DATE = "3-10-2021"
EMILPO_FILE_DATE = "3-21-2021"
EMILPO_TEMP_FILE_DATE = "3-21-2021"
DRRSA_FILE_DATE = "3-12-2021"
UIC_ADDRESS_FILE = "textfile_tab_1269578455_UIC_LOCNM_53057.txt"
PHASES_FILE = "match_phases mos mismatch last.csv"

def check_spaces_files_exist():
    print("  - Verifying that all AOS files are available")
    all_exist = True
    missing_files = []
    if not path.exists(
        DATA_PATH + "/aos/uic_tree/WARCFF C2 UIC TREE " + UIC_TREE_DATE + ".xlsx"
    ):
        missing_files.append("WARCFF C2 UIC TREE " + UIC_TREE_DATE + ".xlsx")
        all_exist = False
    if not path.exists(
        DATA_PATH + "/aos/uic_tree/W00EFF C2 UIC TREE " + UIC_TREE_DATE + ".xlsx"
    ):
        missing_files.append("W00EFF C2 UIC TREE " + UIC_TREE_DATE + ".xlsx")
        all_exist = False
    if not path.exists(
        DATA_PATH + "/aos/uic_tree/WUSAFF C2 UIC TREE " + UIC_TREE_DATE + ".xlsx"
    ):
        missing_files.append("WUSAFF C2 UIC TREE " + UIC_TREE_DATE + ".xlsx")
        all_exist = False
    if not path.exists(
        DATA_PATH + "/aos/uic_tree/WSTAFF C2 UIC TREE " + UIC_TREE_DATE + ".xlsx"
    ):
        missing_files.append("WSTAFF C2 UIC TREE " + UIC_TREE_DATE + ".xlsx")
        all_exist = False
    if not path.exists(DATA_PATH + "/drrsa/drrsa " + DRRSA_FILE_DATE + ".xlsx"):
        missing_files.append("drrsa " + DRRSA_FILE_DATE + ".xlsx")
        all_exist = False
    if not path.exists(DATA_PATH + "/aos/billet_tree/W00EFF/W00EFF C2 BILLET EXPORT " + AOS_FILE_DATE + ".xlsx"):
        missing_files.append(
            "W00EFF C2 BILLET EXPORT " + AOS_FILE_DATE + ".xlsx"
        )
        all_exist = False
    if not path.exists(DATA_PATH + "/aos/billet_tree/WSTAFF/WSTAFF C2 BILLET EXPORT " + AOS_FILE_DATE + ".xlsx"):
        missing_files.append("WSTAFF C2 BILLET EXPORT " + AOS_FILE_DATE + ".xlsx")
        all_exist = False
    if not path.exists(DATA_PATH + "/aos/billet_tree/WUSAFF/WUSAFF C2 BILLET EXPORT " + AOS_FILE_DATE + ".xlsx"):
        missing_files.append("WUSAFF C2 BILLET EXPORT " + AOS_FILE_DATE + ".xlsx")
        all_exist = False
    file_name_end = " C2 BILLET EXPORT " + AOS_FILE_DATE + ".xlsx"
    for i in range(1, WARCFF_PARTITION_COUNT + 1):
        if not path.exists(DATA_PATH + "/aos/billet_tree/WARCFF/WARCF" + str(i) + file_name_end):
            missing_files.append("WARCF" + str(i) + file_name_end)
            all_exist = False
    if(all_exist):
        print("     - All AOS files available")
    else:
        print("     - Missing AOS files:", missing_files)
    return all_exist

def check_address_files_exist():
    print("  - Verifying that the UIC address file is available")
    exists = True
    if not path.exists(DATA_PATH + "/uic_location/" + UIC_ADDRESS_FILE):
        exists = False
        print("     - Missing UIC address file", UIC_ADDRESS_FILE)
    return exists

def check_emilpo_files_exist():
    print("  - Verifying that the EMILPO faces file is available")
    exists = True
    if not path.exists(DATA_PATH + "/emilpo/EMILPO_ASSIGNMENTS_" + EMILPO_FILE_DATE + ".csv"):
        exists = False
        print("     - Missing EMILPO file", "EMILPO_ASSIGNMENTS_" + EMILPO_FILE_DATE + ".csv")
    return exists

def check_emilpo_temp_files_exist():
    print("  - Verifying that the EMILPO attachment file is available")
    exists = True
    if not path.exists(DATA_PATH + "/emilpo/temp_assignments/EMILPO_TEMP_ASSIGNMENTS_" + 
        EMILPO_TEMP_FILE_DATE + ".csv"):
        exists = False
        print("     - Missing EMILPO attachment file", "EMILPO_TEMP_ASSIGNMENTS_" + EMILPO_TEMP_FILE_DATE + ".csv")
    return exists

def check_rcms_files_exist():
    print("  - Verifying that the RCMS and APART faces files are available")
    exists = True
    if not path.exists(DATA_PATH + "/rcmsr/assignments/" + RCMS_FILE):
        exists = False
        print("     - Missing RCMS file", RCMS_FILE)
    if not path.exists(DATA_PATH + "/rcmsr/assignments/" + APART_FILE):
        exists = False
        print("     - Missing APART file", APART_FILE)
    return exists

def check_rcms_columns():
    print("  - Verifying column names in the RCMS faces file")
    rcms = pd.read_excel(
        DATA_PATH + "/rcmsr/assignments/" + RCMS_FILE,
        dtype = {
            "GFC1" : str,
            "GFC 1 Name" : str,
            "GFC2" : str,
            "GFC 2 Name" : str,
            "Mask" : str,
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
    )
    rcms_required_columns = [
        "STRUC_CMD_CD", "GFC1", "GFC 1 Name", "GFC2", "GFC 2 Name", "UPC", "UIC",
        "Unit Name", "RCC", "Mask", "Grade", "MPC", "Rank", "Position Assigned Date",
        "Position Number", "Paragraph", "Line Number", "PMOS", "SMOS", "AMOS",
        "Primary ASI", "Secondary ASI", "Additional ASI"
    ]
    print(rcms[rcms_required_columns].head())
    print("   ...all required RCMS columns present.")
    print("  - Verifying column names in the APART faces file")
    apart = pd.read_excel(DATA_PATH + "/rcmsr/assignments/" + APART_FILE
    )
    apart_required_columns = [
        "GFC", "GFC 1 Name", "UPC", "UIC", "Unit Name", "RCC", "Mask", "Grade",
        "MPC", "Rank", "Position Assigned Date", "Position Number", "Paragraph", 
        "Line Number", "APART_POSN_KEY", "PMOS", "SMOS", "AMOS",
        "Primary ASI", "Secondary ASI", "Additional ASI"
    ]
    print(apart[apart_required_columns].head())
    print("   ...all required APART columns present.")
    

""" Drives the spaces file generation process; calls functions in this load_data
module as well as in process_data.py"""
def load_and_process_spaces(uic_hd_map, country_code_xwalk):        
    print(" - Loading and processing spaces files")
    print("  - Loading DRRS-A file dated", DRRSA_FILE_DATE)
    drrsa = load_drrsa_file()
    spaces = load_army_command_aos_billets()
    spaces = process_data.process_aos_billet_export(spaces)
    spaces = process_data.add_expected_hsduic(spaces, uic_hd_map, "NA")
    spaces = process_data.add_drrsa_data(spaces, drrsa)
    spaces = process_data.calculate_age(
        spaces, utility.get_local_time_as_datetime(), "S_DATE", "POSITION"
    )
    spaces = process_data.add_is_templet_column(spaces)
    spaces = process_data.position_level_compo_adjust(spaces) #must run after DRRSA 
    all_uics = load_uics_from_uic_trees().append(
        spaces[["UIC"]].groupby(["UIC"]).count().reset_index()
    ).drop_duplicates(subset = ["UIC"])
    return spaces, drrsa, all_uics

def load_and_process_address_data(country_code_xwalk):
    address_data = load_uic_current_addresses()
    address_data = process_data.process_address_data(
        address_data, country_code_xwalk
    )
    return address_data

""" Drives the faces file generation process; calls functions in this load_data
module as well as in process_data.py"""
def load_and_process_faces(
        LOAD_EMILPO_FACES,
        LOAD_RCMS_FACES,
        PROCESS_COMMAND_CONSIDERATIONS,
        rank_grade_xwalk,
        grade_mismatch_xwalk,
        aos_ouid_uic_xwalk,
        drrsa,
        uic_hd_map,
        emilpo_faces,
        rcms_faces,
        af_uic_list
    ):
    
    if(LOAD_EMILPO_FACES):
        print(" - Loading and processing emilpo file")
        emilpo_faces = process_data.process_emilpo_or_rcms_assignments(
            load_emilpo(), 
            rank_grade_xwalk,
            grade_mismatch_xwalk,
            consolidate = True,
            source = "eMILPO"
        )
        
    if(LOAD_RCMS_FACES):
        print(" - Loading and processing rcms file")
        rcms_faces = load_rcms()
        apart_data = load_apart()
        rcms_faces = process_data.update_para_ln(target = rcms_faces, source = apart_data)
        rcms_faces = process_data.process_emilpo_or_rcms_assignments(
            rcms_faces,
            rank_grade_xwalk,
            grade_mismatch_xwalk, 
            consolidate = False,
            source = "RCMS"
        )
        
    if(LOAD_EMILPO_FACES or LOAD_RCMS_FACES):
        print(" - Merging emilpo and rcms files and calculating additional fields")
        faces = emilpo_faces.append(rcms_faces, ignore_index = True)
        faces = process_data.add_drrsa_data(faces, drrsa)
        faces = process_data.check_uic_in_aos(
            faces, aos_ouid_uic_xwalk, "DRRSA_ADCON"
        )
        faces = process_data.add_templet_columns(faces, parno = "999")
        faces = process_data.add_expected_hsduic(faces, uic_hd_map, "None")
        faces = process_data.calculate_age(
            faces, utility.get_local_time_as_datetime(), 
            "DUTY_ASG_DT", "ASSIGNMENT"
        )
        if(PROCESS_COMMAND_CONSIDERATIONS):
            faces = process_data.convert_cmd_code_for_uic_in_faces(
                faces, af_uic_list, uic_col_name = "UICOD", 
                cmd_col_name = "MACOM"
            )
    return faces


def load_uic_hd_map():
    return pd.read_csv(DATA_PATH + "/uic_hd_map/UIC_HD_MAP.csv")

""" Retrieve the DRRSA UIC / Location file """
def load_drrsa_file():
    drrsa = pd.read_excel(DATA_PATH + "/drrsa/drrsa " + DRRSA_FILE_DATE + ".xlsx")
    drrsa["DRRSA_FILE_DATE"] = DRRSA_FILE_DATE
    return drrsa

""" Retrieve AF UIC list from Ed """
def load_af_uics():
    return pd.read_excel(DATA_PATH + "/command_considerations/AFC_MASTER_UIC_LISTING.xlsx")

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
    uics = uics.append(
        pd.read_excel(
            DATA_PATH + "/aos/uic_tree/WSTAFF C2 UIC TREE " + UIC_TREE_DATE + ".xlsx",
            header = 2,
            skipfooter = 1
        )        
    )
    uics = uics.drop_duplicates("UIC")[["UIC", "UIC_PATH"]]
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
    print("  - Loading AOS billets, this may take a while...")
    print("   - Loading and combining WARCFX billet exports")
    aos_file =  load_and_append_warcff_billet_export(WARCFF_PARTITION_COUNT)
    print("   - Loading W00EFF billet export")
    aos_file = aos_file.append(
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
            )
    print("   - Loading WSTAFF billet export")
    aos_file = aos_file.append(
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
            )
    print("   - Loading WUSAFF billet export")
    aos_file = aos_file.append(
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
    aos_file["AOS_FILE_DATE"] = AOS_FILE_DATE
    return aos_file
                
def load_rcms():
    rcms = pd.read_excel(
        DATA_PATH + "/rcmsr/assignments/" + RCMS_FILE,
        dtype = {
            "GFC1" : str,
            "GFC 1 Name" : str,
            "GFC2" : str,
            "GFC 2 Name" : str,
            "Mask" : str,
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
    ).rename(
        columns = {
            "Mask" : "SSN_MASK",
            #"UPC" : "UIC",
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
        }
    )
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
    
    rcms["RCMS_FILE"] = RCMS_FILE
    rcms = rcms.where(~rcms.DUTY_ASG_DT.isna()).dropna(how = "all")
    return rcms.where(~rcms.RANK_AB.isna()).dropna(how = "all")

""" Retrieve SSN_MASK, UIC, PARNO, LN POSN KEY from Apart AGR file"""
def load_apart():
    apart = pd.read_excel(DATA_PATH + "/rcmsr/assignments/" + APART_FILE,
        dtype = {
            "Mask" : str,
            "UIC" : str,
            "Paragraph" : str,
            "Line Number" : str,
            "APART_POSN_KEY" : str
        }
    ).rename(
        columns = {
            "Mask" : "SSN_MASK",
            "Paragraph" : "PARNO",
            "Line Number" : "LN",
        }
    )[["SSN_MASK", "UIC", "PARNO", "LN", "APART_POSN_KEY"]]
    apart = apart.where(~apart.SSN_MASK.isna()).dropna(how = "all")
    return apart

""" Retrieve EMILPO position level assignment file """
def load_emilpo():
    emilpo_file = pd.read_csv(
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
    emilpo_file["EMILPO_FILE_DATE"] = EMILPO_FILE_DATE
    return emilpo_file

def load_emilpo_temp_assignments():
    print(" - Loading and processing emilpo temporary assignments file")
    emilpo_temp_assignments = pd.read_csv(
        DATA_PATH + "/emilpo/temp_assignments/EMILPO_TEMP_ASSIGNMENTS_" + 
        EMILPO_TEMP_FILE_DATE + ".csv",
        dtype = {
            "SSN_MASK_HASH" : str,
            "UIC_CD" : str,	
            "RC_ATTACH_CAT_CD" : str,
            "ATTACH_START_DT" : str,
            "ATTACH_EXP_DT" : str,
            "ATTACH_RSN_CD" : str, 
            "ATTACH_TYP_CD" : str,
            "TAPDB_REC_STAT_CD" : str
        }
    )
    emilpo_temp_assignments["ATTACH_START_DT"] = pd.to_datetime(
        emilpo_temp_assignments.ATTACH_START_DT, infer_datetime_format = True, errors = "ignore"
    )
    emilpo_temp_assignments = process_data.calculate_age(
        emilpo_temp_assignments, utility.get_local_time_as_datetime(), 
        "ATTACH_START_DT", "ATTACHMENT"
    )
    return emilpo_temp_assignments[[
        "SSN_MASK_HASH", "UIC_CD", "RC_ATTACH_CAT_CD", "ATTACH_START_DT",
        "ATTACH_EXP_DT", "ATTACH_RSN_CD", "ATTACH_TYP_CD", "TAPDB_REC_STAT_CD"
    ]].rename(
        columns = {
            "SSN_MASK_HASH" : "SSN_MASK",
            "UIC_CD" : "ATTACH_UIC"
        }
    )

""" Retreive match phases file """
def load_match_phases():
    return pd.read_csv(DATA_PATH + "/phases/" + PHASES_FILE).set_index("STAGE")

""" Retrieve rank grade crosswalk file """
def load_rank_grade_xwalk():
    return pd.read_csv(DATA_PATH + "/xwalks/rank_grade_xwalk.csv").set_index("RANK")

""" Retrieve grade mismatch crosswalk file """
def load_grade_mismatch_xwalk():
    return pd.read_csv(DATA_PATH + "/xwalks/grade_mismatch_xwalk.csv").set_index("GRADE")

""" Retrieve OUID to UIC mapping file """
def load_ouid_uic_xwalk():
    return pd.read_csv(DATA_PATH + "/xwalks/OUID_UIC_FY21.CSV")

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

""" Load acronym list for GFM_LNAME generation """
def load_gfm_lname_acronyms():
    print(" - Loading GFIM LName Acronym List File")
    return pd.read_excel(DATA_PATH + "/xwalks/gfm_lname_acronyms.xlsx")

def load_uic_current_addresses():
    print(" - Loading current UIC addresses")
    address_data = pd.read_csv(
        DATA_PATH + "/uic_location/" + UIC_ADDRESS_FILE,
        sep = '\t'
    )
    address_data = address_data.where(
        address_data.TFY == 9999
    ).dropna(how = "all")
    address_data = address_data.drop_duplicates(subset = ["UIC"])
    return address_data

def load_country_code_xwalk():
    print(" - Loading the country code crosswalk")
    country_code_xwalk = pd.read_csv(
        DATA_PATH + "/domain_codes/genc_country_codes.csv"
    )
    return country_code_xwalk

#emilpo: (17,29,30,31,32,46,47,48,49,50,51)
    
