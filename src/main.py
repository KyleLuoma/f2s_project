# -*- coding: utf-8 -*-
"""
Created on Mon Feb 10 13:45:26 2020

@author: LuomaKR
"""
import pandas as pd
from pandas.api.types import CategoricalDtype
import numpy as np
import load_data
import process_data
import export
import utility
import analytics
import analytics.analytics_driver
import analytics.cmd_match_metrics_table
import analytics.cmd_metrics_package
import diagnostics
import match
import analytics.templet_analysis

LOAD_MATCH_PHASES = False
LOAD_AND_PROCESS_MAPS = True
LOAD_COMMAND_CONSIDERATIONS = False
PROCESS_COMMAND_CONSIDERATIONS = False
LOAD_AND_PROCESS_SPACES = False
LOAD_AND_PROCESS_ADDRESS_DATA = False
LOAD_EMILPO_FACES = False
LOAD_RCMS_FACES = False
VERBOSE = False
RUN_MATCH = False
EXPORT_F2S = True
GENERATE_CMD_METRICS = False
EXPORT_UNMATCHED = False
EXPORT_UNMASKED = False #Export ONLY to your local drive, not to a network folder
UPDATE_CONNECTIONS = False
EXPORT_CMD_SPECS = False
COMMAND_EXPORT_LIST = ["AR"] #Leave empty to export all commands

def main():
    global drrsa, spaces, faces, match_phases, rank_grade_xwalk, test_faces 
    global test_spaces, face_space_match, unmatched_faces, unmatched_analysis
    global grade_mismatch_xwalk, all_faces_to_matched_spaces, aos_ouid_uic_xwalk 
    global rmk_codes, uic_hd_map, cmd_description_xwalk, cmd_match_metrics_table
    global cmd_metrics, af_uic_list, remaining_spaces, all_uics, ar_cmd_metrics
    global all_spaces_to_matched_faces, uic_templets, emilpo_faces, rcms_faces
    global ac_ar_metrics, address_data, acronym_list
    
    utility.create_project_directories()
        
    if(LOAD_MATCH_PHASES):
        print(" - Loading match phases")
        match_phases = load_data.load_match_phases()
        
    # Find last stage that uses templets:
    last_templet_stage = match_phases.where(
        match_phases.TEMPLET
    ).dropna(how = "all").tail(1).index[0]
        
    if(LOAD_AND_PROCESS_MAPS):
        print(" - Loading and processing mapping files")
        uic_hd_map = load_data.load_uic_hd_map()
        rank_grade_xwalk = load_data.load_rank_grade_xwalk()
        grade_mismatch_xwalk = load_data.load_grade_mismatch_xwalk()
        aos_ouid_uic_xwalk = load_data.load_ouid_uic_xwalk()
        rmk_codes = load_data.load_rmk_codes()
        cmd_description_xwalk = load_data.load_cmd_description_xwalk()
        acronym_list = load_data.load_gfm_lname_acronyms()
        country_code_xwalk = load_data.load_country_code_xwalk()
        
    if(LOAD_COMMAND_CONSIDERATIONS):
        af_uic_list = load_data.load_af_uics()
        
    if(LOAD_AND_PROCESS_SPACES):
        spaces, drrsa, all_uics, address_data = load_data.load_and_process_spaces(
            uic_hd_map, country_code_xwalk        
        )
        
    if(LOAD_AND_PROCESS_ADDRESS_DATA):
        address_data = load_data.load_and_process_address_data(country_code_xwalk)
    
    if(LOAD_EMILPO_FACES or LOAD_RCMS_FACES):
        faces = load_data.load_and_process_faces(
            LOAD_EMILPO_FACES,
            LOAD_RCMS_FACES,
        )
    
    if(RUN_MATCH):
        # Initiate the matching process by calling this function:
        unmatched_faces, remaining_spaces, face_space_match = match.split_population_full_runs(
            faces,
            spaces,
            match_phases,
            rmk_codes,
        )       
    
        all_faces_to_matched_spaces = diagnostics.run_face_match_diagnostics(
            faces,
            face_space_match,
            spaces,
            last_templet_stage,
            match_phases,
            all_uics
        )
    
        all_spaces_to_matched_faces = diagnostics.space_available_analysis(
            faces, face_space_match, spaces        
        )
    
# =============================================================================
#     uic_templets = analytics.templet_analysis.templet_usage_by_uic(
#         all_spaces_to_matched_faces
#     )
# =============================================================================
    if(GENERATE_CMD_METRICS):
        cmd_metrics, ar_cmd_metrics, ac_ar_metrics = analytics.analytics_driver.run_analytics(
            all_faces_to_matched_spaces,        
        )
    
    export.run_export_jobs(
        EXPORT_F2S,
        EXPORT_UNMATCHED,
        UPDATE_CONNECTIONS,
        EXPORT_UNMASKED,
        EXPORT_CMD_SPECS,
        COMMAND_EXPORT_LIST,
        face_space_match,
        all_faces_to_matched_spaces,
        cmd_metrics,
        ar_cmd_metrics,
        ac_ar_metrics,
        unmatched_faces,
        uic_templets,
        drrsa,
        address_data,
        acronym_list
    )

if (__name__ == "__main__"): main()



