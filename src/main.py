# -*- coding: utf-8 -*-
"""
Created on Mon Feb 10 13:45:26 2020

@author: LuomaKR
"""
import pandas as pd
from pandas.api.types import CategoricalDtype
import numpy as np
import os
os.chdir('\\\\ba-anvl-fs05\\FMDShare\\AOS\\f2s_project\\src')
import load_data
import process_data
import utility
import pyodbc as db
import analytics.cmd_match_metrics_table
import analytics.cmd_metrics_package
import unmask
import diagnostics
import match
import analytics.templet_analysis

LOAD_MATCH_PHASES = True
LOAD_AND_PROCESS_MAPS = True
LOAD_COMMAND_CONSIDERATIONS = True
PROCESS_COMMAND_CONSIDERATIONS = True
LOAD_AND_PROCESS_SPACES = False
LOAD_EMILPO_FACES = False
LOAD_RCMS_FACES = False
VERBOSE = False
EXPORT_F2S = False
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
        
    if(LOAD_COMMAND_CONSIDERATIONS):
        af_uic_list = load_data.load_af_uics()
        
    if(LOAD_AND_PROCESS_SPACES):        
        print(" - Loading and processing spaces files")
        drrsa = load_data.load_drrsa_file()
        spaces = load_data.load_army_command_aos_billets()
        spaces = process_data.process_aos_billet_export(spaces)
        spaces = process_data.add_expected_hsduic(spaces, uic_hd_map, "NA")
        spaces = process_data.add_drrsa_data(spaces, drrsa)
        spaces = process_data.categorical_spaces(spaces)
        spaces = process_data.calculate_age(
            spaces, utility.get_local_time_as_datetime(), "S_DATE", "POSITION"
        )
        spaces = process_data.add_is_templet_column(spaces)
        all_uics = spaces.UIC.drop_duplicates().append(
            load_data.load_uics_from_uic_trees()
        )
        
    if(LOAD_EMILPO_FACES):
        print(" - Loading and processing emilpo file")
        emilpo_faces = process_data.process_emilpo_assignments(
            load_data.load_emilpo(), 
            rank_grade_xwalk,
            grade_mismatch_xwalk,
            consolidate = True
        )
        
    if(LOAD_RCMS_FACES):
        print(" - Loading and processing rcms file")
        rcms_faces = load_data.load_rcms()
        rcms_faces = process_data.process_emilpo_assignments(
            rcms_faces,
            rank_grade_xwalk,
            grade_mismatch_xwalk, 
            consolidate = False
        )       
        
    if(LOAD_EMILPO_FACES or LOAD_RCMS_FACES):
        faces = emilpo_faces.append(rcms_faces, ignore_index = True)
        faces = process_data.add_drrsa_data(faces, drrsa)
        faces = process_data.check_uic_in_aos(
            faces, aos_ouid_uic_xwalk, "DRRSA_ADCON"
        )
        faces = process_data.add_templet_columns(faces, parno = "999")
        faces = process_data.add_expected_hsduic(faces, uic_hd_map, "None")
        faces = process_data.categorical_faces(faces)
        faces = process_data.calculate_age(
            faces, utility.get_local_time_as_datetime(), 
            "DUTY_ASG_DT", "ASSIGNMENT"
        )
        if(PROCESS_COMMAND_CONSIDERATIONS):
            faces = process_data.convert_cmd_code_for_uic_in_faces(
                faces, af_uic_list, uic_col_name = "UICOD", 
                cmd_col_name = "MACOM"
            )
            
    # Full run for AC faces and spaces:
    unmatched_faces, remaining_spaces, face_space_match = match.full_run(
        match_phases, 
        faces, 
        spaces,
        include_only_cmds = [],
        exclude_cmds = [],
        exclude_rmks = rmk_codes.where(rmk_codes.NO_AC)
            .dropna(how = "all")
            .index.to_list()
    )
        
    all_faces_to_matched_spaces = diagnostics.face_space_match_analysis(
        faces, face_space_match, spaces
    )
    all_faces_to_matched_spaces = process_data.add_match_phase_description(
        all_faces_to_matched_spaces, match_phases
    )
    all_faces_to_matched_spaces = diagnostics.diagnose_mismatch_in_target(
        all_faces_to_matched_spaces, 
        all_uics, 
        last_templet_stage
    )
    all_spaces_to_matched_faces = diagnostics.space_available_analysis(
        faces, face_space_match, spaces        
    )
    
    uic_templets = analytics.templet_analysis.templet_usage_by_uic(
        all_spaces_to_matched_faces
    )
    
    cmd_metrics = analytics.cmd_match_metrics_table.make_cmd_f2s_metric_df(
        all_faces_to_matched_spaces,
        utility.get_date_string()
    )
    
    ar_cmd_metrics = analytics.cmd_match_metrics_table.make_cmd_f2s_metric_df(
        all_faces_to_matched_spaces,
        utility.get_date_string(),
        group_by = "GFC",
        include_columns = ["GFC 1 Name"]
    )
    
    ac_ar_metrics = analytics.cmd_match_metrics_table.merge_AC_RC_cmd_metrics(
        cmd_metrics, ar_cmd_metrics
    )
    
    if(EXPORT_F2S): 
        face_space_match.to_csv(
            "..\export\\face_space_matches" 
            + utility.get_file_timestamp() 
            + ".csv"
        )
        all_faces_to_matched_spaces.to_csv(
            "..\export\\all_faces_to_matched_spaces" 
            + utility.get_file_timestamp() 
            + ".csv"
        )
        cmd_metrics.to_csv(
            "../export/command_metrics"
            + utility.get_file_timestamp()
            + ".csv"
        )                
        ar_cmd_metrics.to_csv(
            "../export/ar_command_metrics"
            + utility.get_file_timestamp()
            + ".csv"
        )
        ac_ar_metrics.to_csv(
            "../export/ac_ar_command_metrics"
            + utility.get_file_timestamp()
            + ".csv"
        )
        
    if(EXPORT_UNMATCHED): 
        unmatched_faces.to_csv(
            "..\export\\unmatched_faces" 
            + utility.get_file_timestamp() 
            + ".csv"
        )
        
    if(UPDATE_CONNECTIONS):
        all_faces_to_matched_spaces.to_csv(
            "..\export\\for_connections\\all_faces_to_matched_space_latest.csv"
        )
        cmd_metrics.to_csv("..\export\\for_connections\\cmd_metrics.csv")
        ar_cmd_metrics.to_csv("..\export\\for_connections\\ar_cmd_metrics.csv")
        
    if(EXPORT_UNMASKED):
        unmask.unmask_and_export(
            all_faces_to_matched_spaces, utility.get_file_timestamp(),
            emilpo_key_date = "7-24-2020"
        )
    
    if(EXPORT_CMD_SPECS):
        #import analytics.cmd_metrics_package #Uncomment for debugging
        analytics.cmd_metrics_package.create_cmd_metrics_packages(
            all_faces_to_matched_spaces,
            uic_templets,
            drrsa,
            unmask = EXPORT_UNMASKED,
            date_time_string = utility.get_file_timestamp(),
            commands = COMMAND_EXPORT_LIST
        )
        
    if(EXPORT_UNMASKED and EXPORT_CMD_SPECS):
        unmask.unmask_and_export(
            all_faces_to_matched_spaces.where(
                all_faces_to_matched_spaces.STRUC_CMD_CD.isin(
                    COMMAND_EXPORT_LIST
                )
            ).dropna(how = "all"),
            utility.get_file_timestamp(),
            cmd_labels = utility.make_commands_label(COMMAND_EXPORT_LIST),
            emilpo_key_date = "7-24-2020"
        )
        

if (__name__ == "__main__"): main()



