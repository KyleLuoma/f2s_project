# -*- coding: utf-8 -*-
"""
Created on Mon Feb 10 13:45:26 2020

@author: LuomaKR
"""
import pandas as pd
import numpy as np
import config
import load_data
import aos_unzipper
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


def main():
    global drrsa, spaces, faces, faces_tmp, match_phases, rank_grade_xwalk, test_faces 
    global test_spaces, face_space_match, unmatched_faces, unmatched_analysis
    global grade_mismatch_xwalk, all_faces_to_matched_spaces, aos_ouid_uic_xwalk 
    global rmk_codes, uic_hd_map, cmd_description_xwalk, cmd_match_metrics_table
    global cmd_metrics, af_uic_list, remaining_spaces, all_uics, ar_cmd_metrics
    global uic_templets, emilpo_faces, rcms_faces
    global ac_ar_metrics, address_data, acronym_list, attach_face_space_match
    global curorg_metrics, attached_faces_to_matched_spaces, run_config, file_config
    
    run_config = config.get_run_config()
    file_config = config.get_file_config() 
    
# =============================================================================
#     Check the presence and content of the input files here prior to fully loading
# =============================================================================
    if(run_config['LOAD_AND_PROCESS_SPACES']): 
        aos_unzipper.unzip_aos_files(file_path = file_config['DATA_PATH'])
        assert(load_data.check_spaces_files_exist(file_config))
        
    if(run_config['LOAD_AND_PROCESS_ADDRESS_DATA']): 
        assert(load_data.check_address_files_exist(file_config))
        
    if(run_config['LOAD_EMILPO_FACES']): 
        assert(load_data.check_emilpo_files_exist(file_config))
        
    if(run_config['LOAD_EMILPO_TEMP_ASSIGNMENTS']): 
        assert(load_data.check_emilpo_temp_files_exist(file_config))
        
    if(run_config['LOAD_RCMS_FACES']): 
        assert(load_data.check_rcms_files_exist(file_config))
        load_data.check_rcms_columns(file_config)
    
    utility.create_project_directories()
        
    if(run_config['LOAD_MATCH_PHASES']):
        match_phases = load_data.load_match_phases(file_config)
        
    # Find last stage that uses templets:
    last_templet_stage = match_phases.where(
        match_phases.TEMPLET
    ).dropna(how = "all").tail(1).index[0]
        
    if(run_config['LOAD_AND_PROCESS_MAPS']):
        print(" - Loading and processing mapping files")
        uic_hd_map = load_data.load_uic_hd_map(file_config)
        rank_grade_xwalk = load_data.load_rank_grade_xwalk(file_config)
        grade_mismatch_xwalk = load_data.load_grade_mismatch_xwalk(file_config)
        aos_ouid_uic_xwalk = load_data.load_ouid_uic_xwalk(file_config)
        rmk_codes = load_data.load_rmk_codes(file_config)
        cmd_description_xwalk = load_data.load_cmd_description_xwalk(file_config)
        acronym_list = load_data.load_gfm_lname_acronyms(file_config)
        country_code_xwalk = load_data.load_country_code_xwalk(file_config)
        
    if(run_config['LOAD_COMMAND_CONSIDERATIONS']):
        af_uic_list = load_data.load_af_uics(file_config)
        
    if(run_config['LOAD_AND_PROCESS_SPACES']):
        spaces, drrsa, all_uics = load_data.load_and_process_spaces(
            file_config, uic_hd_map, country_code_xwalk        
        )
        
    if(run_config['LOAD_AND_PROCESS_ADDRESS_DATA']):
        address_data = load_data.load_and_process_address_data(
            file_config, country_code_xwalk
        )
        
    if(run_config['LOAD_EMILPO_FACES']):
        emilpo_faces = pd.DataFrame()
        
    if(run_config['LOAD_RCMS_FACES']):
        rcms_faces = pd.DataFrame()
    
    if(run_config['LOAD_EMILPO_FACES'] or run_config['LOAD_RCMS_FACES']):
        faces = load_data.load_and_process_faces(
            run_config,
            file_config,
            rank_grade_xwalk,
            grade_mismatch_xwalk,
            aos_ouid_uic_xwalk,
            drrsa,
            uic_hd_map,
            emilpo_faces,
            rcms_faces,
            af_uic_list
        )
        
    if(run_config['LOAD_EMILPO_TEMP_ASSIGNMENTS']):
        emilpo_temp = load_data.load_emilpo_temp_assignments(file_config)
        emilpo_temp = process_data.add_expected_hsduic(
            emilpo_temp.rename(columns = {"ATTACH_UIC" : "UIC"}), 
            uic_hd_map, NA_value = "NA"
        ).rename(columns = {"HSDUIC" : "HSDUIC_TEMP", "UIC" : "ATTACH_UIC"})
        if "level_0" in faces.columns:
            del faces["level_0"]
        faces = faces.reset_index().set_index("SSN_MASK").join(
            emilpo_temp.set_index("SSN_MASK"),
            lsuffix = "_faces",
            rsuffix = "_temp"
        ).reset_index()
        if "index_faces" in faces.columns:
            del faces["index_faces"]
        if "index_temp" in faces.columns:
            del faces["index_temp"]
    
    if(run_config['RUN_MATCH']):
        # Initiate the matching process by calling this function:
        unmatched_faces, remaining_spaces, face_space_match, attach_face_space_match = \
        match.split_population_full_runs(
            faces,
            spaces,
            match_phases,
            rmk_codes,
        )
           
    if(run_config['RUN_MATCH_DIAGNOSTICS']):
        all_faces_to_matched_spaces = diagnostics.run_face_match_diagnostics(
            faces,
            face_space_match,
            spaces,
            last_templet_stage,
            match_phases,
            all_uics,
            add_vacant_position_rows = True
        )
        
        attached_faces_to_matched_spaces = diagnostics.run_face_match_diagnostics(
            faces.rename(columns = {
                "UIC" : "ASSIGN_UIC", "ATTACH_UIC" : "UIC"        
            }).dropna(subset = ["UIC"], how = "all"),
            attach_face_space_match,
            spaces,
            last_templet_stage,
            match_phases,
            all_uics,
            add_vacant_position_rows = False
        )
    
    if(run_config['GENERATE_CMD_METRICS']):
        cmd_metrics, ar_cmd_metrics, ac_ar_metrics, curorg_metrics \
            = analytics.analytics_driver.run_analytics(
                all_faces_to_matched_spaces,        
            )
        
    
    export.run_export_jobs(
        run_config,
        file_config,
        face_space_match,
        all_faces_to_matched_spaces,
        attached_faces_to_matched_spaces,
        cmd_metrics,
        ar_cmd_metrics,
        ac_ar_metrics,
        curorg_metrics,
        unmatched_faces,
        drrsa,
        address_data,
        acronym_list
    )

if (__name__ == "__main__"): main()



