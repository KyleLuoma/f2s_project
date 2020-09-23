# -*- coding: utf-8 -*-
import utility
import unmask

def run_export_jobs(
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
    drrsa
):
    if(EXPORT_F2S):
        export_matches(face_space_match, all_faces_to_matched_spaces)
        export_metrics(cmd_metrics, ar_cmd_metrics, ac_ar_metrics)
        
    if(EXPORT_UNMATCHED):
        export_unmatched(unmatched_faces)
        
    if(UPDATE_CONNECTIONS):
        update_connections(
            all_faces_to_matched_spaces, cmd_metrics, ar_cmd_metrics
        )
        
    if(EXPORT_UNMASKED):
        unmask.unmask_and_export(
            all_faces_to_matched_spaces, utility.get_file_timestamp(),
            emilpo_key_date = "7-24-2020"
        )
    
    if(EXPORT_CMD_SPECS):
        import analytics.cmd_metrics_package #Uncomment for debugging
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

def export_matches(face_space_match, all_faces_to_matched_spaces):
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
    
def export_metrics(cmd_metrics, ar_cmd_metrics, ac_ar_metrics):
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
    
def export_unmatched(unmatched_faces):
    unmatched_faces.to_csv(
        "..\export\\unmatched_faces" 
        + utility.get_file_timestamp() 
        + ".csv"
    )
    
def update_connections(all_faces_to_matched_spaces, cmd_metrics, ar_cmd_metrics):
    all_faces_to_matched_spaces.to_csv(
        "..\export\\for_connections\\all_faces_to_matched_space_latest.csv"
    )
    cmd_metrics.to_csv("..\export\\for_connections\\cmd_metrics.csv")
    ar_cmd_metrics.to_csv("..\export\\for_connections\\ar_cmd_metrics.csv")