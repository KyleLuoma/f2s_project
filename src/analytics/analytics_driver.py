# -*- coding: utf-8 -*-

import analytics.cmd_match_metrics_table
import utility


def run_analytics(
    all_faces_to_matched_spaces
):
    cmd_metrics = analytics.cmd_match_metrics_table.make_cmd_f2s_metric_df(
        all_faces_to_matched_spaces,
        utility.get_date_string()
    )
    
    ar_cmd_metrics = analytics.cmd_match_metrics_table.make_cmd_f2s_metric_df(
        all_faces_to_matched_spaces.where(
            all_faces_to_matched_spaces.STRUC_CMD_CD == "AR"
        ).dropna(how = "all"),
        utility.get_date_string(),
        group_by = "GFC1",
        include_columns = ["GFC 1 Name"]
    )
    
    ac_ar_metrics = analytics.cmd_match_metrics_table.merge_AC_RC_cmd_metrics(
        cmd_metrics, ar_cmd_metrics
    )
    
    curorg_metrics = analytics.cmd_match_metrics_table.make_cmd_f2s_metric_df(
        all_faces_to_matched_spaces.where(
            all_faces_to_matched_spaces.STRUC_CMD_CD == "AR"        
        ).dropna(how = "all"),
        utility.get_date_string(),
        group_by = "RCC"
    )
    
    return cmd_metrics, ar_cmd_metrics, ac_ar_metrics, curorg_metrics