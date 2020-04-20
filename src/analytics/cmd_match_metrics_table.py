# -*- coding: utf-8 -*-

def make_cmd_f2s_metric_df(all_faces_to_matched_spaces):
    cmd_metrics = all_faces_to_matched_spaces.groupby(
        ["STRUC_CMD_CD"]
    ).size().reset_index().rename(
        columns = {0 : "ASSIGNED"}
    )
    
    non_matches = all_faces_to_matched_spaces.copy().where(
        all_faces_to_matched_spaces.stage_matched == 0
    ).dropna(how = "all")
    
    cmd_metrics = cmd_metrics.join(
        non_matches[["STRUC_CMD_CD", "SSN_MASK"]].dropna(how = "all").groupby(
            ["STRUC_CMD_CD"]
        ).count().rename(
            columns = {"SSN_MASK" : "UNMATCHED"}
        ),
        on = "STRUC_CMD_CD"
    )
        
    cmd_metrics["MATCHED"] = cmd_metrics.ASSIGNED - cmd_metrics.UNMATCHED
    cmd_metrics["PERCENT_MATCHED"] = cmd_metrics.MATCHED / cmd_metrics.ASSIGNED
    
    cmd_metrics = cmd_metrics.join(
        non_matches.where(
            non_matches.CREATE_TEMPLET == 1.0
        )[["STRUC_CMD_CD", "SSN_MASK"]].dropna(how = "all").groupby(
            ["STRUC_CMD_CD"]        
        ).count().rename(
            columns = {"SSN_MASK" : "UNMATCHED_NO_TEMPLET"}
        ),
        on = "STRUC_CMD_CD"
    )
        
    cmd_metrics["UNMATCHED_NO_UIC"] = cmd_metrics.UNMATCHED - cmd_metrics.UNMATCHED_NO_TEMPLET
    
    cmd_metrics = cmd_metrics.join(
        non_matches.where(
            non_matches.ADD_UIC_TO_AOS == 1.0
        ).dropna(
            how = "all"
        )[["STRUC_CMD_CD", "UIC_emilpo"]].drop_duplicates(
            "UIC_emilpo"
        ).groupby(
            ["STRUC_CMD_CD"],
            as_index = False,
            axis = 0
        ).count().rename(
            columns = {"UIC_emilpo" : "UICS_NOT_IN_AOS"}        
        ).set_index(
            "STRUC_CMD_CD"        
        ),
        on = "STRUC_CMD_CD"
    )
        
    return cmd_metrics