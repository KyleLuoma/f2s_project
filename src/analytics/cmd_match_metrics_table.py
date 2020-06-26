# -*- coding: utf-8 -*-

def make_cmd_f2s_metric_df(
    all_faces_to_matched_spaces, 
    timestamp,
    group_by = "STRUC_CMD_CD",
    include_columns = []
    ):
    cmd_metrics = all_faces_to_matched_spaces.groupby(
        [group_by]
    ).size().reset_index().rename(
        columns = {0 : "ASSIGNED"}
    )
    
    cmd_metrics["METRIC_DATE"] = timestamp
    
    non_matches = all_faces_to_matched_spaces.copy().where(
        all_faces_to_matched_spaces.stage_matched == 0
    ).dropna(how = "all")
    
    cmd_metrics = cmd_metrics.join(
        non_matches[[group_by, "SSN_MASK"]].dropna(how = "all").groupby(
            [group_by]
        ).count().rename(
            columns = {"SSN_MASK" : "UNMATCHED"}
        ),
        on = group_by
    )
        
    cmd_metrics["MATCHED"] = cmd_metrics.ASSIGNED - cmd_metrics.UNMATCHED
    cmd_metrics["PERCENT_MATCHED"] = cmd_metrics.MATCHED / cmd_metrics.ASSIGNED
    
    cmd_metrics = cmd_metrics.join(
        non_matches.where(
            non_matches.CREATE_TEMPLET == 1.0
        )[[group_by, "SSN_MASK"]].dropna(how = "all").groupby(
            [group_by]        
        ).count().rename(
            columns = {"SSN_MASK" : "UNMATCHED_NO_TEMPLET"}
        ),
        on = group_by
    )
        
    cmd_metrics.UNMATCHED_NO_TEMPLET.fillna(0, inplace = True)
    cmd_metrics["UNMATCHED_NO_UIC"] = cmd_metrics.UNMATCHED - cmd_metrics.UNMATCHED_NO_TEMPLET
    cmd_metrics.UNMATCHED_NO_UIC.fillna(0, inplace = True)
    
    cmd_metrics = cmd_metrics.join(
        non_matches.where(
            non_matches.ADD_UIC_TO_AOS == 1.0
        ).dropna(
            how = "all"
        )[[group_by, "UIC_emilpo"]].drop_duplicates(
            "UIC_emilpo"
        ).groupby(
            [group_by],
            as_index = False,
            axis = 0
        ).count().rename(
            columns = {"UIC_emilpo" : "UICS_NOT_IN_AOS"}        
        ).set_index(
            group_by        
        ),
        on = group_by
    )
    cmd_metrics.UICS_NOT_IN_AOS.fillna(0, inplace = True)
    
    cmd_metrics = cmd_metrics.join(
        all_faces_to_matched_spaces.where(
                all_faces_to_matched_spaces.ASG_OLDER_THAN_POS
        ).dropna(
            how = "all"
        )[[group_by, "SSN_MASK"]].groupby(
            [group_by],
            as_index = False,
            axis = 0
        ).count().rename(
            columns = {"SSN_MASK" : "MATCHED_ASG_OLDER_THAN_POS"}
        ).set_index(
            group_by
        ),
        on = group_by
    )
    cmd_metrics.MATCHED_ASG_OLDER_THAN_POS.fillna(0, inplace = True)
        
    return cmd_metrics