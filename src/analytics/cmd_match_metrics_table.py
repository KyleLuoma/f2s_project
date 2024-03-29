# -*- coding: utf-8 -*-

def make_cmd_f2s_metric_df(
    all_faces_to_matched_spaces, 
    timestamp,
    group_by = "STRUC_CMD_CD",
    include_columns = []
    ):
    import pandas as pd
    print("yes, this change is reflected")
    cmd_metrics = all_faces_to_matched_spaces.groupby(
        [group_by]
    ).size().reset_index().rename(
        columns = {0 : "ASSIGNED"}
    )
    print(" - Generating command metrics rollup")
    print("  - Dropping unencumbered positions from all_faces_to_matched_spaces")
    all_faces_to_matched_spaces = all_faces_to_matched_spaces.where(
        ~all_faces_to_matched_spaces.SSN_MASK.isna()        
    )
    
    for column in include_columns:
        if column not in all_faces_to_matched_spaces.columns:
            print("Column " + str(column) + " not available in all_faces_to_matched_spaces.")
            continue
        cmd_metrics = cmd_metrics.reset_index().set_index(group_by).join(
            all_faces_to_matched_spaces.groupby(
                [group_by, column], as_index = False
            ).count().set_index(group_by)[[column]]
        ).reset_index()
        del cmd_metrics["index"]
            
    
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
    
    # Because fillna() isn't working here, replace nan with 0s where commands
    # get 100 percent matches
    cmd_metrics.UNMATCHED.fillna(0, inplace = True)
               
        
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
        )[[group_by, "UIC_facesfile"]].drop_duplicates(
            "UIC_facesfile"
        ).groupby(
            [group_by],
            as_index = False,
            axis = 0
        ).count().rename(
            columns = {"UIC_facesfile" : "UICS_NOT_IN_AOS"}        
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
    
        
    if(group_by == "STRUC_CMD_CD"):
        cmd_metrics = cmd_metrics.join(
            non_matches.where(
                non_matches.RCC == "IMA"
            ).dropna(how = "all")[["TAPDBR_CMD_CD", "SSN_MASK"]].dropna(
                how = "all"
            ).groupby(
                "TAPDBR_CMD_CD"        
            ).count().rename(
                columns = {"SSN_MASK" : "UNMATCHED_IMA"}        
            ),
            on = group_by
        )
        cmd_metrics.UNMATCHED_IMA.fillna(0, inplace = True)
            
        cmd_metrics = cmd_metrics.join(
            non_matches.where(
                non_matches.RCC == "AGR"
            ).dropna(how = "all")[["TAPDBR_CMD_CD", "SSN_MASK"]].dropna(
                how = "all"
            ).groupby(
                "TAPDBR_CMD_CD"        
            ).count().rename(
                columns = {"SSN_MASK" : "UNMATCHED_AGR"}        
            ),
            on = group_by
        )
        cmd_metrics.UNMATCHED_AGR.fillna(0, inplace = True)
        
    return cmd_metrics

def merge_AC_RC_cmd_metrics(cmd_metrics, ar_cmd_metrics):
    cmd_metrics["COMPONENT"] = "AC"
    ar_cmd_metrics["COMPONENT"] = "AR"
    ar_cmd_metrics = ar_cmd_metrics.rename(
        columns = {
            'GFC1' : 'STRUC_CMD_CD'        
        }        
    )
    ACRC_metrics = cmd_metrics.append(ar_cmd_metrics)
    ACRC_metrics.loc[ACRC_metrics["STRUC_CMD_CD"] == "AR", "COMPONENT"] = "AR"
    return ACRC_metrics




