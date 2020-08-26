# -*- coding: utf-8 -*-

import pandas as pd

def create_cmd_metrics_packages(
    all_faces_to_matched_spaces,
    uic_templets,
    drrsa,
    unmask = False,
    date_time_string = "",
    commands = []
):
    uic_templets_needed = all_faces_to_matched_spaces.where(
            all_faces_to_matched_spaces.CREATE_TEMPLET == 1.0
    ).dropna(how = "all")[["UIC_emilpo", "SSN_MASK"]]  
    
    uic_templets_needed = uic_templets_needed.groupby(
        ["UIC_emilpo"],
        observed = True,
        as_index = False
    ).count().rename(
        columns = {
            "UIC_emilpo" : "UIC",
            "SSN_MASK" : "All Command Templet Requirement"
        }        
    )
    
    if(len(commands) > 0):
        print(type(commands))
        assert type(commands) == list
        all_faces_to_matched_spaces = all_faces_to_matched_spaces.where(
            all_faces_to_matched_spaces.STRUC_CMD_CD.isin(commands)        
        ).dropna(how = "all")
        
    # for each command in match file
    cmd_list = all_faces_to_matched_spaces[["STRUC_CMD_CD"]].groupby(
        "STRUC_CMD_CD"
    ).count().index.to_list()
    
    uic_gfcs = all_faces_to_matched_spaces[["UIC_emilpo", "GFC", "GFC 1 Name"]].groupby(
        ["UIC_emilpo", "GFC"], as_index = False
    ).count()[["UIC_emilpo", "GFC"]]
    uic_gfcs = uic_gfcs.set_index("UIC_emilpo").join(
        all_faces_to_matched_spaces[["UIC_emilpo", "GFC 1 Name", "GFC"]].groupby(
            ["UIC_emilpo", "GFC 1 Name"], as_index = False
        ).count()[["UIC_emilpo", "GFC 1 Name"]].set_index("UIC_emilpo")
    ).reset_index()
    
    
    for cmd in cmd_list:
        print(" - Procesing command metrics file for: " + cmd)
        
        # create a DF with all match data included
        cmd_df = all_faces_to_matched_spaces.copy().where(
            all_faces_to_matched_spaces.STRUC_CMD_CD == cmd
        ).dropna(how = "all").reset_index()
        
        # create a DF with a list of UICs needing to be built
        cmd_uics_needed = cmd_df.where(
            cmd_df.ADD_UIC_TO_AOS == 1.0
        ).dropna(how = "all")[["UIC_emilpo", "SSN_MASK"]]
        
        cmd_uics_needed = cmd_uics_needed.groupby(
            ["UIC_emilpo"],
            observed = True,
            as_index = False
        ).count().rename(
            columns = {
                "UIC_emilpo" : "UIC not in AOS",
                "SSN_MASK" : "Num Soldiers assigned to UIC"
            }        
        )
        
        if(cmd == "AR"):
            cmd_uics_needed = cmd_uics_needed.reset_index().set_index("UIC not in AOS").join(
                uic_gfcs.reset_index().set_index("UIC_emilpo"),
                lsuffix = "_cmd_uics_needed",
                rsuffix = "_uic_gfcs"
            ).reset_index()
        
        if(cmd_uics_needed.shape[0] > 0):
            cmd_uics_needed["UIC in DRRSA"] = cmd_uics_needed.apply(
                lambda row: row["UIC not in AOS"] in drrsa.UIC.tolist(),
                axis = 1
            )
            
            cmd_uics_needed = cmd_uics_needed.reset_index().set_index("UIC not in AOS").join(
                drrsa.reset_index().set_index("UIC")[[
                    "ANAME", "LNAME", "ADCON", "GEOLOCATIONNAME"
                ]]
            ).reset_index()
        
        # create a DF with a list of UICs that require templets
        cmd_templets_needed = cmd_df.where(
            cmd_df.CREATE_TEMPLET == 1.0
        ).dropna(how = "all")[["UIC_emilpo", "SSN_MASK"]]
        
        cmd_templets_needed = cmd_templets_needed.groupby(
            ["UIC_emilpo"],
            observed = True,
            as_index = False
        ).count().rename(
            columns = {
                "UIC_emilpo" : "UICs requiring templets",
                "SSN_MASK" : cmd + " Templet Requirement"
            }        
        )
        
        if(cmd == "AR"):
            cmd_templets_needed = cmd_templets_needed.reset_index().set_index(
                "UICs requiring templets"
            ).join(
                uic_gfcs.reset_index().set_index("UIC_emilpo"),
                lsuffix = "_cmd_uics_needed",
                rsuffix = "_uic_gfcs"
            ).reset_index()
        
        cmd_templets_needed = cmd_templets_needed.set_index(
            "UICs requiring templets"
        ).join(
            uic_templets_needed.set_index("UIC")
        ).reset_index()
# =============================================================================
#         cmd_templets_needed = cmd_templets_needed.set_index(
#             "UICs requiring templets"
#         ).join(
#             uic_templets.set_index("UIC")        
#         ).reset_index()
# =============================================================================
        
        # create a DF with a list of positions with assignment age > position age
        cmd_asg_age = cmd_df.where(
            cmd_df.ASG_OLDER_THAN_POS == 1.0
        ).dropna(how = "all")[[
            "UIC_emilpo", 
            "PARNO_emilpo",
            "LN_emilpo",
            "FMID",
            "S_DATE",
            "POSITION_AGE",
            "DUTY_ASG_DT",
            "ASSIGNMENT_AGE",
            "SSN_MASK"
        ]].rename(columns = {
            "UIC_emilpo" : "UIC",
            "PARNO_emilpo" : "PARNO",
            "LN_emilpo" : "LN",
            "FMID" : "Position FMID",
            "S_DATE" : "Position Start Date",
            "DUTY_ASG_DT" : "Assignment Start Date"
        })
        
        # include an option to export unmasked metrics
        if(unmask):
            print(" *** still need to build unmask functionality ***")
        
        # export to a file
        print(" - Saving " + cmd + " metrics as file")
        with pd.ExcelWriter(
            "//ba-anvl-fs05/FMDShare/AOS/f2s_project/export/cmd_metrics/" + 
            cmd + 
            "_f2s_metrics" + 
            date_time_string + 
            ".xlsx"
        ) as writer:
            cmd_df.to_excel(
                writer, sheet_name = "face-space-detail", freeze_panes = (1,0)
            )
            cmd_uics_needed.to_excel(
                writer, sheet_name = "UICs-not-in-AOS", freeze_panes = (1,0)
            )
            cmd_templets_needed.to_excel(
                writer, sheet_name = "Templets-to-build", freeze_panes = (1,0)
            )
            cmd_asg_age.to_excel(
                writer, sheet_name = "Old-Assignments", freeze_panes = (1,0)        
            )
    
    
