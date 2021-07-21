# -*- coding: utf-8 -*-

import pandas as pd
import analytics.lname_generator
import unmask

def create_cmd_metrics_packages(
    run_config,
    file_config,
    all_faces_to_matched_spaces,
    drrsa,
    address_data,
    acronym_list,
    curorg_metrics = pd.DataFrame(),
    ar_cmd_metrics = pd.DataFrame(),
    date_time_string = "",
):
    uic_templets_needed = all_faces_to_matched_spaces.where(
        all_faces_to_matched_spaces.CREATE_TEMPLET == 1.0
    ).dropna(how = "all")[["UIC_facesfile", "SSN_MASK"]]  
    
    uic_templets_needed = uic_templets_needed.groupby(
        ["UIC_facesfile"],
        #observed = True,
        as_index = False
    ).count().rename(
        columns = {
            "UIC_facesfile" : "UIC",
            "SSN_MASK" : "All Command Templet Requirement"
        }        
    )
    commands = run_config['COMMAND_EXPORT_LIST']    
    if(len(commands) > 0):
        print(type(commands))
        assert type(commands) == list
        only_cmd_faces = all_faces_to_matched_spaces.where(
            all_faces_to_matched_spaces.STRUC_CMD_CD.isin(commands)        
        ).dropna(how = "all")
    else:
        only_cmd_faces = all_faces_to_matched_spaces.copy()
        
    # for each command in match file
    cmd_list = only_cmd_faces[["STRUC_CMD_CD"]].groupby(
        "STRUC_CMD_CD"
    ).count().index.tolist()
    
    uic_gfcs = pd.DataFrame()
    if "AR" in cmd_list:
        print(" - AR selected for export, processing AR-specific data frames")
        ar_faces_spaces = all_faces_to_matched_spaces.query(
            'STRUC_CMD_CD == "AR" or DRRSA_ASGMT == "AR"'
        )
        
        uic_gfcs = ar_faces_spaces.groupby(
            ["GFC1", "UIC_facesfile"], as_index = False
        ).count()[["UIC_facesfile", "GFC1"]]
        
        uic_gfcs = uic_gfcs.set_index("UIC_facesfile").join(
            ar_faces_spaces[["UIC_facesfile", "GFC 1 Name", "GFC1"]].groupby(
                ["GFC 1 Name", "UIC_facesfile"], as_index = False
            ).count()[["UIC_facesfile", "GFC 1 Name"]].set_index("UIC_facesfile")
        ).reset_index()
            
    if not (len(cmd_list) == 1 and "AR" in cmd_list): #Make sure we're not doing an AR only run
        print(" - Commands other than AR selected for export, processing non-AR data frames")
        non_ar_faces_spaces = all_faces_to_matched_spaces.where(
            all_faces_to_matched_spaces.STRUC_CMD_CD != "AR"        
        ).dropna(how = "all")
        uic_dmls = non_ar_faces_spaces.groupby(
            ["DML_CD", "UIC_facesfile"], as_index = False        
        ).count()[["UIC_facesfile", "DML_CD"]]
        uic_dmsls = non_ar_faces_spaces.groupby(
            ["DMSL_CD", "UIC_facesfile"], as_index = False        
        ).count()[["UIC_facesfile", "DMSL_CD"]]
        uic_dml_dmsl = uic_dmls.set_index("UIC_facesfile").join(
            uic_dmsls.set_index("UIC_facesfile")        
        ).reset_index()
        
    for cmd in cmd_list:
        print(" - Procesing command metrics file for: " + cmd)
        
        # create a DF with all match data included
        cmd_df = all_faces_to_matched_spaces.query(
            'STRUC_CMD_CD == "' + cmd + '" or DRRSA_ASGMT == "' + cmd + '"'
        ).copy().reset_index()
# =============================================================================
#         copy().where(
#             all_faces_to_matched_spaces.STRUC_CMD_CD == cmd or
#             all_faces_to_matched_spaces.DRRSA_ASGMT == cmd
#         ).dropna(how = "all").reset_index()
# =============================================================================
        
        # create a DF with a list of UICs needing to be built
        cmd_uics_needed = cmd_df.where(
            cmd_df.ADD_UIC_TO_AOS == 1.0
        ).dropna(how = "all")[["UIC_facesfile", "SSN_MASK"]]
        cmd_uics_needed = cmd_uics_needed.where(
            ~cmd_uics_needed.SSN_MASK.isna()
        ).dropna(how = "all")
        cmd_uics_needed = cmd_uics_needed.groupby(
            ["UIC_facesfile"],
            as_index = False
        ).count().rename(
            columns = {
                "UIC_facesfile" : "UIC not in AOS"
            }        
        )
        del cmd_uics_needed["SSN_MASK"]
        
        if(cmd_uics_needed.shape[0] > 0):
            #Check if UIC is in DRRS-A
            cmd_uics_needed["UIC in DRRSA"] = cmd_uics_needed.apply(
                lambda row: row["UIC not in AOS"] in drrsa.UIC.tolist(),
                axis = 1
            )
            #Join DRRSA UIC data to cmd_uics_needed DF
            cmd_uics_needed = cmd_uics_needed.reset_index().set_index("UIC not in AOS").join(
                drrsa.reset_index().set_index("UIC")[[
                    "ADCON", "ANAME", "LNAME"
                ]]
            ).rename(columns = {
                    "ADCON" : "ADCON_PARENT",
                    "ANAME" : "UIC_ANAME",
                    "LNAME" : "UIC_LNAME"
                }
            )
            #Join address data to cmd_uics_needed DF
            cmd_uics_needed = cmd_uics_needed.join(
                address_data.reset_index().set_index("UIC")[[
                    "STACO", "ARLOC", "PH_CITY_TXT", "PH_GEO_TXT", 
                    "PH_POSTAL_CODE_TXT", "PH_COUNTRY_TXT"        
                ]]        
            ).reset_index()
            del cmd_uics_needed["index"]
            
            cmd_uics_needed = analytics.lname_generator.derive_gfm_lname(
                cmd_uics_needed, acronym_list, from_column = "UIC_ANAME", alt_from_column = "UIC_LNAME"       
            )
            
            cmd_uics_needed = cmd_uics_needed.rename(columns = {
                "ARLOC" : "HOGEO"        
            })         
            
            if(cmd == "AR"):
                ar_cmd_uics_needed = cmd_df.where(
                    cmd_df.ADD_UIC_TO_AOS == 1.0        
                ).dropna(how = "all")[["UIC_facesfile", "GFC1", "GFC 1 Name", "SSN_MASK"]]
                ar_cmd_uics_needed = ar_cmd_uics_needed.where(
                    ~ar_cmd_uics_needed.SSN_MASK.isna()        
                ).dropna(how = "all")
                ar_cmd_uics_needed = ar_cmd_uics_needed.groupby(
                    ["UIC_facesfile", "GFC1", "GFC 1 Name"]
                ).count().reset_index()
                cmd_uics_needed = ar_cmd_uics_needed.set_index("UIC_facesfile").join(
                    cmd_uics_needed.set_index("UIC not in AOS")     
                ).reset_index().rename(
                    columns = {
                        "index" : "UIC",
                        "SSN_MASK" : "AR Templet Requirement"
                    }
                )
            else:
                ac_cmd_uics_needed = cmd_df.where(
                    cmd_df.ADD_UIC_TO_AOS == 1.0        
                ).dropna(how = "all")[["UIC_facesfile", "DML_CD", "DMSL_CD", "SSN_MASK"]]
                ac_cmd_uics_needed = ac_cmd_uics_needed.where(
                    ~ac_cmd_uics_needed.SSN_MASK.isna()        
                ).dropna(how = "all")
                ac_cmd_uics_needed = ac_cmd_uics_needed.groupby(
                    ["UIC_facesfile", "DML_CD", "DMSL_CD"]
                ).count().reset_index()
                cmd_uics_needed = ac_cmd_uics_needed.set_index("UIC_facesfile").join(
                    cmd_uics_needed.set_index("UIC not in AOS")        
                ).reset_index().rename(
                    columns = {
                        "UIC_facesfile" : "UIC",
                        "SSN_MASK" : "Num Soldiers assigned to UIC"
                    }
                )
        
        # create a DF with a list of UICs that require templets
        cmd_templets_needed = pd.DataFrame        
        if(cmd == "AR"):
            cmd_templets_needed = cmd_df.query(
                'CREATE_TEMPLET == 1.0 and STRUC_CMD_CD == "' + cmd + '"' 
            )[["GFC1", "GFC 1 Name", "UIC_facesfile", "UIC_PATH", "SSN_MASK"]]
            cmd_templets_needed = cmd_templets_needed.groupby(
                ["UIC_facesfile", "UIC_PATH", "GFC1", "GFC 1 Name"]
            ).count().reset_index().rename(
                columns = {
                    "UIC_facesfile" : "UICs requiring templets",
                    "SSN_MASK" : "AR Templet Requirement"
                }
            )
            ar_columns = cmd_templets_needed.columns.tolist()
            ar_columns.remove("GFC1")
            ar_columns.remove("GFC 1 Name")
            ar_columns.insert(0, "GFC 1 Name")
            ar_columns.insert(0, "GFC1")
            cmd_templets_needed = cmd_templets_needed[ar_columns]
        else:
            cmd_templets_needed = cmd_df.query(
                'CREATE_TEMPLET == 1.0 and STRUC_CMD_CD == "' + cmd + '"' 
            )[["DML_CD", "DMSL_CD", "UIC_facesfile", "UIC_PATH", "SSN_MASK"]]
            cmd_templets_needed = cmd_templets_needed.groupby(
                ["UIC_facesfile", "UIC_PATH", "DML_CD", "DMSL_CD"]
            ).count().reset_index().rename(
                columns = {
                    "UIC_facesfile" : "UICs requiring templets",
                    "SSN_MASK" : "AR Templet Requirement"
                }
            )
            ac_columns = cmd_templets_needed.columns.tolist()
            ac_columns.remove("DML_CD")
            ac_columns.remove("DMSL_CD")
            ac_columns.insert(0, "DMSL_CD")
            ac_columns.insert(0, "DML_CD")
            cmd_templets_needed = cmd_templets_needed[ac_columns]
            
        if(cmd_templets_needed.shape[0] > 0):
            print(cmd_templets_needed.columns)
            cmd_templets_needed = cmd_templets_needed.reset_index(
                drop = True        
            ).set_index("UICs requiring templets").join(
                address_data.reset_index().set_index("UIC")[[
                    "STACO", "ARLOC", "PH_CITY_TXT", "PH_GEO_TXT", 
                    "PH_POSTAL_CODE_TXT", "PH_COUNTRY_TXT"        
                ]]        
            ).reset_index().rename(columns = {"index" : "UICs requiring templets"})
        
        export_path = file_config['MASKED_CMD_METRICS_EXPORT_PATH']
        unmask_file_name_modifier = ""
        
        # include an option to export unmasked metrics
        if(run_config['EXPORT_UNMASKED_CMD_SPECS']):
            print(" - Unmasking command metrics package detail worksheet")
            cmd_df = unmask.unmask_and_return(file_config, cmd_df)
            export_path = file_config['UNMASKED_CMD_METRICS_EXPORT_PATH']
            unmask_file_name_modifier = "_unmasked"
        
        # export to a file
        print(" - Saving " + cmd + " metrics as file to " + export_path)
        with pd.ExcelWriter(
            export_path + 
            cmd + 
            unmask_file_name_modifier +
            "_f2s_metrics" + 
            file_config["EXPORT_FILENAME_NOTE"] +
            date_time_string + 
            ".xlsx"
        ) as writer:
            cmd_df.to_excel(
                writer, sheet_name = "face-space-detail"
            )
            cmd_uics_needed.to_excel(
                writer, sheet_name = "UICs-not-in-AOS"
            )
            cmd_templets_needed.to_excel(
                writer, sheet_name = "Templets-to-build"
            )
            if cmd == "AR":
                curorg_metrics.to_excel(
                    writer, sheet_name = "CURORG Metrics"            
                )
                ar_cmd_metrics.to_excel(
                    writer, sheet_name = "AR CMD Metrics"        
                )
    
    
