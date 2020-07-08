# -*- coding: utf-8 -*-



def templet_usage_by_uic(all_spaces_to_matched_faces):
    templet_spaces = all_spaces_to_matched_faces.where(
        all_spaces_to_matched_faces.IS_TEMPLET        
    ).dropna(how = "all")
    encumbered_templets = templet_spaces.where(
        templet_spaces.stage_matched_face_space_match == 0        
    ).dropna(how = "all")
    uic_templets = templet_spaces.reset_index()[[
        "UIC_spaces", "FMID"
    ]].groupby("UIC_spaces").count().rename(columns = {
        "FMID" : "Total Templets"
    })
    uic_templets_used = encumbered_templets.reset_index()[[
        "UIC_spaces", "FMID"        
    ]].groupby("UIC_spaces").count().rename(columns = {
        "FMID" : "Encumbered Templets"        
    })
    uic_templets = uic_templets.join(uic_templets_used)
    uic_templets.reset_index(inplace = True)
    uic_templets = uic_templets.rename(columns = {"UIC_spaces" : "UIC"})
    return uic_templets
    
    