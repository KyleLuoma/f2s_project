# -*- coding: utf-8 -*-
import pandas as pd

def unmask_and_export(
        all_faces_to_matched_spaces, 
        timestamp, 
        emilpo_key_date,
        cmd_labels = ""
):
    key_file = pd.read_csv(
        "C:/Users/LuomaKR/Documents/emilpo_maps/emilpo map " + emilpo_key_date + ".csv",
        dtype = {"SSN_MASK_HASH" : "str", "SSN" : "str"}
    ).set_index("SSN_MASK_HASH")
    all_faces_to_matched_spaces = all_faces_to_matched_spaces.join(
        key_file,
        on = "SSN_MASK"        
    )
    del all_faces_to_matched_spaces['SSN_MASK']
    all_faces_to_matched_spaces.to_excel(
        "C:/Users/LuomaKR/Documents/f2s_unmasked/" + 
        cmd_labels 
        + "all_faces_matched_spaces_"
        + timestamp + ".xlsx"
    )
