# -*- coding: utf-8 -*-
import pandas as pd

def unmask_and_export(all_faces_to_matched_spaces, timestamp):
    key_file = pd.read_csv(
        "C:/Users/LuomaKR/Documents/emilpo_maps/emilpo map 4-30-2020.csv",
        dtype = {"SSN_MASK_HASH" : "str", "SSN" : "str"}
    ).set_index("SSN_MASK_HASH")
    all_faces_to_matched_spaces = all_faces_to_matched_spaces.join(
        key_file,
        on = "SSN_MASK"        
    )
    all_faces_to_matched_spaces.to_csv(
        "C:/Users/LuomaKR/Documents/f2s_unmasked/all_faces_matched_spaces_"
        + timestamp + ".csv"
    )
