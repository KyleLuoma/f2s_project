# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

wq1va3_spaces = spaces.where(spaces.UIC == "WQ1VA3").dropna(how = "all")
wq1va3_faces = faces.where(faces.UIC == "WQ1VA3").dropna(how = "all")

match(match_phases, wq1va3_faces, wq1va3_spaces, 1, face_space_match, False)

import pandas as pd

mismatch = pd.read_csv("//ba-anvl-fs05/FMDShare/AOS/master_files/misc/missing_uics.csv")

mismatch_no_na = mismatch.dropna()

mismatch = mismatch.where(~mismatch.HRC_UIC.isin(mismatch_no_na.HRC_UIC)).dropna(how = "all")

in_aos = mismatch.where(mismatch.HRC_UIC.isin(aos.UIC)).dropna(how = "all")
not_in_aos = mismatch.where(~mismatch.HRC_UIC.isin(aos.UIC)).dropna(how = "all")

usar_uic_load = load_data.load_usar_uic_file()

usar_uic_load["UIC_LNAME"] = "NKN"
usar_uic_load["SOURCE"] = "USARG33"

usar_uic_load = usar_uic_load.reset_index().set_index("DUIC").join(
    drrsa[["UIC", "LNAME", "LEVEL_BELOW_AOS", "ASGMT"]].reset_index().set_index("UIC"),
    lsuffix = "_usar",
    rsuffix = "_drrsa"     
).reset_index()

usar_uic_load_backup = usar_uic_load

usar_uic_load.UIC_LNAME = usar_uic_load.apply(
    lambda row: "NKN" if len(str(row.LNAME)) == 0 else row.LNAME,
    axis = 1        
)

usar_uic_load = derive_gfm_lname(usar_uic_load, acronyms, from_column = "UIC_ANAME")
usar_uic_load["PARENT_IN_AOS"] = usar_uic_load.PARENT_UIC.isin(aos.UIC)

usar_orphans = usar_uic_load.where(~usar_uic_load.PARENT_IN_AOS).dropna(how = "all")
usar_orphans.to_csv("./usar_orphans.csv")


#Compare the DUIC and LDUIC generation to usar file and append delta

duic_delta = duic_export.where(
    ~duic_export.isin(usar_uic_load.DUIC)).dropna(how = "all"
).rename(
    columns = {
        "UIC" : "DUIC",
        "CMD" : "ASGMT"
    }
)[[
    "DUIC", "PARENT_UIC", "UIC_LNAME", "UIC_ANAME", "GFM_LNAME", 
    "LEVEL_BELOW_AOS", "ASGMT", "PARENT_IN_AOS"
]]

all_uic_load = usar_uic_load.append(duic_delta)

all_uic_load["GFM_SNAME"] = "NKN"

all_uic_load = all_uic_load[[
    "PARENT_UIC", "DUIC", "UIC_LNAME", "UIC_ANAME", "GFM_LNAME", "GFM_SNAME",
    "LEVEL_BELOW_AOS", "ASGMT", "PARENT_IN_AOS", "SOURCE"       
]]

all_uic_load["LAST_2"] = all_uic_load.apply(
    lambda row: row.DUIC[4:6],
    axis = 1        
)

all_uic_load["SUBCODE_REJECT"] = all_uic_load.apply(
    lambda row: row.LAST_2 in EXCLUDE_CODES,
    axis = 1    
)

all_uic_load.to_excel("../USAR_and_AC_UIC_LOAD_8-25-2020.xlsx")

aos_lduics


