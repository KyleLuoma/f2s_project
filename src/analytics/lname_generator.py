# -*- coding: utf-8 -*-


def derive_gfm_lname(target, acronyms, from_column = "ANAME"):
    #Split the string
    print("Deriving GFM LNames from DRRSA LName or AName")
    for row in target.itertuples():
        name = str(row.ANAME)
        if(row.LNAME != "NKN"):
            name = str(row.LNAME)
        name = name.replace(".", "")
        name = name.replace(",", "")
        name = name.replace("-", " - ")
        name = name.replace("  ", " ")
        name = name.replace("(", " ")
        name = name.replace(")", " ")
        name_list = name.split()
        #Convert to sentence case, exclude acronyms
        for i in range(0, len(name_list)):
            if name_list[i] not in acronyms.Acronym.tolist():
                name_list[i] = name_list[i].capitalize()
        string_index = 0
        #Remove first word if len = 4 and starts with W
        if(name_list[0][0] == 'W' and len(name_list[0]) == 4):
            string_index += 1
        #Drop leading zeroes in first word if len = 4 and starts with 0
        if(name_list[string_index][0] == '0' and len(name_list[string_index]) == 4):
            name_list[string_index] = name_list[string_index].lstrip('0')  
        target.at[row.Index, "GFM_LNAME"] = " ".join(name_list[string_index:len(name_list)])
    return target