# -*- coding: utf-8 -*-

def create_address_update_file(
    address_data,
    country_code_xwalk
):
    print(" - Creating address update file to join to UIC file")
    address_update_file = address_data.copy()[[
        "UIC",
        "STACO",
        "GELOC",
        "CITY",
        "STATE",
        "ZIP",
        "COUNTRY"
    ]]
    address_update_file["PH_COUNTRY_TXT"] = "NKN"
    address_update_file = address_update_file.rename(
        columns = {
            "GELOC" : "ARLOC",
            "CITY" : "PH_CITY_TXT",
            "STATE" : "PH_GEO_TXT",
            "ZIP" : "PH_POSTAL_CODE_TXT"
        }        
    )
    country_code_xwalk = country_code_xwalk.reset_index().set_index("name")
    country_key_errors = 0
    for row in address_update_file.itertuples():
        try:
            address_update_file.at[
                row.Index, "PH_COUNTRY_TXT"
            ] = country_code_xwalk.loc[row.COUNTRY].three_char
        except KeyError:
            country_key_errors += 1
    print(
        "  - Encountered", 
        str(country_key_errors), 
        "KeyError exceptions when attempting to map country code to country field from location data"
    )
    address_update_file =  address_update_file[[
        "UIC", 
        "STACO", 
        "ARLOC", 
        "PH_CITY_TXT",
        "PH_GEO_TXT",
        "PH_POSTAL_CODE_TXT",
        "PH_COUNTRY_TXT"
    ]]
    address_update_file.STACO = address_update_file.STACO.astype("str")
    address_update_file.ARLOC = address_update_file.ARLOC.astype("str")
    address_update_file.PH_POSTAL_CODE_TXT = address_update_file.PH_POSTAL_CODE_TXT.astype("str")
    return address_update_file