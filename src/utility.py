# -*- coding: utf-8 -*-

import time as tm
import pyodbc as db
import urllib
import sqlalchemy
import pandas as pd

def get_file_timestamp():
    t = tm.localtime()
    return (
            str(t.tm_year) + "-" +
            str(t.tm_mon)  + "-" +
            str(t.tm_mday) + "-" +
            str(t.tm_hour) + "-" +
            str(t.tm_min)  + "-" +
            str(t.tm_sec)
            )
    
def get_date_string():
    t = tm.localtime()
    return (
        str(t.tm_mon) + "/" + str(t.tm_mday) + "/" + str(t.tm_year)        
    )

def get_local_time_as_string():
    return tm.ctime()

def get_local_time_as_datetime():
    return pd.to_datetime(tm.ctime())

def get_dq_db_connection():
    db_connection = db.connect(r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=X:\AOS\aos_dq_analysis.accdb;')
    return db_connection

def get_sql_engine():
    connection_string = (
        r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=X:\AOS\aos_dq_analysis.accdb;ExtendedAnsiSQL=1;'            
    )
    connection_url = f"access+pyodbc:///?odbc_connect={urllib.parse.quote_plus(connection_string)}"
    engine = sqlalchemy.create_engine(connection_url)
    return engine

# Installing packages:
# pip install --trusted-host pypi.python.org  --trusted-host pypi.org --trusted-host files.pythonhosted.org sqlalchemy-access
    
def reload_spaces():
    spaces = load_army_command_aos_billets()
    spaces = process_aos_billet_export(spaces)
    spaces = add_expected_hsduic(spaces, uic_hd_map, "NA")
    spaces = add_drrsa_data(spaces, drrsa)
    spaces = categorical_spaces(spaces)
    return spaces

def test_stage(
    stage, criteria, faces, spaces, 
    include_only_cmds = [], exclude_cmds = [], exclude_rmks = []
):
    match.match(
            match_phases,  
            faces,
            spaces, 
            stage,
            face_space_match
    )
    
def make_commands_label(command_export_list):
    label = ""
    for cmd in command_export_list:
        label = cmd + "_" + label
    return label

def create_project_directories():
    print(" - Checking project directory structure")
    directory_list = [
        "data",
        "data/processed",
        "export",
        "export/cmd_metrics",
        "export/for_connections"
    ]
    import os
    import os.path as path
    for directory in directory_list:
        if not path.exists("../" + directory):
            print("  - Creating " + directory + " folder")
            os.makedirs("../" + directory)
        
    
# Shell script to add zip to all_faces_to_matched_spaces
# =============================================================================
# all_faces_to_matched_spaces = all_faces_to_matched_spaces.reset_index().join(
#     drrsa.reset_index().set_index("UIC")["UNPRSNTLOCZIP"],
#     on = "UIC_emilpo"
# )    
# all_faces_to_matched_spaces.UNPRSNTLOCZIP = all_faces_to_matched_spaces.apply(
#     lambda row: str(row.UNPRSNTLOCZIP)[0:5],
#     axis = 1      
# )
# =============================================================================
