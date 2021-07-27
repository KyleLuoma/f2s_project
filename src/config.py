# -*- coding: utf-8 -*-

def get_run_config():
    run_config = {
        'LOAD_MATCH_PHASES'              : True,
        'LOAD_AND_PROCESS_MAPS'          : True,
        'LOAD_COMMAND_CONSIDERATIONS'    : True,
        'PROCESS_COMMAND_CONSIDERATIONS' : True,
        'LOAD_AND_PROCESS_SPACES'        : True,
        'LOAD_AND_PROCESS_ADDRESS_DATA'  : True,
        'LOAD_EMILPO_FACES'              : True,
        'LOAD_EMILPO_TEMP_ASSIGNMENTS'   : True,
        'LOAD_RCMS_FACES'                : True,
        'VERBOSE'                        : False,
        'RUN_MATCH'                      : True,
        'RUN_MATCH_DIAGNOSTICS'          : True,
        'GENERATE_CMD_METRICS'           : True,
        'EXPORT_F2S'                     : False,
        'EXPORT_UNMATCHED'               : False,
        #Export ONLY to your local drive, not to a network folder:
        'EXPORT_UNMASKED'                : False, 
        'UPDATE_CONNECTIONS'             : False,
        'EXPORT_CMD_SPECS'               : False,
        'EXPORT_UNMASKED_CMD_SPECS'      : False,
        'COMMAND_EXPORT_LIST' : [] #Leave empty to export all commands
		# Find list of all commands in ./export/for_connections/ac_cmd_metric_dashboard_connected.xlsx
    }
    return run_config

def get_file_config():
    file_config = {
        'DATA_PATH' : "Y:/AOS/master_files",
        #Update this to reflect the current key files for unmasking
        'AC_KEY_DATE'           : "06-09-2021",
        'AR_KEY_DATE'           : "06-09-2021",
        'KEY_PATH'              : "C:/Users/LuomaKR/Documents/",
        #Update to match dates on faces data queries from emilpo and tapdb-r
        'TAPDBR_FILE_DATE'      : "07-08-2021",
        'EMILPO_FILE_DATE'      : "07-08-2021",
        'EMILPO_TEMP_FILE_DATE' : "07-08-2021",
        #Update this if using RCMS data as source
        'USAR_DATA_SOURCE'      : "tapdbr", #select "tapdbr" or "rcms"
        'RCMS_FILE'             : "USAR_BDE_SELRES_F2S_19MAY_FINAL_corrected.xlsx",
        'APART_FILE'            : "USAR_AGR_F2S_17MAY_FINAL.XLSX",
        'RCMS_IMA_FILE'         : "IMA_hoy96_all_20200505_Hash.xlsx",
        #Update to match dates on AOS billet and UIC exports
        'AOS_FILE_DATE'         : "07-08-21",
        'UIC_TREE_DATE'         : "07-08-21",
        'WARCFF_PARTITION_COUNT' : 5,
        #Update to reflect dates or file names of misc data input files
        'DRRSA_FILE_DATE'       : "6-11-2021",
        'UIC_ADDRESS_FILE'      : "textfile_tab_1269578455_UIC_LOCNM_11491036.txt",
        'PHASES_FILE'           : "match_phases mos mismatch last.csv",
        #Update absolute paths for command metric exports
        'MASKED_CMD_METRICS_EXPORT_PATH'   : "Y:/AOS/f2s_project/export/cmd_metrics/",
        'UNMASKED_CMD_METRICS_EXPORT_PATH' : "C:/Users/LuomaKR/Documents/",
        #Export File Name note - addes whatever string value you enter here between the file name and date stamp
        'EXPORT_FILENAME_NOTE'             : "_FY21_8_JUL_AOS_TO_8_JUL_FACES_"
    }
    
    file_config['AC_KEY_FILE'] = file_config['KEY_PATH'] \
        + 'emilpo assignments map ' + file_config['AC_KEY_DATE'] + '.csv'
    file_config['AR_KEY_FILE'] = file_config['KEY_PATH'] \
        + 'tapdbr assignments map ' + file_config['AR_KEY_DATE'] + '.csv'
        
    return file_config

