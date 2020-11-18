# -*- coding: utf-8 -*-

# Scan a folder for all files

import zipfile
from zipfile import ZipFile
import os

AOS_FILE_DIRECTORIES = (
    "uic_tree",
    "billet_tree/W00EFF",
    "billet_tree/WARCFF",
    "billet_tree/WSTAFF",
    "billet_tree/WUSAFF",
    "non_billet_tree/W00EFF",
    "non_billet_tree/WARCFF",
    "non_billet_tree/WSTAFF",
    "non_billet_tree/WUSAFF"            
)

def unzip_aos_files(file_path):
    aos_path = file_path + "/aos/"
    #cycle through all directories:
    print(" - Unzipping AOS files")
    for directory in AOS_FILE_DIRECTORIES:
        #scan folder for zip files:
        zip_files = find_files_of_extension(aos_path + directory, "zip")
        #scan folder for xlsx files:
        xlsx_files = find_files_of_extension(aos_path + directory, "xlsx")
        #pare out xlsx file names
        xlsx_names = []
        for file in xlsx_files:
            xlsx_names.append(file.split(".")[0])
        # Check if only .zip file exists or if a .zip and a .xlsx file exists
        for file in zip_files:
            # Unzip if only .zip file exists
            if(file.split(".")[0] not in xlsx_names):
                ZipFile.extractall(
                    ZipFile(aos_path + directory + "/" + file),
                    aos_path + directory + "/"
                )
                print("  - Unzipped", file, "to", aos_path + directory)

def find_files_of_extension(directory, extension):
    file_list = os.listdir(path = directory)
    sub_list = []
    for file in file_list:
        if(get_file_extension(file) == extension):
            sub_list.append(file)
    return sub_list

def get_file_extension(file_name):
    name_list = file_name.split(".")
    extension = name_list[len(name_list) - 1]
    return extension






def test_get_file_extension(file_name = "test.test", test_extension = "zip"):
    file_name = file_name + "." + test_extension
    result_extension = get_file_extension(file_name)
    print(file_name, "extension is", result_extension)
    assert(result_extension == test_extension)
    print("test_get_file_extension success")
    
def test_find_files_of_extension(
    directory = "F:/aos/master_files/aos/uic_tree/",
    extension = "zip"
):
    files = find_files_of_extension(directory, extension)
    print(files)
    for file in files:
        assert(extension in file)
    print("test_find_files_of_extension success")
    
