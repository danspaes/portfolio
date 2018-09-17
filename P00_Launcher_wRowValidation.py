#!/usr/bin/python
# -*- coding: utf-8 -*-

# Script developped by the Big Data Team
# Author: Maxime Sirois: maxime.sirois@bnc.ca
# Last update: 2018-05-15 15:15

# This version (2.0.3)
# Since last version:
# - Version 0.0 <-- Initial
# - Version 0.1 <-- Cleansed log traces
# - Version 0.2 <-- Added Dispatcher part
# - Version 0.3 <-- Rerooting stdout to log for priting issues
# - Version 0.4 <-- Light fixes in messages displaying.
#									- Switch script for md5-validation (python version)
# - Version 0.5 <-- Added different GET types.
#									- BatchId provided for Control Script
# - Version 0.6 <-- Cleansing in the code + skip get implemented.
# - Version 0.7 <-- Create HDFS and Local Folders
# - Version 0.8 <-- Change the scripts names (P0X..)
# - Version 0.8.1 <-- Log appearance change
# - Version 0.9 <-- Added Run_Status
# - Version 0.10 <-- Condition update: no more flags in the main loop.
# - Version 0.11 <-- Added: cleansing script
# - Version 0.12 <-- Added: copybook path
# - Version 0.13 <-- Added: oschdir: schedule ready
# - Version 0.14 <-- Added: rerouting logs
# - Version 0.15 <-- Added: timestamp creation and heritage to Control and Filter scripts.
# - Version 0.16 <-- Added: profiling
# - Version 0.17 <-- Added: passing INGEST_TMSP to profiling
# - Version 0.18 <-- Added: generate DDL script
#                  - Modified: Cleansing script name: from P12 to P13
#                  - Modified: End of ingestion timestamp: now based on DDL_CREATE, since it is the last script to run
# - Version 0.19 <-- Historical Load Flag added. If yes, pattern takes the date in the file name for treatments
#                    and there is no profiling.
# - Version 0.20 <-- Do DDL creation before Ingest Script
# 								 - Add Ordered list file for ingestion
# - Version 0.21 <-- Upgrade: add an option to know if we do profiling or not
# - Version 0.22 <-- Upgrade: add business column upgrade
# - Version 0.23 <-- Upgrade: partition date is now corresponding to business date
# - Version 0.24 <-- Upgrade: added date time length 12 for YYYYMMDDHHMM
# - Version 0.25 <-- Upgrade: passing partition date to filter script to update the CTRL table with the partition date.
# - Version 0.26 <-- Upgrade: - Adding the HDR folder path and passing it to control script.
#														  - Error message when there are no files to treat for information (handles frequent error)
# - Version 0.27 <-- Upgrade: passes table name instead of file name to GenerateDDL script.
# - Version 0.28 <-- Upgrade: now handles dates with length 6
# - Version 0.29 <-- FIX: Continue loop if control finds an empty file
# - Version 0.30 <-- Upgrade: new parameter to script that indicates which data folder to choose,
#                    it is used to centralize scripts at one place while keeping the latitude
#                    to use another data directory.
#                  - Upgrade: Added ddl path and passing it to the GenerateDDL Script
#                  - Upgrade: Added custom message while updating control table for empty files (related to version 0,29)
# - Version 1.0 <-- New major version:
#                   - RUN_STATUS is checked before each run
#                   - Logging upgrades
#                   - New parameters to createhdfsfolder script
#                   - New parameters to filter script
#                   - New parameters to md5 script
#                   - Deleting folder contents if a script crashes
# - Version 1.1 <-- Fix: shut verbose on moveFile function
# - Version 1.2 <-- Upgrade: bypass md5 script if no md5
# - Version 1.3 <-- Upgrade: file sizes, md5 and nb of lines in Launcher instead of Filter
# - Version 1.3.1 <-- BugFix: Grab the real extracted file name after unzipping.
# - Version 1.4 <-- Update: changed hard paths
#                 - Modified: New parameters for filter script
# - Version 1.5 <-- Update: create ini file that contains python binary + data path
#                 - Sending python_exe to uniform script
# - Version 1.6 <-- Fix: global sql queries, insert stop timestamp
# - Version 2.0 <-- New Major Version:
#                 - More advanced ini config file, so every script can call it instead of passing parameters
#                 - Parallelism
#                 - Single file ingestion
#                 - Automode (replacing P00_INGEST)
# - Version 2.0.1 <-- (2018-05-03 12:31) Hotfix: continue if file not found in parameter temp file
# - Version 2.0.2 <-- (2018-05-04 14:13) Hotfix: exit without error if no files to treat
# - Version 2.0.3 <-- (2018-05-15 15:15) Hotfix: changed grep file after unzip cause files had different name
# - Version 2.0.4 <-- (2018-08-14 15:27) Hotfix: changed to treat only files with records (row greater than 0 on files without header and row greater than 1 on files with header)

# Description:
# Launcher for the Generic Ingestion Pattern

# Imports
import utils.raw_ingest_func as utils
import os
import sys
import argparse
import subprocess
import time
from datetime import datetime


#
# Functions #
#
def run_script(pythonexe, scriptname, arglist):
    """User-defined function to run a given script"""
    # Building command with arguments
    cmdList = [pythonexe, scriptname]
    for arg in arglist:
        cmdList.append(arg)
    # utils.write_log(log,u"Running Command: {}".format(cmdList))
    proc = subprocess.Popen(cmdList, shell=False, stderr=subprocess.PIPE, stdout=log)
    proc.wait()
    out, err = proc.communicate()
    return proc.returncode, str(err)


def read_file_to_list(path):
    """
    Read Lines and return list
    """
    if os.path.exists(path):
        with open(path, 'r') as f:
            tab = f.readlines()
        return [e.replace('\n', '') for e in tab]  # Remove \n


def clean_environment(*args):
    """
    Clean environment of every temporary file for a run
    """
    for f in args:
        if os.path.exists(f):
            utils.write_log(log, "[INFO] Deleting '{}'".format(f))
            utils.delete_file(f)


def error_display(errormsg):
    errormsg = errormsg.split('\n')
    utils.write_log(log, '--------- ERROR DIAGNOSTIC ---------')
    for e in [e for e in errormsg if e.strip() != '']:
        utils.write_log(log, '[ERROR] ' + e)
    utils.write_log(log, '------------------------------------')


#
# Main Script #
#
if __name__ == "__main__":
    # Get date/time
    BATCH_ID = time.strftime("%Y%m%d%H%M%S")
    
    #
    # Parameters handling #
    #
    parser = argparse.ArgumentParser(
        description="Usage: python P00_Launcher.py <source> [-p] [-d <dataFolder>] [-f <SRCE_NM>] [-s]",
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("srcmask", metavar="SOURCE_MASK", type=str, help="The source mask (ex: co).")
    parser.add_argument("-f", "--singlefile", type=str, help="(Optionnal) SRCE_NM of file you wish to ingest.")
    parser.add_argument("-d", "--datadir", type=str, help="(Optionnal) Directory of data folder user wishes to use.")
    parser.add_argument("-p", "--profiling", action="store_true", help="(Optional) Activates profiling.")
    parser.add_argument("-a", "--automode", action="store_true",
                        help="(Optional) Auto Mode fetches the next file to ingest automatically.")
    
    args = parser.parse_args()
    
    # Capture parameters
    SOURCE_MASK = str(args.srcmask).lower()  # Source name converted to lower case
    # If there is a user specified data folder.
    if args.datadir:
        DATA_FOLDER = str(args.datadir)
    else:
        DATA_FOLDER = "data"  # Default value
    
    #
    # ------------ CONFIG ------------ #
    #
    SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))  # Actual Script Folder
    SCRIPT_BASE_DIR = os.path.dirname(SCRIPT_DIR)  # ../
    LOG_PATH = os.path.join(SCRIPT_BASE_DIR, "log/")
    COPYBOOKS_PATH = os.path.join(SCRIPT_BASE_DIR, "copybooks/")
    INI_CONFIG_PATH = os.path.join(SCRIPT_DIR, 'param', 'config.ini')
    # Change CWD
    os.chdir(SCRIPT_DIR)
    # Parse settings
    d_config = utils.parse_ini(INI_CONFIG_PATH)
    
    #
    # ------------ FAIL FAST ------------ #
    #
    # --> Exit script if one of those configuration is not set properly
    # Validate if all config file sections are present
    if not all(k in d_config.keys() for k in ('MYSQL', 'PATHS', 'HADOOP', 'SYSTEM')):
        raise utils.ScriptError("Config file doesn't hold all correct sections: 'MYSQL','PATHS','HADOOP','SYSTEM'")
    
    #
    # ------------ PATHS ------------ #
    #
    d_config_paths = d_config.get('PATHS')  # Define dict for paths
    # MAIN #
    DATA_BASE_DIR = d_config_paths.get('DATA_BASE_DIR')
    ARCHIVE = d_config_paths.get('ARCHIVE')
    ARC_QUARANTINE = d_config_paths.get('ARC_QUARANTINE')
    HDFS_PATH = d_config_paths.get('HDFS_PATH')
    HDFS_PROFILING_PATH = d_config_paths.get('HDFS_PROFILING_PATH')
    
    SOURCE_PATH = os.path.join(DATA_BASE_DIR, DATA_FOLDER, 'input/edhingest')
    DATADROP_PATH = os.path.join(DATA_BASE_DIR, DATA_FOLDER, 'input/dataDropArea')
    POOL_PATH = os.path.join(DATA_BASE_DIR, DATA_FOLDER, 'input/pool')
    
    # SOURCE SPECIFIC #
    LZ_PATH = os.path.join(SOURCE_PATH, SOURCE_MASK, 'lz')
    WRK_PATH = os.path.join(SOURCE_PATH, SOURCE_MASK, 'wrk')
    UTF_PATH = os.path.join(SOURCE_PATH, SOURCE_MASK, 'utf8')
    TEMP_PATH = os.path.join(SOURCE_PATH, SOURCE_MASK, 'temp')
    PROFILING_PATH = os.path.join(SOURCE_PATH, SOURCE_MASK, 'profiling')
    QUARANTINE_PATH = os.path.join(SOURCE_PATH, SOURCE_MASK, 'quarantine')
    EXCLUSIONS_PATH = os.path.join(SOURCE_PATH, SOURCE_MASK, 'exclusions')
    HDR_PATH = os.path.join(SOURCE_PATH, SOURCE_MASK, 'hdr')
    DDL_PATH = os.path.join(SOURCE_PATH, SOURCE_MASK, 'ddl')
    UNIFORM_PATH = os.path.join(SOURCE_PATH, SOURCE_MASK, 'uniform')
    # SYSTEM #
    PYTHON_EXE = d_config.get('SYSTEM').get('PYTHON_EXE')
    ENV = d_config.get('SYSTEM').get('ENV').upper()  # Environment (TU,TI,PPROD,PROD)
    # PARAM FILE #
    BATCH_KEY = BATCH_ID + '-' + utils.random_generator(10)
    PARAM_FILE_PATH = os.path.join(TEMP_PATH, SOURCE_MASK + '_' + BATCH_KEY + '.tmp')
    LIST_FILE_PATH = os.path.join(TEMP_PATH, 'FILELIST_' + BATCH_KEY + '.tmp')
    
    #
    # ---------- MAIN PROGRAM STARTING ---------- #
    #
    logFileName = "{}_INGESTION_{}.log".format(SOURCE_MASK.upper(), BATCH_KEY)
    with open(os.path.join(LOG_PATH, logFileName), 'w+') as log:
        #
        # /////// FAIL FAST  \\\\\\\\ #
        #
        # --> Exit script if one of those configuration is not set properly
        # Validate if chosen data folder exists
        if not os.path.exists(SOURCE_PATH):
            utils.write_log(log, "[ERROR] Data Folder chosen does not exist: '{}'".format(DATA_FOLDER))
            raise IOError(2, 'Data Folder chosen does not exist', DATA_FOLDER)
        
        if not ENV in ['TU', 'TI', 'PPROD', 'PROD']:
            raise utils.ScriptError('Config ENV parameter should contain: TU, TI, PPROD or PROD.')
        
        #
        # PARAMETERS OUTPUT #
        #
        utils.write_log(log, "################## LAUNCHER ##################")
        utils.write_log(log, "[INFO] Running script on: {}".format(SOURCE_PATH))
        utils.write_log(log, "[INFO] Interpreter used: '{}'".format(PYTHON_EXE))
        utils.write_log(log, "[INFO] Batch Key: {}".format(BATCH_KEY))
        if args.singlefile:
            utils.write_log(log, "[OPTION] Single file mode: ON")
        if args.profiling:
            utils.write_log(log, "[OPTION] Profliling: ON")
        if args.automode:
            utils.write_log(log, "[OPTION] Auto Mode: ON")
        utils.write_log(log, "")
        utils.write_log(log, "[ACTION] Connecting to the database...")
        try:
            db = utils.Database(d_config.get('MYSQL'))  # Initiate DB connection with MYSQL config
        except Exception as e:
            utils.write_log(log, "[ERROR] Database Connection Fail: {}".format(str(e)))
            raise utils.DatabaseError(e)
        else:
            utils.write_log(log, "")
            utils.write_log(log, "---------- -------------------- ----------")
            utils.write_log(log, "---------- CREATE LOCAL FOLDERS ----------")
            utils.write_log(log, "---------- -------------------- ----------")
            argList = [SOURCE_MASK, SOURCE_PATH]
            returncode, err = run_script(PYTHON_EXE, "P02_CreateLocalFolders.py", argList)
            if returncode != 0:
                error_display(err)
                raise utils.ScriptError(err)
            
            if args.automode:
                utils.write_log(log, "")
                utils.write_log(log, "---------- --------- ----------")
                utils.write_log(log, "---------- AUTO-MODE ----------")
                utils.write_log(log, "---------- --------- ----------")
                # Make a list of desired files to fetch
                if args.singlefile:
                    srceNm2Fetch = [str(args.singlefile).strip()]
                else:
                    sqlQuery = """SELECT SRCE_NM
                    FROM INGEST_CTRL
                    WHERE CODE_APPL='{}';""".format(SOURCE_MASK)
                    result, header = db.sql(sqlQuery)
                    srceNm2Fetch = [e[0] for e in result]
                
                # Call script in a loop over list
                filesFetched = False
                for SRCE_NM in srceNm2Fetch:
                    utils.write_log(log, "[ACTION] Fetching: '{}'".format(SRCE_NM))
                    returncode, err = run_script(PYTHON_EXE, "P00_Fetch.py", [SOURCE_MASK, SRCE_NM, DATADROP_PATH])
                    if returncode == 2:
                        continue
                    elif returncode != 0:
                        error_display(err)
                        raise utils.ScriptError(err)
                    elif returncode == 0:
                        filesFetched = True
                
                if not filesFetched:
                    utils.write_log(log, "")
                    utils.write_log(log, "[WARNING] No new files to ingest, script ends.")
                    sys.exit(0)
            
            utils.write_log(log, "")
            utils.write_log(log, "---------- ---------- ----------")
            utils.write_log(log, "---------- DISPATCHER ----------")
            utils.write_log(log, "---------- ---------- ----------")
            argList = [SOURCE_MASK, SOURCE_PATH, INI_CONFIG_PATH, DATADROP_PATH, LIST_FILE_PATH]
            if args.singlefile:
                argList.append("-r {}".format(args.singlefile))  # If single file mode is activated, pass regex
            returncode, err = run_script(PYTHON_EXE, "P03_Dispatcher.py", argList)
            if returncode != 0:
                error_display(err)
                raise utils.ScriptError(err)
            
            utils.write_log(log, "")
            utils.write_log(log, "---------- ----------- ----------")
            utils.write_log(log, "---------- PARAM SETUP ----------")
            utils.write_log(log, "---------- ----------- ----------")
            argList = [SOURCE_MASK, INI_CONFIG_PATH, PARAM_FILE_PATH]
            if args.singlefile:
                argList.append("-f {}".format(args.singlefile))  # Build parameter file for SRCE_NM only
            returncode, err = run_script(PYTHON_EXE, "P04_ParamSetup.py", argList)
            if returncode != 0:
                error_display(err)
                raise utils.ScriptError(err)
            
            utils.write_log(log, "")
            utils.write_log(log, "---------- ------------------- ----------")
            utils.write_log(log, "---------- CREATE HDFS FOLDERS ----------")
            utils.write_log(log, "---------- ------------------- ----------")
            argList = [SOURCE_MASK, HDFS_PATH, PARAM_FILE_PATH]
            returncode, err = run_script(PYTHON_EXE, "P05_CreateHdfsFolders.py", argList)
            if returncode != 0:
                error_display(err)
                raise utils.ScriptError(err)
            
            #
            # MAIN LOOP #
            #
            utils.write_log(log, "")
            utils.write_log(log, "---------- ------------------ ----------")
            utils.write_log(log, "---------- STARTING INGESTION ----------")
            utils.write_log(log, "---------- ------------------ ----------")
            d_param = utils.parse_param_file(PARAM_FILE_PATH)
            if not d_param:
                utils.write_log(log, "[ERROR] Oops, Seems like you have no file to run after all.")
                utils.write_log(log, "[ERROR] Make sure the RUN_STATUS is OK and ACTIVE_IND is Y")
                utils.write_log(log, "--> Deleting '{}'".format(PARAM_FILE_PATH))
                utils.write_log(log, "--> Deleting '{}'".format(LIST_FILE_PATH))
                clean_environment(PARAM_FILE_PATH, LIST_FILE_PATH)
                raise utils.CtrlTableError("No files to process: check RUN_STATUS and ACTIVE_IND")
            
            fileList = utils.extract_param_column(d_param, 'SRCE_NM')  # Extract SRCE_NM column from param file
            # Make the list of the files in LZ
            lzFiles = read_file_to_list(LIST_FILE_PATH)  # Read file list from temp file
            # Remove the md5 for the looping list
            lzFiles = [e for e in lzFiles if not e.lower().endswith(".md5")]
            # Order the file list to ingest by Date, Name
            lzFiles = [utils.File(f) for f in lzFiles]
            lzFiles = sorted([[f.name, f.tablename, f.normalized_date] for f in lzFiles], key=lambda x: (x[2], x[1]))
            
            lzFiles = [e[0] for e in lzFiles]
            
            utils.write_log(log, "Files to treat:")
            for e in lzFiles:
                utils.write_log(log, "- " + e)
            
            if not lzFiles:
                utils.write_log(log, "[WARNING] No files to treat, program ending.")
                utils.write_log(log, "[WARNING] Verify if source status is 'OK' and ACTIVE_IND is 'Y'.")
                utils.write_log(log, "[NOTE] You can check if the file name matches the CTRL table's.")
                utils.write_log(log, "[NOTE] --> File name should be equal to SRCE_NM")
                utils.write_log(log, "[NOTE] --> Matching is also case sensitive")
                sys.exit(0)
            
            for f in lzFiles:
                utils.write_log(log, "")
                utils.write_log(log, "=========================================================================")
                utils.write_log(log, "=================-------------------------------------===================")
                utils.write_log(log, "=========================================================================")
                # Description #
                # For the main loop, we basically need to loop over each file to treat them end to end.
                file = utils.File(f)  # Create file object
                # file.tablename = utils.getTableName(file) # Extract table name in file
                # file.date = utils.getFileDate(file) # Extract date in file
                # Creating timestamp
                execTmsp = time.strftime("%Y-%m-%d %H:%M:%S")
                
                # Code to verify if the file is empty added on 14-08-2018
                
                sqlQuery = """SELECT SCHEM_NM, COL_HDR, HEADER_LN, FOOTER_LN, COMP_TYPE
                FROM INGEST_PARAM
                WHERE CODE_APPL='{}' and SRCE_NM='{}';""".format(SOURCE_MASK, file.tablename)
                result, header = db.sql(sqlQuery)
                
                schemNm = result[0][0].strip().upper()
                colHdrFlag = result[0][1].strip().upper()
                hdrLines = result[0][2].strip()
                fterLines = result[0][3].strip()
                compType = result[0][4].strip().upper()
                
                if compType in ['ZIP','GZ','BZ2','GZIP']:

                    proc = subprocess.Popen("zgrep -Ec \"\$\" " + LZ_PATH + "/" + file.name, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
                    proc.wait()
                
                elif compType == 'N/A':

                    proc = subprocess.Popen("grep -Ec \"\$\" " + LZ_PATH + "/" + file.name, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
                    proc.wait()
                
                amtLineFile, err = proc.communicate()
                
                if (colHdrFlag == 'N' and amtLineFile.strip() == '0') or (colHdrFlag == 'Y' and amtLineFile.strip() == '1') or amtLineFile.strip() == '0':
                    utils.write_log(log, "[ERROR] No rows in file {}: Skipping file.".format(file.name))
                    utils.write_log(log, "[ERROR] Moving to exclusions: '{}'".format(EXCLUSIONS_PATH))
                    utils.move_file(os.path.join(LZ_PATH, file.name), EXCLUSIONS_PATH)
                    
                    db.sql(utils.glob_SQL_update_CTRL.format('ABND', datetime.now().strftime('%Y-%m-%d'),datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'Execution stopped because the file is empty', SOURCE_MASK, schemNm, file.tablename))

                    db.sql(utils.glob_SQL_inserIntoCTRL.format(SOURCE_MASK, schemNm, file.tablename))
                    continue

                # end of the Code to verify if the file is empty
               
                elif not file.date:
                    utils.write_log(log, "[ERROR] No date in file name: Skipping file.")
                    utils.write_log(log, "[ERROR] Moving to exclusions: '{}'".format(EXCLUSIONS_PATH))
                    utils.move_file(os.path.join(LZ_PATH, file.name), EXCLUSIONS_PATH)

                    # Addition to update control tables:

                    db.sql(utils.glob_SQL_update_CTRL.format('ABND', datetime.now().strftime('%Y-%m-%d'),datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'Execution stopped because there are no date in file name', SOURCE_MASK, schemNm, file.tablename))

                    db.sql(utils.glob_SQL_inserIntoCTRL.format(SOURCE_MASK, schemNm, file.tablename))
                    continue
                
                elif len(file.date) == 6:
                    businessDt = datetime.strptime(file.date, '%Y%m').strftime('%Y-%m-%d')
                else:
                    try:
                        businessDt = datetime.strptime(file.normalized_date, '%Y%m%d%H%M%S').strftime('%Y-%m-%d')
                    except ValueError:
                        utils.write_log(log,
                                        "[ERROR] Date in file name does not exist: {}".format(file.normalized_date))
                        utils.write_log(log, "[ERROR] Moving to exclusions: '{}'".format(EXCLUSIONS_PATH))
                        utils.move_file(os.path.join(LZ_PATH, file.name), EXCLUSIONS_PATH)
                        
                        # Addition to update control tables:

                        db.sql(utils.glob_SQL_update_CTRL.format('ABND', datetime.now().strftime('%Y-%m-%d'),datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'Execution stopped because date in file name does not exist', SOURCE_MASK, schemNm, file.tablename))

                        db.sql(utils.glob_SQL_inserIntoCTRL.format(SOURCE_MASK, schemNm, file.tablename))

                        continue
                
                partitionDt = str(businessDt)
                
                # Parameters
                d_param_file = d_param.get(file.tablename)  # Extract parameters for specific file
                # Verify if file was in parameter table
                if not d_param_file:
                    utils.write_log(log,
                                    "[WARNING] Previous file '{}' was either ABND or not active.".format(file.name))
                    utils.write_log(log, "[ACTION] Moving to exclusions")
                    utils.move_file(os.path.join(LZ_PATH, file.name), EXCLUSIONS_PATH)
                    continue
                
                codeAppl = d_param_file.get('CODE_APPL').strip()
                schemNm = d_param_file.get('SCHEM_NM').strip()
                srceNm = d_param_file.get('SRCE_NM').strip()
                compType = d_param_file.get('COMP_TYPE').upper().strip()
                
                # Create path to file which will contain column names and widths
                hdrFile = os.path.join(TEMP_PATH, file.tablename + '_' + BATCH_KEY + '_hdrs_widths.tmp')
                
                # Extract the current RUN_STATUS from DB
                runStatus = \
                    db.sql("SELECT RUN_STATUS FROM INGEST_CTRL where SRCE_NM='{}' and CODE_APPL='{}';".format(
                        file.tablename, SOURCE_MASK))[0][0][0].upper()
                
                # Extract the current DDL_CREATE from DB
                createDDL = \
                    db.sql("SELECT DDL_CREATE FROM INGEST_PARAM where SRCE_NM='{}' and CODE_APPL='{}';".format(
                        file.tablename, SOURCE_MASK))[0][0][0].upper()
                
                #
                # FILE INFO #
                #
                utils.write_log(log, "------------ FILE INFO ------------")
                utils.write_log(log, "- CODE_APPL:  {}".format(codeAppl))
                utils.write_log(log, "- SCHEM_NM:   {}".format(schemNm))
                utils.write_log(log, "- SRCE_NM:    {}".format(srceNm))
                utils.write_log(log, "- COMP_TYPE:  {}".format(compType))
                utils.write_log(log, "- TABLE NAME: {}".format(file.tablename))
                utils.write_log(log, "- FILE DATE:  {}".format(file.date))
                utils.write_log(log, "- RUN STATUS: {}".format(runStatus))
                utils.write_log(log, "- DDL CREATE: {}".format(createDDL))
                utils.write_log(log, "--------------- END ---------------")
                
                #
                # VALIDATE THE RUN_STATUS #
                #
                if runStatus != 'OK':
                    utils.write_log(log, "[WARNING] Couldn't load '{}', because RUN_STATUS = '{}'".format(file.name,
                                                                                                          runStatus))
                    utils.write_log(log, "[WARNING] Moving to exclusions: '{}'".format(EXCLUSIONS_PATH))
                    utils.move_file(os.path.join(LZ_PATH, file.name), EXCLUSIONS_PATH)
                else:
                    #
                    # FILTER #
                    #
                    utils.write_log(log, "")
                    utils.write_log(log, "---------- ------ ----------")
                    utils.write_log(log, "---------- FILTER ---------- " + file.name)
                    utils.write_log(log, "---------- ------ ----------")
                    argList = [SOURCE_MASK, INI_CONFIG_PATH, LZ_PATH, WRK_PATH, PARAM_FILE_PATH, ARCHIVE,
                               ARC_QUARANTINE, file.name, execTmsp, partitionDt, BATCH_ID, logFileName]
                    returncode, err = run_script(PYTHON_EXE, "P06_Filter.py", argList)
                    if returncode != 0:
                        error_display(err)
                        db.sql(utils.glob_SQL_update_CTRL.format('ABND', datetime.now().strftime('%Y-%m-%d'),
                                                                 datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                                 'Execution stopped at Filter', codeAppl, schemNm,
                                                                 srceNm))
                        db.sql(utils.glob_SQL_inserIntoCTRL.format(codeAppl, schemNm, srceNm))
                        clean_environment(os.path.join(WRK_PATH, file.name))
                        continue
                    
                    #
                    # UNZIP #
                    #
                    if compType == 'N/A':
                        utils.move_file(os.path.join(WRK_PATH, file.name), UNIFORM_PATH)
                    else:
                        utils.write_log(log, "")
                        utils.write_log(log, "---------- ----- ----------")
                        utils.write_log(log, "---------- UNZIP ---------- " + file.name)
                        utils.write_log(log, "---------- ----- ----------")
                        argList = [WRK_PATH, PARAM_FILE_PATH, file.name]
                        returncode, err = run_script(PYTHON_EXE, "P07_Unzip.py", argList)
                        if returncode != 0:
                            error_display(err)
                            db.sql(utils.glob_SQL_update_CTRL.format('ABND', datetime.now().strftime('%Y-%m-%d'),
                                                                     datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                                     'Execution stopped at Unzip', codeAppl,
                                                                     schemNm, srceNm))
                            db.sql(utils.glob_SQL_inserIntoCTRL.format(codeAppl, schemNm, srceNm))
                            clean_environment(os.path.join(WRK_PATH, file.name))
                            continue
                        else:
                            # If success, we need to move the unzipped file to uniform
                            # For this, we need to fetch the new file name without moving
                            # concurrent process file (as it may run in parallel with other file
                            utils.write_log(log, "[ACTION] Moving File to ../uniform/")
                            # Get the new file name
                            newFileName = \
                            [f for f in os.listdir(WRK_PATH) if utils.File(f).tablename == file.tablename][0]
                            file = utils.File(newFileName)  # Overwrite file object with new file name
                            utils.move_file(os.path.join(WRK_PATH, file.name), UNIFORM_PATH)
                            utils.write_log(log, "[INFO] Extracted File Name: {}".format(file.name))
                    
                    #
                    # GET ORIGINAL UNZIPPED FILE INFO #
                    #
                    utils.write_log(log, "")
                    utils.write_log(log, "---------- ------------------ ----------")
                    utils.write_log(log, "---------- FETCHING FILE INFO ---------- " + file.name)
                    utils.write_log(log, "---------- ------------------ ----------")
                    md5value = utils.get_md5(os.path.join(UNIFORM_PATH, file.name))
                    fileSize = str(os.path.getsize(os.path.join(UNIFORM_PATH, file.name)))
                    nbLines = utils.get_nb_lines(os.path.join(UNIFORM_PATH, file.name))
                    
                    # Update CTRL Table
                    utils.write_log(log, "[ACTION] Updating CTRL table")
                    db.sql(
                        utils.glob_SQL_update_CTRL_info.format(fileSize, md5value, nbLines, codeAppl, schemNm, srceNm))
                    
                    #
                    # UNIFORM #
                    #
                    utils.write_log(log, "")
                    utils.write_log(log, "---------- ------- ----------")
                    utils.write_log(log, "---------- UNIFORM ---------- " + file.name)
                    utils.write_log(log, "---------- ------- ----------")
                    argList = ["P08_UniformCaller.py", file.name, PARAM_FILE_PATH, hdrFile, UNIFORM_PATH, UTF_PATH,
                               COPYBOOKS_PATH, INI_CONFIG_PATH, PYTHON_EXE]
                    returncode, err = run_script(PYTHON_EXE, "-u", argList)
                    if returncode == 1:
                        error_display(err)
                        db.sql(utils.glob_SQL_update_CTRL.format('ABND', datetime.now().strftime('%Y-%m-%d'),
                                                                 datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                                 'Execution stopped at Uniform', codeAppl,
                                                                 schemNm, srceNm))
                        db.sql(utils.glob_SQL_inserIntoCTRL.format(codeAppl, schemNm, srceNm))
                        clean_environment(os.path.join(UNIFORM_PATH, file.name), hdrFile)
                        continue
                    elif returncode == 2:
                        utils.write_log(log, "[INFO] SubFiles were treated successfuly.")
                        utils.write_log(log, "[INFO] Script terminated successfuly.")
                        sys.exit(0)
                    
                    #
                    # CONTROL #
                    #
                    utils.write_log(log, "")
                    utils.write_log(log, "---------- ------- ----------")
                    utils.write_log(log, "---------- CONTROL ---------- " + file.name)
                    utils.write_log(log, "---------- ------- ----------")
                    argList = [SOURCE_MASK, UTF_PATH, HDR_PATH, BATCH_ID, execTmsp, businessDt, file.name,
                               PARAM_FILE_PATH, hdrFile]
                    returncode, err = run_script(PYTHON_EXE, "P09_Control.py", argList)
                    if returncode == 2:  # Condition received that the file trying to be ingested is empty.
                        utils.write_log(log, "--> Updating ctrl status to OK.")
                        db.sql(utils.glob_SQL_update_CTRL.format('OK', datetime.now().strftime('%Y-%m-%d'),
                                                                 datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                                 'FILE WAS EMPTY', codeAppl, schemNm, srceNm))
                        utils.write_log(log, "--> Inserting into CTRL_LOG.")
                        db.sql(utils.glob_SQL_inserIntoCTRL.format(codeAppl, schemNm, srceNm))
                        clean_environment(os.path.join(UTF_PATH, file.name), hdrFile)
                        continue
                    elif returncode != 0:
                        error_display(err)
                        db.sql(utils.glob_SQL_update_CTRL.format('ABND', datetime.now().strftime('%Y-%m-%d'),
                                                                 datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                                 'Execution stopped at Control', codeAppl,
                                                                 schemNm, srceNm))
                        db.sql(utils.glob_SQL_inserIntoCTRL.format(codeAppl, schemNm, srceNm))
                        clean_environment(os.path.join(UTF_PATH, file.name), hdrFile)
                        continue
                    
                    #
                    # PROFILING #
                    #
                    if args.profiling:
                        utils.write_log(log, "")
                        utils.write_log(log, "---------- --------- ----------")
                        utils.write_log(log, "---------- PROFILING ---------- " + file.name)
                        utils.write_log(log, "---------- --------- ----------")
                        argList = [SOURCE_MASK, UTF_PATH, PROFILING_PATH, HDFS_PROFILING_PATH, BATCH_ID,
                                   execTmsp, businessDt, file.name, PARAM_FILE_PATH, hdrFile]
                        returncode, err = run_script(PYTHON_EXE, "P10_Profiling.py", argList)
                        if returncode != 0:
                            error_display(err)
                            db.sql(utils.glob_SQL_update_CTRL.format('ABND', datetime.now().strftime('%Y-%m-%d'),
                                                                     datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                                     'Execution stopped at Profiling', codeAppl,
                                                                     schemNm, srceNm))
                            db.sql(utils.glob_SQL_inserIntoCTRL.format(codeAppl, schemNm, srceNm))
                            clean_environment(os.path.join(UTF_PATH, file.name), hdrFile)
                            continue
                    
                    #
                    # GENERATE DDL #
                    #
                    if createDDL == 'Y':
                        utils.write_log(log, "")
                        utils.write_log(log, "---------- ------------ ----------")
                        utils.write_log(log, "---------- GENERATE DDL ---------- " + file.name)
                        utils.write_log(log, "---------- ------------ ----------")
                        argList = [SOURCE_MASK, DDL_PATH, HDFS_PATH, INI_CONFIG_PATH, file.name, PARAM_FILE_PATH,
                                   hdrFile, ENV]
                        returncode, err = run_script(PYTHON_EXE, "P11_GenerateDDL.py", argList)
                        if returncode != 0:
                            error_display(err)
                            db.sql(utils.glob_SQL_update_CTRL.format('ABND', datetime.now().strftime('%Y-%m-%d'),
                                                                     datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                                     'Execution stopped at DDL', codeAppl,
                                                                     schemNm, srceNm))
                            db.sql(utils.glob_SQL_inserIntoCTRL.format(codeAppl, schemNm, srceNm))
                            clean_environment(os.path.join(UTF_PATH, file.name), hdrFile)
                            continue
                        else:
                            # If everything executes fine; update param table and put DDL_CREATE at "C",
                            # # meaning it has been created already.
                            # So it wont execute for the second run.
                            db.sql(utils.glob_SQL_update_PARAM_DDL.format(codeAppl, schemNm, srceNm))
                            utils.write_log(log, "----- DDL Generated successfuly. -----")
                    
                    #
                    # INGEST #
                    #
                    utils.write_log(log, "")
                    utils.write_log(log, "---------- ------ ----------")
                    utils.write_log(log, "---------- INGEST ---------- " + file.name)
                    utils.write_log(log, "---------- ------ ----------")
                    argList = [SOURCE_MASK, HDFS_PATH, INI_CONFIG_PATH, PARAM_FILE_PATH, UTF_PATH, partitionDt,
                               file.name, ENV]
                    returncode, err = run_script(PYTHON_EXE, "P12_Ingest.py", argList)
                    if returncode != 0:
                        error_display(err)
                        db.sql(utils.glob_SQL_update_CTRL.format('ABND', datetime.now().strftime('%Y-%m-%d'),
                                                                 datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                                 'Execution stopped at Ingest', codeAppl,
                                                                 schemNm, srceNm))
                        db.sql(utils.glob_SQL_inserIntoCTRL.format(codeAppl, schemNm, srceNm))
                        clean_environment(os.path.join(UTF_PATH, file.name), hdrFile)
                        continue
                    else:
                        utils.write_log(log, "----- File ingested successfuly. -----")
                        utils.write_log(log, "--> Updating ctrl status to OK.")
                        db.sql(utils.glob_SQL_update_CTRL_end.format('OK', datetime.now().strftime('%Y-%m-%d'),
                                                                     datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                                     'NULL', codeAppl, schemNm, srceNm))
                        utils.write_log(log, "--> Inserting into CTRL_LOG.")
                        db.sql(utils.glob_SQL_inserIntoCTRL.format(codeAppl, schemNm, srceNm))
                    
                    #
                    # Delete hdrwidth #
                    #
                    # Delete this file after each loop (those are file created files)
                    utils.write_log(log, "")
                    if os.path.exists(hdrFile):
                        utils.write_log(log, "----- FILE CLEANSING -----")
                        utils.write_log(log, "--> Deleting '{}'".format(hdrFile))
                        utils.delete_file(hdrFile)
            
            #
            # CLEANSING #
            #
            # Delete files once it's over (those are batch created files)
            utils.write_log(log, "")
            utils.write_log(log, "----- BATCH CLEANSING -----")
            clean_environment(PARAM_FILE_PATH, LIST_FILE_PATH)
            utils.write_log(log, "")
            utils.write_log(log, "---------- Pattern terminated. ----------")
