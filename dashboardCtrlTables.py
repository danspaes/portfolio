#!/usr/bin/python
# -*- coding: utf-8 -*-

# Script developped by the Big Data Team
# Author: Daniel Paes: daniel.silvapaes@bnc.ca
# Last update: 2018-08-27 11:45

# This version (2.0.3)
# Since last version:
# - Version 0.0 <-- Initial

# Imports
import utils.raw_ingest_func as utils
import os
import csv
import sys
import argparse
import subprocess
import time
from datetime import datetime
import numpy

# Parse settings
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))  # Actual Script Folder
SCRIPT_BASE_DIR = os.path.dirname(SCRIPT_DIR)  # ../
LOG_PATH = os.path.join(SCRIPT_BASE_DIR, "log/")
COPYBOOKS_PATH = os.path.join(SCRIPT_BASE_DIR, "copybooks/")
INI_CONFIG_PATH = os.path.join(SCRIPT_DIR, 'param', 'config.ini')
d_config = utils.parse_ini(INI_CONFIG_PATH)

if not (k in d_config.keys() for k in ('MYSQL', 'PATHS', 'HADOOP', 'SYSTEM')):
    raise utils.ScriptError("Config file doesn't hold all correct sections: 'MYSQL','PATHS','HADOOP','SYSTEM'")


#
# Function to connect on databases
#

def db_connection(config):
    try:
        db = utils.Database(d_config.get(config))  # Initiate DB connection with MYSQL config
    except Exception as e:
        print("[ERROR] Database Connection Fail: {}".format(e))
        raise utils.DatabaseError(e)
    return db

#
# Function to run the queries
#

def get_cursor_query(sqlQuery):
    dbConn = db_connection('MYSQL')
    try:
        result, header = dbConn.sql(sqlQuery)
    except Exception as e:
        print("[ERROR] Error while generating the subset of data during the query execution: {}".format(e))
        raise utils.DatabaseError(e)

    return result

#
# Main Script #
#

if __name__ == "__main__":

    sqlQuery = """SELECT 
'RAW' as ZONE,
T2.CODE_APPL,
T1.RUN_STATUS as RUN_STATUS,
T2.TRGT_TBL_NM as TRGT_TBL_NM,
FIRST_SUCC_DATE,
LAST_SUCC_DATE
FROM 
(select 
CODE_APPL, 
SRCE_NM, 
RUN_STATUS,
MIN(LAST_RUN_STOP_TMSP) AS FIRST_SUCC_DATE,
MAX(LAST_RUN_STOP_TMSP) AS LAST_SUCC_DATE
FROM
ctrl_ingestion.INGEST_CTRL_LOG
group by CODE_APPL, SRCE_NM having RUN_STATUS = 'OK') T1 
JOIN ctrl_ingestion.INGEST_CTRL T2 
ON T1.CODE_APPL = T2.CODE_APPL AND T1.SRCE_NM = T2.SRCE_NM
UNION ALL
select 
'CURATED' as ZONE,
PROC_NM, 
TRGT_TBL_NM, 
RUN_STATUS,
MIN(LAST_RUN_STOP_TMSP) AS FIRST_SUCC_DATE,
MAX(LAST_RUN_STOP_TMSP) AS LAST_SUCC_DATE
FROM
ctrl_curated.CURATED_CTRL_LOG
group by PROC_NM, TRGT_TBL_NM having RUN_STATUS = 'OK'
UNION ALL
select 
'CONSUMPTION' as ZONE,
PROC_NM, 
TRGT_TBL_NM, 
RUN_STATUS,
MIN(LAST_RUN_STOP_TMSP) AS FIRST_SUCC_DATE,
MAX(LAST_RUN_STOP_TMSP) AS LAST_SUCC_DATE
FROM
ctrl_consumption.CONSUMPTION_CTRL_LOG
group by PROC_NM, TRGT_TBL_NM having RUN_STATUS = 'OK';"""
    
    result = get_cursor_query(sqlQuery)

    try:
       csvTablesOutput = os.path.join(SCRIPT_DIR, 'tables_{}.csv'.format(datetime.now().strftime('%Y%m%d%H%M%S')))

       with open(csvTablesOutput, "w") as output:
           writer = csv.writer(output, delimiter= '|', lineterminator='\n')
           for val in result:
                writer.writerow([val[0], val[1], val[2], val[3], val[4], val[5]])

    except Exception as e:
        print("[ERROR] File Creation failure: {}".format(e))
