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

if not (k in d_config.keys() for k in ('MYSQL', 'PATHS', 'HADOOP', 'SYSTEM', 'METASTORE')):
    raise utils.ScriptError("Config file doesn't hold all correct sections: 'MYSQL','PATHS','HADOOP','SYSTEM','METASTORE'")


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
    dbConn = db_connection('METASTORE')
    try:
        result, header = dbConn.sql(sqlQuery)
    except Exception as e:
        print("[ERROR] Error while generating the subset of data during the query execution: {}".format(e))
        raise utils.DatabaseError(e)

    return result, header

#
# Main Script #
#

if __name__ == "__main__":

    sqlQuery = """SELECT 
T2.NAME
,T1.TBL_NAME
,T4.COLUMN_NAME
,T4.TYPE_NAME
from hive.TBLS T1 
JOIN hive.DBS T2 ON T2.DB_ID = T1.DB_ID 
join hive.SDS T3 ON T3.SD_ID = T1.SD_ID
JOIN hive.COLUMNS_V2 T4 ON T4.CD_ID = T3.CD_ID
where T2.NAME = 'raw_ctrl'
and T4.COLUMN_NAME not in ('sys_curated_tmsp','sys_ingest_tmsp', 'sys_consumption_tmsp', 'part_dt', 'business_process_dt', 'sys_batch_id','sys_source_name')
;"""
    
    result, header = get_cursor_query(sqlQuery)

    try:
       csvTablesOutput = os.path.join(SCRIPT_DIR, 'consCtrltables_{}.csv'.format(datetime.now().strftime('%Y%m%d%H%M%S')))

       with open(csvTablesOutput, "w") as output:
           writer = csv.writer(output, delimiter= '|', lineterminator='\n')
           writer.writerow(header)
	   for val in result:
                writer.writerow([val[0], val[1], val[2], val[3]])

    except Exception as e:
        print("[ERROR] File Creation failure: {}".format(e))
