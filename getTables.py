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

    listOfSources = "\'acbs \',\'acg \',\'adobeanalytics \',\'ageman \',\'bankmate\',\'cardpac\',\'carteclient \',\'cdi \',\'co \',\'conso\',\'croesus \',\'fbs\',\'fis \',\'gbfonds\',\'gua\',\'guichet\',\'IAM\',\'icm \',\'ofce \',\'oss\',\'pat \',\'ph\',\'phenix\',\'reer \',\'sapcrm \',\'tip \',\'zone_employe\'"
    
    sqlQuery = """SELECT 	
                CODE_APPL,
				SRCE_NM,
				TRGT_SCHEMA,
				TRGT_TBL_NM,
				TRGT_BD,
				TRGT_NM
        FROM ctrl_ingestion.INGEST_PARAM
        WHERE CODE_APPL in ({});""".format(listOfSources)
    
    result = get_cursor_query(sqlQuery)

    try:
       csvTablesOutput = os.path.join(SCRIPT_DIR, 'list_tables_{}.csv'.format(datetime.now().strftime('%Y%m%d%H%M%S')))

       with open(csvTablesOutput, "w") as output:
           writer = csv.writer(output, delimiter= '|', lineterminator='\n')
           for val in result:
                writer.writerow([val[0], val[1], val[2], val[3], val[4]])

    except Exception as e:
        print("[ERROR] File Creation failure: {}".format(e))
