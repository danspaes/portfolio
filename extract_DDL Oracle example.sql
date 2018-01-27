select dbms_metadata.get_ddl('TABLE','LE_F_002_SLS_TXN_DTL_EXT','WH_ETL') from dual;
select dbms_metadata.get_ddl('TABLE','LE_F_020_INV_SKU_STR_D_EXT','WH_ETL') from dual;
select dbms_metadata.get_ddl('TABLE','LE_F_010_RECEIPTS_DC_DTL_EXT','WH_ETL') from dual;
select dbms_metadata.get_ddl('TABLE','LE_D_P09_STYLE_EXT','WH_ETL') from dual;
select dbms_metadata.get_ddl('TABLE','LE_D_P10_SIZE_RANGE_EXT','WH_ETL') from dual;
select dbms_metadata.get_ddl('TABLE','LE_D_P11_STYLE_COLOUR_EXT','WH_ETL') from dual;
select dbms_metadata.get_ddl('TABLE','LE_D_P12_SIZE_EXT','WH_ETL') from dual;
select dbms_metadata.get_ddl('TABLE','LE_D_P13_SKU_EXT','WH_ETL') from dual;


  CREATE TABLE "WH_ETL"."LE_F_002_SLS_TXN_DTL_EXT_RTK" 
   (	"SLS_TXN_DTLKEY" VARCHAR2(255), 
	"SLS_TXN_HDRKEY" VARCHAR2(255), 
	"TXN_NBR" VARCHAR2(255), 
	"TXN_DATE" VARCHAR2(255), 
	"SKU_NBR" VARCHAR2(255), 
	"STORE_NBR" VARCHAR2(255), 
	"MD_TYPE" VARCHAR2(255), 
	"TXN_TIME" VARCHAR2(255), 
	"SLS_FLG" VARCHAR2(255), 
	"RETRN_FLG" VARCHAR2(255), 
	"SLS_UNTS" VARCHAR2(255), 
	"SLS_AMT" VARCHAR2(255), 
	"COGS_AMT" VARCHAR2(255), 
	"SLS_PERM_MD_AMT" VARCHAR2(255), 
	"SLS_PROMO_MD_AMT" VARCHAR2(255), 
	"SLS_DISCOUNT_AMT" VARCHAR2(255), 
	"LOAD_DATE" VARCHAR2(255), 
	"LAST_UPDATE" VARCHAR2(255), 
	"EMPLOYEE_NBR" VARCHAR2(255), 
	"EMPLOYEE_DISCOUNT_FLG" VARCHAR2(255)
   ) 
   ORGANIZATION EXTERNAL 
    ( TYPE ORACLE_LOADER
      DEFAULT DIRECTORY "LAND_DIR"
      ACCESS PARAMETERS
      ( records delimited by '\n' 
 characterset WE8ISO8859P1 
 fields terminated by '|' 
 missing field values are NULL     )
      LOCATION
       ( 'R_F_002_SLS_TXN_LINE_DETAIL_RTK.DAT'
       )
    )
;