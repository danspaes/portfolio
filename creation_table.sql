--Trace table

create table proc_trace_log (exec_id number, proc_name varchar2(100), log_mesg varchar2(2000), start_dt date, end_dt date );
-- Create pk of trace table
CREATE SEQUENCE proc_id_log_seq start with 1 increment by 1
;
CREATE OR REPLACE TRIGGER proc_id_log_trg 
BEFORE INSERT ON proc_trace_log
FOR EACH ROW
BEGIN
  SELECT proc_id_log_seq.NEXTVAL
  INTO   :new.exec_id
  FROM   dual;
END;
-- External Table Landing Zone
CREATE TABLE lz_ds_all_brands
(
event_id          VARCHAR2(200),
event_dt            date,
transaction_dt      date,
order_dt            date,
order_status        VARCHAR2(200),
cancel_dt           date,
cancel_src          VARCHAR2(200),
client_order_id     VARCHAR2(200),
vc_order_id         VARCHAR2(200),
po_order_id         VARCHAR2(200),
po_order_dt         VARCHAR2(200),
qty_purchased       VARCHAR2(200),
price               VARCHAR2(200),
shipping_price      VARCHAR2(200),
shipping_cost       VARCHAR2(200),
tax         		VARCHAR2(200),
discount          	VARCHAR2(200),
client_order_dt     date,
category            VARCHAR2(200),
product_nm          VARCHAR2(200),
opt_data_1        	VARCHAR2(200),
opt_data_2        	VARCHAR2(200),
vendor_sku          VARCHAR2(200),
line_status       	VARCHAR2(200),
manufacturer      	VARCHAR2(200),
coupon              VARCHAR2(200),
ref_id_1            VARCHAR2(200),
ref_id_2            VARCHAR2(200),
cust_first_name   	VARCHAR2(200),
cust_last_name    	VARCHAR2(200),
cust_email        	VARCHAR2(200),
cust_phone        	VARCHAR2(200),
billto_name         VARCHAR2(200),
billto_address_1    VARCHAR2(200),
billto_address_2    VARCHAR2(200),
billto_city         VARCHAR2(200),
billto_state        VARCHAR2(200),
billto_postal_code  VARCHAR2(200),
shipto_name         VARCHAR2(200),
shipto_address_1    VARCHAR2(200),
shipto_address_2    VARCHAR2(200),
shipto_city         VARCHAR2(200),
shipto_state        VARCHAR2(200),
shipto_postal_code  VARCHAR2(200),
tracking            VARCHAR2(200),
loyal_nbr     		VARCHAR2(200),
estore_div_id   	VARCHAR2(200),
load_dt      		varchar2(14)
)
ORGANIZATION EXTERNAL (
  TYPE ORACLE_LOADER
  DEFAULT DIRECTORY LAND_DIR
  ACCESS PARAMETERS (
    RECORDS DELIMITED BY NEWLINE
    BADFILE LAND_DIR:'lz_ds_all_brands_%a_%p.bad'
    LOGFILE LAND_DIR:'lz_ds_all_brands_%a_%p.log'
    FIELDS TERMINATED BY '~^~' LRTRIM 
  MISSING FIELD VALUES ARE NULL
    (
  event_id ,  
  event_dt date "MM/DD/YYYY HH24:MI:SS", 
  transaction_dt date "MM/DD/YYYY HH24:MI:SS", 
  order_dt date "MM/DD/YYYY HH24:MI:SS", 
  order_status, 
  cancel_dt date "MM/DD/YYYY HH24:MI:SS", 
  cancel_src, client_order_id, vc_order_id, po_order_id, 
  po_order_dt date "MM/DD/YYYY HH24:MI:SS", 
  qty_purchased, price, shipping_price, shipping_cost, tax, discount, 
  client_order_dt date "MM/DD/YYYY HH24:MI:SS", 
  category,
  product_nm, opt_data_1, opt_data_2, vendor_sku, line_status, manufacturer, coupon, ref_id_1, ref_id_2, cust_first_name,
  cust_last_name, cust_email, cust_phone, billto_name, billto_address_1, billto_address_2, billto_city, billto_state,
  billto_postal_code, shipto_name, shipto_address_1, shipto_address_2, shipto_city, shipto_state, shipto_postal_code,
  tracking, loyal_nbr, estore_div_id, load_dt
  )
  )
  LOCATION ('ordertransaction.csv')
)
PARALLEL 1
REJECT LIMIT UNLIMITED
;

--Staging table 
CREATE TABLE STG_DS_ALL_BRANDS
(
EVENT_ID                          ,
EVENT_DT                  ,
TRANSACTION_DT            ,
ORDER_DT                  ,
ORDER_STATUS                        ,
CANCEL_DT               ,
CANCEL_SRC                          ,
CLIENT_ORDER_ID                     ,
VC_ORDER_ID                         ,
PO_ORDER_ID                         ,
PO_ORDER_DT               ,
QTY_PURCHASED                       ,
PRICE                     ,
SHIPPING_PRICE            ,
SHIPPING_COST             ,
TAX                       ,
DISCOUNT                  ,
CLIENT_ORDER_DT           ,
CATEGORY                            ,
PRODUCT_NM                          ,
OPT_DATA_1                          ,
OPT_DATA_2                          ,
VENDOR_SKU                          ,
LINE_STATUS                         ,
MANUFACTURER                        ,
COUPON                              ,
REF_ID_1                            ,
REF_ID_2                            ,
CUST_FIRST_NAME                     ,
CUST_LAST_NAME                      ,
CUST_EMAIL                          ,
CUST_PHONE                    ,
BILLTO_NAME                         ,
BILLTO_ADDRESS_1                    ,
BILLTO_ADDRESS_2                    ,
BILLTO_CITY                         ,
BILLTO_STATE                        ,
BILLTO_POSTAL_CODE                  ,
SHIPTO_NAME                         ,
SHIPTO_ADDRESS_1                    ,
SHIPTO_ADDRESS_2                    ,
SHIPTO_CITY                         ,
SHIPTO_STATE                        ,
SHIPTO_POSTAL_CODE                  ,
TRACKING                            ,
LOYAL_NBR                           ,
ESTORE_DIV_ID                   ,
load_dt)
AS 
SELECT 
TRIM(REGEXP_REPLACE(SRC.EVENT_ID,'"|\||,',''))                            ,
SRC.EVENT_DT,
SRC.TRANSACTION_DT,
SRC.ORDER_DT,
TRIM(REGEXP_REPLACE(SRC.ORDER_STATUS,'"|\||,',''))                        ,
SRC.CANCEL_DT,
TRIM(REGEXP_REPLACE(SRC.CANCEL_SRC,'"|\||,',''))                          ,
TRIM(REGEXP_REPLACE(SRC.CLIENT_ORDER_ID,'"|\||,',''))                     ,
TRIM(REGEXP_REPLACE(SRC.VC_ORDER_ID,'"|\||,',''))                         ,
TRIM(REGEXP_REPLACE(SRC.PO_ORDER_ID,'"|\||,',''))                         ,
SRC.PO_ORDER_DT,
TRIM(REGEXP_REPLACE(SRC.QTY_PURCHASED,'"|\||,','')),
TO_NUMBER(REGEXP_REPLACE(SRC.PRICE,'"|\||,',''))              ,
TO_NUMBER(REGEXP_REPLACE(SRC.SHIPPING_PRICE,'"|\||,',''))               ,
TO_NUMBER(REGEXP_REPLACE(SRC.SHIPPING_COST,'"|\||,',''))                ,
TO_NUMBER(REGEXP_REPLACE(SRC.TAX,'"|\||,',''))                          ,
TO_NUMBER(REGEXP_REPLACE(SRC.DISCOUNT,'"|\||,',''))                     ,
SRC.CLIENT_ORDER_DT,
TRIM(REGEXP_REPLACE(SRC.CATEGORY,'"|\||,',''))                            ,
REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(SRC.PRODUCT_NM, 'À|Á|Â|Ã|Ä|Å|Æ', 'A'),'È|É|Ê|Ë','E'), 'Ì|Í|Î|Ï','I'), 'Ò|Ó|Ô|Õ|Ö','O'), 'Ù|Ú|Û|Ü', 'U'), 'Ç','C'), 'Ñ','N'), '×','X'), 'Ý','Y'), 'à|á|â|ã|ä|å|æ', 'a'),'ç','c'),'è|é|ê|ë','e'),'ì|í|î|ï','i'),'ñ','n'),'ò|ó|ô|õ|ö','o'),'ù|ú|û|ü','u'),'ý|ÿ','y'),'Ð|ø|þ|ð|÷|ß|Þ|Ø|¡|¢|£|¤|¥|¦|§|¨|©|ª|«|¬|®|¯|°|±|²|³|´|µ|¶|·|¸|¹|º|»|¼|½|¾|¿|"|,|\|', ''),
TRIM(REGEXP_REPLACE(SRC.OPT_DATA_1,'\"|\||,',''))                          ,
TRIM(REGEXP_REPLACE(SRC.OPT_DATA_2,'"|\||,',''))                          ,
TRIM(REGEXP_REPLACE(SRC.VENDOR_SKU,'"|\||,',''))                          ,
TRIM(REGEXP_REPLACE(SRC.LINE_STATUS,'"|\||,',''))                         ,
TRIM(REGEXP_REPLACE(SRC.MANUFACTURER,'"|\||,',''))                        ,
TRIM(REGEXP_REPLACE(SRC.COUPON,'"|\||,',''))                              ,
TRIM(REGEXP_REPLACE(SRC.REF_ID_1,'"|\||,',''))                            ,
TRIM(REGEXP_REPLACE(SRC.REF_ID_2,'"|\||,',''))                            ,
REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(SRC.CUST_FIRST_NAME, 'À|Á|Â|Ã|Ä|Å|Æ', 'A'),'È|É|Ê|Ë','E'), 'Ì|Í|Î|Ï','I'), 'Ò|Ó|Ô|Õ|Ö','O'), 'Ù|Ú|Û|Ü', 'U'), 'Ç','C'), 'Ñ','N'), '×','X'), 'Ý','Y'), 'à|á|â|ã|ä|å|æ', 'a'),'ç','c'),'è|é|ê|ë','e'),'ì|í|î|ï','i'),'ñ','n'),'ò|ó|ô|õ|ö','o'),'ù|ú|û|ü','u'),'ý|ÿ','y'),'Ð|ø|þ|ð|÷|ß|Þ|Ø|¡|¢|£|¤|¥|¦|§|¨|©|ª|«|¬|®|¯|°|±|²|³|´|µ|¶|·|¸|¹|º|»|¼|½|¾|¿|"|\||,', ''),
REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(SRC.CUST_LAST_NAME, 'À|Á|Â|Ã|Ä|Å|Æ', 'A'),'È|É|Ê|Ë','E'), 'Ì|Í|Î|Ï','I'), 'Ò|Ó|Ô|Õ|Ö','O'), 'Ù|Ú|Û|Ü', 'U'), 'Ç','C'), 'Ñ','N'), '×','X'), 'Ý','Y'), 'à|á|â|ã|ä|å|æ', 'a'),'ç','c'),'è|é|ê|ë','e'),'ì|í|î|ï','i'),'ñ','n'),'ò|ó|ô|õ|ö','o'),'ù|ú|û|ü','u'),'ý|ÿ','y'),'Ð|ø|þ|ð|÷|ß|Þ|Ø|¡|¢|£|¤|¥|¦|§|¨|©|ª|«|¬|®|¯|°|±|²|³|´|µ|¶|·|¸|¹|º|»|¼|½|¾|¿|"|\||,', ''),
REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(SRC.CUST_EMAIL, 'À|Á|Â|Ã|Ä|Å|Æ', 'A'),'È|É|Ê|Ë','E'), 'Ì|Í|Î|Ï','I'), 'Ò|Ó|Ô|Õ|Ö','O'), 'Ù|Ú|Û|Ü', 'U'), 'Ç','C'), 'Ñ','N'), '×','X'), 'Ý','Y'), 'à|á|â|ã|ä|å|æ', 'a'),'ç','c'),'è|é|ê|ë','e'),'ì|í|î|ï','i'),'ñ','n'),'ò|ó|ô|õ|ö','o'),'ù|ú|û|ü','u'),'ý|ÿ','y'),'Ð|ø|þ|ð|÷|ß|Þ|Ø|¡|¢|£|¤|¥|¦|§|¨|©|ª|«|¬|®|¯|°|±|²|³|´|µ|¶|·|¸|¹|º|»|¼|½|¾|¿|"|\||,', ''),
REGEXP_REPLACE(SRC.CUST_PHONE, '-| |\(|\)|"|\||,','') ,
REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(SRC.BILLTO_NAME, 'À|Á|Â|Ã|Ä|Å|Æ', 'A'),'È|É|Ê|Ë','E'), 'Ì|Í|Î|Ï','I'), 'Ò|Ó|Ô|Õ|Ö','O'), 'Ù|Ú|Û|Ü', 'U'), 'Ç','C'), 'Ñ','N'), '×','X'), 'Ý','Y'), 'à|á|â|ã|ä|å|æ', 'a'),'ç','c'),'è|é|ê|ë','e'),'ì|í|î|ï','i'),'ñ','n'),'ò|ó|ô|õ|ö','o'),'ù|ú|û|ü','u'),'ý|ÿ','y'),'Ð|ø|þ|ð|÷|ß|Þ|Ø|¡|¢|£|¤|¥|¦|§|¨|©|ª|«|¬|®|¯|°|±|²|³|´|µ|¶|·|¸|¹|º|»|¼|½|¾|¿|"|\||,', ''),
REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(SRC.BILLTO_ADDRESS_1, 'À|Á|Â|Ã|Ä|Å|Æ', 'A'),'È|É|Ê|Ë','E'), 'Ì|Í|Î|Ï','I'), 'Ò|Ó|Ô|Õ|Ö','O'), 'Ù|Ú|Û|Ü', 'U'), 'Ç','C'), 'Ñ','N'), '×','X'), 'Ý','Y'), 'à|á|â|ã|ä|å|æ', 'a'),'ç','c'),'è|é|ê|ë','e'),'ì|í|î|ï','i'),'ñ','n'),'ò|ó|ô|õ|ö','o'),'ù|ú|û|ü','u'),'ý|ÿ','y'),'Ð|ø|þ|ð|÷|ß|Þ|Ø|¡|¢|£|¤|¥|¦|§|¨|©|ª|«|¬|®|¯|°|±|²|³|´|µ|¶|·|¸|¹|º|»|¼|½|¾|¿|"|\||,', ''),
REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(SRC.BILLTO_ADDRESS_2, 'À|Á|Â|Ã|Ä|Å|Æ', 'A'),'È|É|Ê|Ë','E'), 'Ì|Í|Î|Ï','I'), 'Ò|Ó|Ô|Õ|Ö','O'), 'Ù|Ú|Û|Ü', 'U'), 'Ç','C'), 'Ñ','N'), '×','X'), 'Ý','Y'), 'à|á|â|ã|ä|å|æ', 'a'),'ç','c'),'è|é|ê|ë','e'),'ì|í|î|ï','i'),'ñ','n'),'ò|ó|ô|õ|ö','o'),'ù|ú|û|ü','u'),'ý|ÿ','y'),'Ð|ø|þ|ð|÷|ß|Þ|Ø|¡|¢|£|¤|¥|¦|§|¨|©|ª|«|¬|®|¯|°|±|²|³|´|µ|¶|·|¸|¹|º|»|¼|½|¾|¿|"|\||,', ''),
REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(SRC.BILLTO_CITY, 'À|Á|Â|Ã|Ä|Å|Æ', 'A'),'È|É|Ê|Ë','E'), 'Ì|Í|Î|Ï','I'), 'Ò|Ó|Ô|Õ|Ö','O'), 'Ù|Ú|Û|Ü', 'U'), 'Ç','C'), 'Ñ','N'), '×','X'), 'Ý','Y'), 'à|á|â|ã|ä|å|æ', 'a'),'ç','c'),'è|é|ê|ë','e'),'ì|í|î|ï','i'),'ñ','n'),'ò|ó|ô|õ|ö','o'),'ù|ú|û|ü','u'),'ý|ÿ','y'),'Ð|ø|þ|ð|÷|ß|Þ|Ø|¡|¢|£|¤|¥|¦|§|¨|©|ª|«|¬|®|¯|°|±|²|³|´|µ|¶|·|¸|¹|º|»|¼|½|¾|¿|"|\||,', ''),
TRIM(REGEXP_REPLACE(SRC.BILLTO_STATE      ,'"|\||,','')),
TRIM(REGEXP_REPLACE(SRC.BILLTO_POSTAL_CODE,'"|\||,','')),
REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(SRC.SHIPTO_NAME, 'À|Á|Â|Ã|Ä|Å|Æ', 'A'),'È|É|Ê|Ë','E'), 'Ì|Í|Î|Ï','I'), 'Ò|Ó|Ô|Õ|Ö','O'), 'Ù|Ú|Û|Ü', 'U'), 'Ç','C'), 'Ñ','N'), '×','X'), 'Ý','Y'), 'à|á|â|ã|ä|å|æ', 'a'),'ç','c'),'è|é|ê|ë','e'),'ì|í|î|ï','i'),'ñ','n'),'ò|ó|ô|õ|ö','o'),'ù|ú|û|ü','u'),'ý|ÿ','y'),'Ð|ø|þ|ð|÷|ß|Þ|Ø|¡|¢|£|¤|¥|¦|§|¨|©|ª|«|¬|®|¯|°|±|²|³|´|µ|¶|·|¸|¹|º|»|¼|½|¾|¿|"|\||,', ''),
REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(SRC.SHIPTO_ADDRESS_1, 'À|Á|Â|Ã|Ä|Å|Æ', 'A'),'È|É|Ê|Ë','E'), 'Ì|Í|Î|Ï','I'), 'Ò|Ó|Ô|Õ|Ö','O'), 'Ù|Ú|Û|Ü', 'U'), 'Ç','C'), 'Ñ','N'), '×','X'), 'Ý','Y'), 'à|á|â|ã|ä|å|æ', 'a'),'ç','c'),'è|é|ê|ë','e'),'ì|í|î|ï','i'),'ñ','n'),'ò|ó|ô|õ|ö','o'),'ù|ú|û|ü','u'),'ý|ÿ','y'),'Ð|ø|þ|ð|÷|ß|Þ|Ø|¡|¢|£|¤|¥|¦|§|¨|©|ª|«|¬|®|¯|°|±|²|³|´|µ|¶|·|¸|¹|º|»|¼|½|¾|¿|"|\||,', ''),
REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(SRC.SHIPTO_ADDRESS_2, 'À|Á|Â|Ã|Ä|Å|Æ', 'A'),'È|É|Ê|Ë','E'), 'Ì|Í|Î|Ï','I'), 'Ò|Ó|Ô|Õ|Ö','O'), 'Ù|Ú|Û|Ü', 'U'), 'Ç','C'), 'Ñ','N'), '×','X'), 'Ý','Y'), 'à|á|â|ã|ä|å|æ', 'a'),'ç','c'),'è|é|ê|ë','e'),'ì|í|î|ï','i'),'ñ','n'),'ò|ó|ô|õ|ö','o'),'ù|ú|û|ü','u'),'ý|ÿ','y'),'Ð|ø|þ|ð|÷|ß|Þ|Ø|¡|¢|£|¤|¥|¦|§|¨|©|ª|«|¬|®|¯|°|±|²|³|´|µ|¶|·|¸|¹|º|»|¼|½|¾|¿|"|\||,', ''),
REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(SRC.SHIPTO_CITY, 'À|Á|Â|Ã|Ä|Å|Æ', 'A'),'È|É|Ê|Ë','E'), 'Ì|Í|Î|Ï','I'), 'Ò|Ó|Ô|Õ|Ö','O'), 'Ù|Ú|Û|Ü', 'U'), 'Ç','C'), 'Ñ','N'), '×','X'), 'Ý','Y'), 'à|á|â|ã|ä|å|æ', 'a'),'ç','c'),'è|é|ê|ë','e'),'ì|í|î|ï','i'),'ñ','n'),'ò|ó|ô|õ|ö','o'),'ù|ú|û|ü','u'),'ý|ÿ','y'),'Ð|ø|þ|ð|÷|ß|Þ|Ø|¡|¢|£|¤|¥|¦|§|¨|©|ª|«|¬|®|¯|°|±|²|³|´|µ|¶|·|¸|¹|º|»|¼|½|¾|¿|"|\||,', ''),
TRIM(REGEXP_REPLACE(SRC.SHIPTO_STATE      ,'"|\||,','')),
TRIM(REGEXP_REPLACE(SRC.SHIPTO_POSTAL_CODE,'"|\||,','')),
TRIM(REGEXP_REPLACE(SRC.TRACKING          ,'"|\||,','')),
TRIM(REGEXP_REPLACE(SRC.LOYAL_NBR         ,'"|\||,','')),
TRIM(REGEXP_REPLACE(SRC.ESTORE_DIV_ID    ,'"|\||,','')),                                                                                                                                                                                                            
TO_DATE(REGEXP_REPLACE(SRC.LOAD_DT ,'"|\||,',''), 'MMDDYYYYHH24MISS')
FROM LZ_DS_ALL_BRANDS SRC WHERE ROWNUM =0;