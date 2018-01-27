create or replace procedure STG_LOAD AS
  l_rowcount number;
  l_sqlerrm  varchar(2000);
  l_proc_name varchar(100);
  l_log_tbl_name varchar(32);
  l_start_time date;
  l_proc_id number;
BEGIN
  l_log_tbl_name := 'PROC_TRACE_LOG';
  l_proc_name := 'STG_LOAD';
  l_start_time := sysdate;
  execute immediate 'INSERT INTO '|| l_log_tbl_name ||' (proc_name, log_mesg, start_dt) VALUES ('|| l_proc_name||', ''DELETE ON STG_DS_ALL_BRANDS RUNNING'','||l_start_time||')';
  COMMIT;
  execute immediate 'select max(exec_id) into '|| l_proc_id ||' from '|| l_log_tbl_name ||' where proc_name = '|| l_proc_name ||' and start_dt = '|| l_start_time ||' and log_mesg like ''DELETE%''';
  execute immediate 'DELETE FROM STG_DS_ALL_BRANDS';
  l_rowcount := SQL%rowcount;
  execute immediate 'UPDATE '|| l_log_tbl_name ||' SET log_mesg = ''DELETE SUCCESSFUL... DELETED: ' || l_rowcount || ' ROWS'', end_dt = SYSDATE WHERE proc_name = '''||l_proc_name||''' AND start_dt = '||l_start_time||' AND exec_id = '|| l_proc_id||'';
  BEGIN
	execute immediate 'INSERT INTO '|| l_log_tbl_name ||' (proc_name, log_mesg, start_dt) VALUES ('|| l_proc_name||', ''INSERT ON STG_DS_ALL_BRANDS RUNNING'','||l_start_time||')';
  COMMIT;
  execute immediate 'select max(exec_id) into '|| l_proc_id ||' from '|| l_log_tbl_name ||' where proc_name = '|| l_proc_name ||' and start_dt = '|| l_start_time ||' and log_mesg like ''INSERT%''';
  execute immediate 'INSERT INTO STG_DS_ALL_BRANDS SELECT TRIM(REGEXP_REPLACE(SRC.EVENT_ID,''"|\||,'',''''))                            , SRC.EVENT_DT,
SRC.TRANSACTION_DT,
SRC.ORDER_DT,
TRIM(REGEXP_REPLACE(SRC.ORDER_STATUS,''"|\||,'',''''))                        ,
SRC.CANCEL_DT,
TRIM(REGEXP_REPLACE(SRC.CANCEL_SRC,''"|\||,'',''''))                          ,
TRIM(REGEXP_REPLACE(SRC.CLIENT_ORDER_ID,''"|\||,'',''''))                     ,
TRIM(REGEXP_REPLACE(SRC.VC_ORDER_ID,''"|\||,'',''''))                         ,
TRIM(REGEXP_REPLACE(SRC.PO_ORDER_ID,''"|\||,'',''''))                         ,
SRC.PO_ORDER_DT,
TRIM(REGEXP_REPLACE(SRC.QTY_PURCHASED,''"|\||,'','''')),
TO_NUMBER(REGEXP_REPLACE(SRC.PRICE,''"|\||,'',''''))              ,
TO_NUMBER(REGEXP_REPLACE(SRC.SHIPPING_PRICE,''"|\||,'',''''))               ,
TO_NUMBER(REGEXP_REPLACE(SRC.SHIPPING_COST,''"|\||,'',''''))                ,
TO_NUMBER(REGEXP_REPLACE(SRC.TAX,''"|\||,'',''''))                          ,
TO_NUMBER(REGEXP_REPLACE(SRC.DISCOUNT,''"|\||,'',''''))                     ,
SRC.CLIENT_ORDER_DT,
TRIM(REGEXP_REPLACE(SRC.CATEGORY,''"|\||,'',''''))                            ,
REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(SRC.PRODUCT_NM, ''À|Á|Â|Ã|Ä|Å|Æ'', ''A''),''È|É|Ê|Ë'',''E''), ''Ì|Í|Î|Ï'',''I''), ''Ò|Ó|Ô|Õ|Ö'',''O''), ''Ù|Ú|Û|Ü'', ''U''), ''Ç'',''C''), ''Ñ'',''N''), ''×'',''X''), ''Ý'',''Y''), ''à|á|â|ã|ä|å|æ'', ''a''),''ç'',''c''),''è|é|ê|ë'',''e''),''ì|í|î|ï'',''i''),''ñ'',''n''),''ò|ó|ô|õ|ö'',''o''),''ù|ú|û|ü'',''u''),''ý|ÿ'',''y''),''Ð|ø|þ|ð|÷|ß|Þ|Ø|¡|¢|£|¤|¥|¦|§|¨|©|ª|«|¬|®|¯|°|±|²|³|´|µ|¶|·|¸|¹|º|»|¼|½|¾|¿|"|,|\|'', ''''),
TRIM(REGEXP_REPLACE(SRC.OPT_DATA_1,''\"|\||,'',''''))                          ,
TRIM(REGEXP_REPLACE(SRC.OPT_DATA_2,''"|\||,'',''''))                          ,
TRIM(REGEXP_REPLACE(SRC.VENDOR_SKU,''"|\||,'',''''))                          ,
TRIM(REGEXP_REPLACE(SRC.LINE_STATUS,''"|\||,'',''''))                         ,
TRIM(REGEXP_REPLACE(SRC.MANUFACTURER,''"|\||,'',''''))                        ,
TRIM(REGEXP_REPLACE(SRC.COUPON,''"|\||,'',''''))                              ,
TRIM(REGEXP_REPLACE(SRC.REF_ID_1,''"|\||,'',''''))                            ,
TRIM(REGEXP_REPLACE(SRC.REF_ID_2,''"|\||,'',''''))                            ,
REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(SRC.CUST_FIRST_NAME, ''À|Á|Â|Ã|Ä|Å|Æ'', ''A''),''È|É|Ê|Ë'',''E''), ''Ì|Í|Î|Ï'',''I''), ''Ò|Ó|Ô|Õ|Ö'',''O''), ''Ù|Ú|Û|Ü'', ''U''), ''Ç'',''C''), ''Ñ'',''N''), ''×'',''X''), ''Ý'',''Y''), ''à|á|â|ã|ä|å|æ'', ''a''),''ç'',''c''),''è|é|ê|ë'',''e''),''ì|í|î|ï'',''i''),''ñ'',''n''),''ò|ó|ô|õ|ö'',''o''),''ù|ú|û|ü'',''u''),''ý|ÿ'',''y''),''Ð|ø|þ|ð|÷|ß|Þ|Ø|¡|¢|£|¤|¥|¦|§|¨|©|ª|«|¬|®|¯|°|±|²|³|´|µ|¶|·|¸|¹|º|»|¼|½|¾|¿|"|\||,'', ''''),
REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(SRC.CUST_LAST_NAME, ''À|Á|Â|Ã|Ä|Å|Æ'', ''A''),''È|É|Ê|Ë'',''E''), ''Ì|Í|Î|Ï'',''I''), ''Ò|Ó|Ô|Õ|Ö'',''O''), ''Ù|Ú|Û|Ü'', ''U''), ''Ç'',''C''), ''Ñ'',''N''), ''×'',''X''), ''Ý'',''Y''), ''à|á|â|ã|ä|å|æ'', ''a''),''ç'',''c''),''è|é|ê|ë'',''e''),''ì|í|î|ï'',''i''),''ñ'',''n''),''ò|ó|ô|õ|ö'',''o''),''ù|ú|û|ü'',''u''),''ý|ÿ'',''y''),''Ð|ø|þ|ð|÷|ß|Þ|Ø|¡|¢|£|¤|¥|¦|§|¨|©|ª|«|¬|®|¯|°|±|²|³|´|µ|¶|·|¸|¹|º|»|¼|½|¾|¿|"|\||,'', ''''),
REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(SRC.CUST_EMAIL, ''À|Á|Â|Ã|Ä|Å|Æ'', ''A''),''È|É|Ê|Ë'',''E''), ''Ì|Í|Î|Ï'',''I''), ''Ò|Ó|Ô|Õ|Ö'',''O''), ''Ù|Ú|Û|Ü'', ''U''), ''Ç'',''C''), ''Ñ'',''N''), ''×'',''X''), ''Ý'',''Y''), ''à|á|â|ã|ä|å|æ'', ''a''),''ç'',''c''),''è|é|ê|ë'',''e''),''ì|í|î|ï'',''i''),''ñ'',''n''),''ò|ó|ô|õ|ö'',''o''),''ù|ú|û|ü'',''u''),''ý|ÿ'',''y''),''Ð|ø|þ|ð|÷|ß|Þ|Ø|¡|¢|£|¤|¥|¦|§|¨|©|ª|«|¬|®|¯|°|±|²|³|´|µ|¶|·|¸|¹|º|»|¼|½|¾|¿|"|\||,'', ''''),
REGEXP_REPLACE(SRC.CUST_PHONE, ''-| |\(|\)|"|\||,'','''') ,
REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(SRC.BILLTO_NAME, ''À|Á|Â|Ã|Ä|Å|Æ'', ''A''),''È|É|Ê|Ë'',''E''), ''Ì|Í|Î|Ï'',''I''), ''Ò|Ó|Ô|Õ|Ö'',''O''), ''Ù|Ú|Û|Ü'', ''U''), ''Ç'',''C''), ''Ñ'',''N''), ''×'',''X''), ''Ý'',''Y''), ''à|á|â|ã|ä|å|æ'', ''a''),''ç'',''c''),''è|é|ê|ë'',''e''),''ì|í|î|ï'',''i''),''ñ'',''n''),''ò|ó|ô|õ|ö'',''o''),''ù|ú|û|ü'',''u''),''ý|ÿ'',''y''),''Ð|ø|þ|ð|÷|ß|Þ|Ø|¡|¢|£|¤|¥|¦|§|¨|©|ª|«|¬|®|¯|°|±|²|³|´|µ|¶|·|¸|¹|º|»|¼|½|¾|¿|"|\||,'', ''''),
REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(SRC.BILLTO_ADDRESS_1, ''À|Á|Â|Ã|Ä|Å|Æ'', ''A''),''È|É|Ê|Ë'',''E''), ''Ì|Í|Î|Ï'',''I''), ''Ò|Ó|Ô|Õ|Ö'',''O''), ''Ù|Ú|Û|Ü'', ''U''), ''Ç'',''C''), ''Ñ'',''N''), ''×'',''X''), ''Ý'',''Y''), ''à|á|â|ã|ä|å|æ'', ''a''),''ç'',''c''),''è|é|ê|ë'',''e''),''ì|í|î|ï'',''i''),''ñ'',''n''),''ò|ó|ô|õ|ö'',''o''),''ù|ú|û|ü'',''u''),''ý|ÿ'',''y''),''Ð|ø|þ|ð|÷|ß|Þ|Ø|¡|¢|£|¤|¥|¦|§|¨|©|ª|«|¬|®|¯|°|±|²|³|´|µ|¶|·|¸|¹|º|»|¼|½|¾|¿|"|\||,'', ''''),
REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(SRC.BILLTO_ADDRESS_2, ''À|Á|Â|Ã|Ä|Å|Æ'', ''A''),''È|É|Ê|Ë'',''E''), ''Ì|Í|Î|Ï'',''I''), ''Ò|Ó|Ô|Õ|Ö'',''O''), ''Ù|Ú|Û|Ü'', ''U''), ''Ç'',''C''), ''Ñ'',''N''), ''×'',''X''), ''Ý'',''Y''), ''à|á|â|ã|ä|å|æ'', ''a''),''ç'',''c''),''è|é|ê|ë'',''e''),''ì|í|î|ï'',''i''),''ñ'',''n''),''ò|ó|ô|õ|ö'',''o''),''ù|ú|û|ü'',''u''),''ý|ÿ'',''y''),''Ð|ø|þ|ð|÷|ß|Þ|Ø|¡|¢|£|¤|¥|¦|§|¨|©|ª|«|¬|®|¯|°|±|²|³|´|µ|¶|·|¸|¹|º|»|¼|½|¾|¿|"|\||,'', ''''),
REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(SRC.BILLTO_CITY, ''À|Á|Â|Ã|Ä|Å|Æ'', ''A''),''È|É|Ê|Ë'',''E''), ''Ì|Í|Î|Ï'',''I''), ''Ò|Ó|Ô|Õ|Ö'',''O''), ''Ù|Ú|Û|Ü'', ''U''), ''Ç'',''C''), ''Ñ'',''N''), ''×'',''X''), ''Ý'',''Y''), ''à|á|â|ã|ä|å|æ'', ''a''),''ç'',''c''),''è|é|ê|ë'',''e''),''ì|í|î|ï'',''i''),''ñ'',''n''),''ò|ó|ô|õ|ö'',''o''),''ù|ú|û|ü'',''u''),''ý|ÿ'',''y''),''Ð|ø|þ|ð|÷|ß|Þ|Ø|¡|¢|£|¤|¥|¦|§|¨|©|ª|«|¬|®|¯|°|±|²|³|´|µ|¶|·|¸|¹|º|»|¼|½|¾|¿|"|\||,'', ''''),
TRIM(REGEXP_REPLACE(SRC.BILLTO_STATE      ,''"|\||,'','''')),
TRIM(REGEXP_REPLACE(SRC.BILLTO_POSTAL_CODE,''"|\||,'','''')),
REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(SRC.SHIPTO_NAME, ''À|Á|Â|Ã|Ä|Å|Æ'', ''A''),''È|É|Ê|Ë'',''E''), ''Ì|Í|Î|Ï'',''I''), ''Ò|Ó|Ô|Õ|Ö'',''O''), ''Ù|Ú|Û|Ü'', ''U''), ''Ç'',''C''), ''Ñ'',''N''), ''×'',''X''), ''Ý'',''Y''), ''à|á|â|ã|ä|å|æ'', ''a''),''ç'',''c''),''è|é|ê|ë'',''e''),''ì|í|î|ï'',''i''),''ñ'',''n''),''ò|ó|ô|õ|ö'',''o''),''ù|ú|û|ü'',''u''),''ý|ÿ'',''y''),''Ð|ø|þ|ð|÷|ß|Þ|Ø|¡|¢|£|¤|¥|¦|§|¨|©|ª|«|¬|®|¯|°|±|²|³|´|µ|¶|·|¸|¹|º|»|¼|½|¾|¿|"|\||,'', ''''),
REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(SRC.SHIPTO_ADDRESS_1, ''À|Á|Â|Ã|Ä|Å|Æ'', ''A''),''È|É|Ê|Ë'',''E''), ''Ì|Í|Î|Ï'',''I''), ''Ò|Ó|Ô|Õ|Ö'',''O''), ''Ù|Ú|Û|Ü'', ''U''), ''Ç'',''C''), ''Ñ'',''N''), ''×'',''X''), ''Ý'',''Y''), ''à|á|â|ã|ä|å|æ'', ''a''),''ç'',''c''),''è|é|ê|ë'',''e''),''ì|í|î|ï'',''i''),''ñ'',''n''),''ò|ó|ô|õ|ö'',''o''),''ù|ú|û|ü'',''u''),''ý|ÿ'',''y''),''Ð|ø|þ|ð|÷|ß|Þ|Ø|¡|¢|£|¤|¥|¦|§|¨|©|ª|«|¬|®|¯|°|±|²|³|´|µ|¶|·|¸|¹|º|»|¼|½|¾|¿|"|\||,'', ''''),
REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(SRC.SHIPTO_ADDRESS_2, ''À|Á|Â|Ã|Ä|Å|Æ'', ''A''),''È|É|Ê|Ë'',''E''), ''Ì|Í|Î|Ï'',''I''), ''Ò|Ó|Ô|Õ|Ö'',''O''), ''Ù|Ú|Û|Ü'', ''U''), ''Ç'',''C''), ''Ñ'',''N''), ''×'',''X''), ''Ý'',''Y''), ''à|á|â|ã|ä|å|æ'', ''a''),''ç'',''c''),''è|é|ê|ë'',''e''),''ì|í|î|ï'',''i''),''ñ'',''n''),''ò|ó|ô|õ|ö'',''o''),''ù|ú|û|ü'',''u''),''ý|ÿ'',''y''),''Ð|ø|þ|ð|÷|ß|Þ|Ø|¡|¢|£|¤|¥|¦|§|¨|©|ª|«|¬|®|¯|°|±|²|³|´|µ|¶|·|¸|¹|º|»|¼|½|¾|¿|"|\||,'', ''''),
REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(SRC.SHIPTO_CITY, ''À|Á|Â|Ã|Ä|Å|Æ'', ''A''),''È|É|Ê|Ë'',''E''), ''Ì|Í|Î|Ï'',''I''), ''Ò|Ó|Ô|Õ|Ö'',''O''), ''Ù|Ú|Û|Ü'', ''U''), ''Ç'',''C''), ''Ñ'',''N''), ''×'',''X''), ''Ý'',''Y''), ''à|á|â|ã|ä|å|æ'', ''a''),''ç'',''c''),''è|é|ê|ë'',''e''),''ì|í|î|ï'',''i''),''ñ'',''n''),''ò|ó|ô|õ|ö'',''o''),''ù|ú|û|ü'',''u''),''ý|ÿ'',''y''),''Ð|ø|þ|ð|÷|ß|Þ|Ø|¡|¢|£|¤|¥|¦|§|¨|©|ª|«|¬|®|¯|°|±|²|³|´|µ|¶|·|¸|¹|º|»|¼|½|¾|¿|"|\||,'', ''''),
TRIM(REGEXP_REPLACE(SRC.SHIPTO_STATE      ,''"|\||,'','''')),
TRIM(REGEXP_REPLACE(SRC.SHIPTO_POSTAL_CODE,''"|\||,'','''')),
TRIM(REGEXP_REPLACE(SRC.TRACKING          ,''"|\||,'','''')),
TRIM(REGEXP_REPLACE(SRC.LOYAL_NBR         ,''"|\||,'','''')),
TRIM(REGEXP_REPLACE(SRC.ESTORE_DIV_ID    ,''"|\||,'','''')),
TO_DATE(SRC.LOAD_DT, ''MMDDYYYYHH24MISS'')
FROM LZ_DS_ALL_BRANDS SRC,
       (SELECT RANK() OVER(PARTITION BY ESTORE_DIV_ID, CLIENT_ORDER_ID ORDER BY EVENT_DT DESC) RNK_ID,
               TRIM(REGEXP_REPLACE(ESTORE_DIV_ID,''"|\||,'','''')) AS ESTORE_DIV_ID,
               TRIM(REGEXP_REPLACE(CLIENT_ORDER_ID,''"|\||,'','''')) AS CLIENT_ORDER_ID,
               TRIM(REGEXP_REPLACE(VENDOR_SKU,''"|\||,'','''')) AS VENDOR_SKU,
               TRIM(REGEXP_REPLACE(EVENT_ID,''"|\||,'','''')) AS EVENT_ID
          FROM LZ_DS_ALL_BRANDS) LST_RNK
 WHERE LST_RNK.RNK_ID = 1 -- ORDERED BY EVENT_ID DESC
   AND LST_RNK.ESTORE_DIV_ID   = TRIM(REGEXP_REPLACE(SRC.ESTORE_DIV_ID,''"|\||,'',''''))
   AND LST_RNK.CLIENT_ORDER_ID   = TRIM(REGEXP_REPLACE(SRC.CLIENT_ORDER_ID,''"|\||,'',''''))
   AND LST_RNK.VENDOR_SKU     = TRIM(REGEXP_REPLACE(SRC.VENDOR_SKU,''"|\||,'',''''))
   AND LST_RNK.EVENT_ID     = TRIM(REGEXP_REPLACE(SRC.EVENT_ID,''"|\||,'',''''))';
    l_rowcount := SQL%rowcount;
	execute immediate 'UPDATE '|| l_log_tbl_name ||' SET log_mesg = ''INSERT SUCCESSFUL... INSERTED: ' || l_rowcount || ' ROWS'', end_dt = SYSDATE WHERE proc_name = '''||l_proc_name||''' AND start_dt = '||l_start_time||' AND exec_id = '|| l_proc_id||'';
    COMMIT;
  exception
    when others then
      l_sqlerrm := sqlerrm;
	  execute immediate 'UPDATE '|| l_log_tbl_name ||' SET log_mesg = ''INSERT FAILED ERROR: ' || l_sqlerrm || ''', end_dt = SYSDATE WHERE proc_name = '''||l_proc_name||''' AND start_dt = '||l_start_time||' AND exec_id = '|| l_proc_id||'';
      ROLLBACK;
      commit;
  END;
exception
  when others then
    l_sqlerrm := sqlerrm;
  execute immediate 'UPDATE '|| l_log_tbl_name ||' SET log_mesg = ''DELETE FAILED ERROR: ' || l_sqlerrm || ''', end_dt = SYSDATE WHERE proc_name = '''||l_proc_name||''' AND start_dt = '||l_start_time||' AND exec_id = '|| l_proc_id||'';
  ROLLBACK;
    commit;
END;
