#!/bin/bash

FILE="conc_08012018.csv"

sqlplus -s paesdan/paeshp1@ihp1  <<EOF

SET PAGESIZE 0
SET ECHO OFF
SET COLSEP "|"
SET LINESIZE 10000
SET FEEDBACK OFF
SET TRIMSPOOL ON 
SET SQLPROMPT ''

SPOOL $FILE

SELECT DISTINCT
       CUST.CUSTOMERID AS NO_CLI,
       CUST.CUSTOMERACCOUNTNO AS NO_CPT_CLI,
       FIRST_VALUE (
          CASE
             WHEN PDCT_CAR.CARACTERISTICNAME = 'ID_COMPTE_INTERNET'
             THEN
                PDCT_CAR.CARACTERISTICVALUE
             ELSE
                NULL
          END)
       IGNORE NULLS
       OVER (PARTITION BY PDCT.PRODUCTID, PDCT.PARENTPRODUCTID)
          AS ID_CPT_INET,
       FIRST_VALUE (
          CASE
             WHEN PDCT_CAR.CARACTERISTICNAME = 'NUMERO_OFFRE_SPECIALE'
             THEN
                PDCT_CAR.CARACTERISTICVALUE
             ELSE
                NULL
          END)
       IGNORE NULLS
       OVER (PARTITION BY PDCT.PRODUCTID, PDCT.PARENTPRODUCTID)
          AS NO_OFFRE_SPEC,
       CASE
          WHEN FIRST_VALUE (
                  CASE
                     WHEN PDCT_CAR.CARACTERISTICNAME =
                             'NUMERO_OFFRE_SPECIALE'
                     THEN
                        PDCT_CAR.CARACTERISTICVALUE
                     ELSE
                        NULL
                  END)
               IGNORE NULLS
               OVER (PARTITION BY PDCT.PRODUCTID, PDCT.PARENTPRODUCTID)
                  IS NOT NULL
          THEN
             PDCT.PRODUCTEFFECTIVESTARTDATE
          ELSE
             NULL
       END
          AS DT_DEB_OFFRE,
       CASE
          WHEN FIRST_VALUE (
                  CASE
                     WHEN PDCT_CAR.CARACTERISTICNAME =
                             'NUMERO_OFFRE_SPECIALE'
                     THEN
                        PDCT_CAR.CARACTERISTICVALUE
                     ELSE
                        NULL
                  END)
               IGNORE NULLS
               OVER (PARTITION BY PDCT.PRODUCTID, PDCT.PARENTPRODUCTID)
                  IS NOT NULL
          THEN
             PDCT.PRODUCTEFFECTIVEENDDATE
          ELSE
             NULL
       END
          AS DT_FIN_OFFRE,
       FIRST_VALUE (
          CASE
             WHEN PDCT_CAR.CARACTERISTICNAME = 'QTE_OFFRE_TOTAL'
             THEN
                PDCT_CAR.CARACTERISTICVALUE
             ELSE
                NULL
          END)
       IGNORE NULLS
       OVER (PARTITION BY PDCT.PRODUCTID, PDCT.PARENTPRODUCTID)
          AS QTE_OFFRE_TOTAL,
       FIRST_VALUE (
          CASE
             WHEN PDCT_CAR.CARACTERISTICNAME = 'QTE_OFFRE_VARIATION'
             THEN
                PDCT_CAR.CARACTERISTICVALUE
             ELSE
                NULL
          END)
       IGNORE NULLS
       OVER (PARTITION BY PDCT.PRODUCTID, PDCT.PARENTPRODUCTID)
          AS QTE_OFFRE_VAR,
       FIRST_VALUE (
          CASE
             WHEN PDCT_CAR.CARACTERISTICNAME = 'SGA_NUMERO_COMPOSANTE'
             THEN
                PDCT_CAR.CARACTERISTICVALUE
             ELSE
                NULL
          END)
       IGNORE NULLS
       OVER (PARTITION BY PDCT.PRODUCTID, PDCT.PARENTPRODUCTID)
          AS NO_PROD,
       PDCT.PRODUCTID AS NO_SEQ_PROD,
       PDCT.PARENTPRODUCTID AS NO_SEQ_PARENT,
       FIRST_VALUE (
          CASE
             WHEN PDCT_CAR.CARACTERISTICNAME = 'SGA_TYPE_COMPOSANTE'
             THEN
                PDCT_CAR.CARACTERISTICVALUE
             ELSE
                NULL
          END)
       IGNORE NULLS
       OVER (PARTITION BY PDCT.PRODUCTID, PDCT.PARENTPRODUCTID)
          AS TYP_PROD,
       FIRST_VALUE (
          CASE
             WHEN PDCT_CAR.CARACTERISTICNAME = 'COMMANDE_EN_COURS'
             THEN
                PDCT_CAR.CARACTERISTICVALUE
             ELSE
                NULL
          END)
       IGNORE NULLS
       OVER (PARTITION BY PDCT.PRODUCTID, PDCT.PARENTPRODUCTID)
          AS CD_EXIS_COM,
       CASE
          WHEN FIRST_VALUE (
                  CASE
                     WHEN PDCT_CAR.CARACTERISTICNAME =
                             'NUMERO_OFFRE_SPECIALE'
                     THEN
                        PDCT_CAR.CARACTERISTICVALUE
                     ELSE
                        NULL
                  END)
               IGNORE NULLS
               OVER (PARTITION BY PDCT.PRODUCTID, PDCT.PARENTPRODUCTID)
                  IS NULL
          THEN
             PDCT.PRODUCTEFFECTIVESTARTDATE
          ELSE
             NULL
       END
          AS DT_DEB_PROD,
       CASE
          WHEN FIRST_VALUE (
                  CASE
                     WHEN PDCT_CAR.CARACTERISTICNAME =
                             'NUMERO_OFFRE_SPECIALE'
                     THEN
                        PDCT_CAR.CARACTERISTICVALUE
                     ELSE
                        NULL
                  END)
               IGNORE NULLS
               OVER (PARTITION BY PDCT.PRODUCTID, PDCT.PARENTPRODUCTID)
                  IS NULL
          THEN
             PDCT.PRODUCTEFFECTIVEENDDATE
          ELSE
             NULL
       END
          AS DT_FIN_PROD,
       PDCT.PRODUCTSTATUS AS CODE_APPROV,
       FIRST_VALUE (
          CASE
             WHEN PDCT_CAR.CARACTERISTICNAME = 'SGA_NUMERO_SERVICE'
             THEN
                PDCT_CAR.CARACTERISTICVALUE
             ELSE
                NULL
          END)
       IGNORE NULLS
       OVER (PARTITION BY PDCT.PRODUCTID, PDCT.PARENTPRODUCTID)
          AS NO_SERV_ASSOC_A,
       FIRST_VALUE (
          CASE
             WHEN PDCT_CAR.CARACTERISTICNAME = 'DESCRIPTION_FRANCAIS'
             THEN
                PDCT_CAR.CARACTERISTICVALUE
             ELSE
                NULL
          END)
       IGNORE NULLS
       OVER (PARTITION BY PDCT.PRODUCTID, PDCT.PARENTPRODUCTID)
          AS DESC_PROD_FR,
       FIRST_VALUE (
          CASE
             WHEN PDCT_CAR.CARACTERISTICNAME = 'DESCRIPTION_ANGLAIS'
             THEN
                PDCT_CAR.CARACTERISTICVALUE
             ELSE
                NULL
          END)
       IGNORE NULLS
       OVER (PARTITION BY PDCT.PRODUCTID, PDCT.PARENTPRODUCTID)
          AS DESC_PROD_EN
  FROM IHPRODUCT PDCT,
       IHPRODUCTCARACTERISTIC PDCT_CAR,
       IHCUSTOMERACCOUNT CUST
 WHERE     PDCT_CAR.PRODUCTRECID = PDCT.PRODUCTRECID
       AND CUST.CUSTOMERACCOUNTNO = PDCT.CUSTOMERACCOUNTNO
       AND CUST.IHEFFECTIVEENDTIMESTAMP IS NULL
       AND PDCT.CUSTOMERACCOUNTNO IN
              ('500001010015',
               '500001100014',
               '500001440014',
               '500002680014',
               '500002840014',
               '500003730016',
               '500004620018',
               '500005000012',
               '500005350011',
               '500005860019',
               '500006240013',
               '500006910011',
               '500007300014',
               '500007480014',
               '500007720013',
               '500008530015',
               '500009260018',
               '500010000015',
               '500010270014',
               '500011670014',
               '500011830014',
               '500011910014',
               '500012050018',
               '500012480017',
               '500013960017',
               '500014180011',
               '500014340011',
               '500014930019',
               '500015660011',
               '500016470014',
               '500016550013',
               '500017100016',
               '500017600015',
               '500018410018',
               '500019140010',
               '500019220010',
               '500019300010',
               '500019570018',
               '500020740014',
               '500021390017',
               '500022790017',
               '500023170011',
               '500023330011',
               '500024220013',
               '500024810011',
               '500025200014',
               '500025380014',
               '500025970012',
               '500026350016',
               '500026940014',
               '500027080018',
               '500027160018',
               '500027320018',
               '500027590016',
               '500029530010',
               '500029610010',
               '500030200017',
               '500031350019',
               '500031600017',
               '500031780017',
               '500031860017',
               '500032240011',
               '500032400011',
               '500032590019',
               '500033050013',
               '500033560011',
               '500035180016',
               '500035260016',
               '500035690014',
               '500036150018',
               '500036580016',
               '500036660016',
               '500036900016',
               '500037470019',
               '500037800017',
               '500038100011',
               '500038280011',
               '500039090013',
               '500039680011',
               '500040500018',
               '500040690017',
               '500040850017',
               '500040930017',
               '500041310011',
               '500041400010',
               '500042550011',
               '500042710011',
               '500042800010',
               '500042980010',
               '500043100014',
               '500043360014',
               '500043520013',
               '500043790012',
               '500043870012',
               '500043950012',
               '500044410016',
               '500045300018',
               '500046200019',
               '500046700018',
               '500046970017',
               '500047270011',
               '500047510010',
               '500047940019',
               '500048240013',
               '500048320013',
               '500048590011',
               '500048910011',
               '500049300014',
               '500050060010',
               '500050140010',
               '500050220010',
               '500050650018',
               '500051110012',
               '500051200011',
               '500051890019',
               '500052190013',
               '500052350013');

SPOOL OFF
EXIT
EOF
