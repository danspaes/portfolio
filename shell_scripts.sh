//Demand sales scripts 
## Script para FTP (rodar como ora_mstr)
#!/bin/bash
echo "target directory on Micstro server is /disk07/oradir/RDWDEV/in"
sftp ora_mstr@corpfs<<EOF
get ../EcommDemandSales/daily/*/*csv
quit
EOF

## Script para concatenar os arquivos e adicionar o Division_ID

#!/bin/bash
cd /disk07/oradir/RDWDEV/in
data=$(date '+%m%d%Y%H%M%S');
rm -f ./OrderTransaction*tmp*.csv
if [ $(ls -1 ./OrderTransaction-* | wc -l) != 0 ]
                then
        if [ $(ls -1 ./OrderTransaction-AE* | wc -l) != 0 ]
                        then
						rm -f OrderTransaction-AE_tmp.csv OrderTransaction-AE_tmp2.csv
						for file in $(ls -1 ./OrderTransaction-AE*); do 
							#AE
							tail -n+2 $file | head -n -1 >> OrderTransaction-AE_tmp.csv;
							tail -n-1 $file | rev | sed 's/^//' | rev >> OrderTransaction-AE_tmp.csv;
						done
							sed 's/$/,"27"/' OrderTransaction-AE_tmp.csv > OrderTransaction-AE_tmp2.csv
		else
                        echo "there aren't any Addition Elle files to process"
        fi
        if [ $(ls -1 ./OrderTransaction-PE* | wc -l) != 0 ]
                        then
						rm -f OrderTransaction-PE_tmp.csv OrderTransaction-PE_tmp2.csv
						for file in $(ls -1 ./OrderTransaction-PE*); do 
							#PE
							tail -n+2 $file | head -n -1 >> OrderTransaction-PE_tmp.csv;
							tail -n-1 $file | rev | sed 's/^//' | rev >> OrderTransaction-PE_tmp.csv;
						done
                        sed 's/$/,"29"/' OrderTransaction-PE_tmp.csv > OrderTransaction-PE_tmp2.csv
        else
                        echo "there aren't any Pennington files to process"
        fi
        if [ $(ls -1 ./OrderTransaction-RE* | wc -l) != 0 ]
                        then
						rm -f OrderTransaction-RE_tmp.csv OrderTransaction-RE_tmp2.csv
						for file in $(ls -1 ./OrderTransaction-RE*); do 
							#RE
							tail -n+2 $file | head -n -1 >> OrderTransaction-RE_tmp.csv;
							tail -n-1 $file | rev | sed 's/^//' | rev >> OrderTransaction-RE_tmp.csv;
						done
                        sed 's/$/,"26"/' OrderTransaction-RE_tmp.csv > OrderTransaction-RE_tmp2.csv
        else
                        echo "there aren't any Reitmans files to process"
        fi
        if [ $(ls -1 ./OrderTransaction-RW* | wc -l) != 0 ]
                        then
						rm -f OrderTransaction-RW_tmp.csv OrderTransaction-RW_tmp2.csv
						for file in $(ls -1 ./OrderTransaction-RW*); do 
							#RW
							tail -n+2 $file | head -n -1 >> OrderTransaction-RW_tmp.csv;
							tail -n-1 $file | rev | sed 's/^//' | rev >> OrderTransaction-RW_tmp.csv;
						done
                        sed 's/$/,"30"/' OrderTransaction-RW_tmp.csv > OrderTransaction-RW_tmp2.csv
        else
                        echo "there aren't any RW files to process"
        fi
        if [ $(ls -1 ./OrderTransaction-TH* | wc -l) != 0 ]
                        then
						rm -f OrderTransaction-TH_tmp.csv OrderTransaction-TH_tmp2.csv
						for file in $(ls -1 ./OrderTransaction-TH*); do 
							#TH
							tail -n+2 $file | head -n -1 >> OrderTransaction-TH_tmp.csv;
							tail -n-1 $file | rev | sed 's/^//' | rev >> OrderTransaction-TH_tmp.csv;
						done
                        sed 's/$/,"31"/' OrderTransaction-TH_tmp.csv > OrderTransaction-TH_tmp2.csv
        else
                        echo "there aren't any Thyme files to process"
        fi
        #Concatenate all
		
		sed 's/\",\"/\"\~\^\~\"/g;s/$/\~\^\~'${data}'/' ./OrderTransaction*tmp2.csv > ordertransaction.csv

		#Remove temporary
    rm -f ./OrderTransaction*
        echo "files concatenated successfully"
else
        echo "there aren't any files from any brand to process"
fi
