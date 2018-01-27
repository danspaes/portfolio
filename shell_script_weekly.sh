//Demand sales scripts 
## Script para FTP (rodar como ora_mstr)
#!/bin/bash
echo "target directory on Micstro server is /disk07/oradir/RDWDEV/in"
sftp ora_mstr@corpfs<<EOF
get ../EcommDemandSales/weekly/*/DS_PROJ/*csv
quit
EOF

## Script para concatenar os arquivos e adicionar o Division_ID

#!/bin/bash
cd /disk07/oradir/RDWDEV/in
rm -f ./OrderTransaction*tmp*.csv
if [ $(ls -1 ./OrderTransaction-W* | wc -l) != 0 ]
                then
        if [ $(ls -1 ./OrderTransaction-W-AE* | wc -l) != 0 ]
                        then
						rm -f OrderTransaction-W-AE_tmp.csv OrderTransaction-W-AE_tmp2.csv
						for file in $(ls -1 ./OrderTransaction-W-AE*); do 
							#AE
							tail -n+2 $file | head -n -1 >> OrderTransaction-W-AE_tmp.csv;
							tail -n-1 $file | rev | sed 's/^//' | rev >> OrderTransaction-W-AE_tmp.csv;
						done
							sed 's/$/,"27"/' OrderTransaction-W-AE_tmp.csv > OrderTransaction-W-AE_tmp2.csv
		else
                        echo "there aren't any Addition Elle files to process"
        fi
        if [ $(ls -1 ./OrderTransaction-W-PE* | wc -l) != 0 ]
                        then
						rm -f OrderTransaction-W-PE_tmp.csv OrderTransaction-W-PE_tmp2.csv
						for file in $(ls -1 ./OrderTransaction-W-PE*); do 
							#PE
							tail -n+2 $file | head -n -1 >> OrderTransaction-W-PE_tmp.csv;
							tail -n-1 $file | rev | sed 's/^//' | rev >> OrderTransaction-W-PE_tmp.csv;
						done
                        sed 's/$/,"29"/' OrderTransaction-W-PE_tmp.csv > OrderTransaction-W-PE_tmp2.csv
        else
                        echo "there aren't any Pennington files to process"
        fi
        if [ $(ls -1 ./OrderTransaction-W-RE* | wc -l) != 0 ]
                        then
						rm -f OrderTransaction-W-RE_tmp.csv OrderTransaction-W-RE_tmp2.csv
						for file in $(ls -1 ./OrderTransaction-W-RE*); do 
							#RE
							tail -n+2 $file | head -n -1 >> OrderTransaction-W-RE_tmp.csv;
							tail -n-1 $file | rev | sed 's/^//' | rev >> OrderTransaction-W-RE_tmp.csv;
						done
                        sed 's/$/,"26"/' OrderTransaction-W-RE_tmp.csv > OrderTransaction-W-RE_tmp2.csv
        else
                        echo "there aren't any Reitmans files to process"
        fi
        if [ $(ls -1 ./OrderTransaction-W-RW* | wc -l) != 0 ]
                        then
						rm -f OrderTransaction-W-RW_tmp.csv OrderTransaction-W-RW_tmp2.csv
						for file in $(ls -1 ./OrderTransaction-W-RW*); do 
							#RW
							tail -n+2 $file | head -n -1 >> OrderTransaction-W-RW_tmp.csv;
							tail -n-1 $file | rev | sed 's/^//' | rev >> OrderTransaction-W-RW_tmp.csv;
						done
                        sed 's/$/,"30"/' OrderTransaction-W-RW_tmp.csv > OrderTransaction-W-RW_tmp2.csv
        else
                        echo "there aren't any RW files to process"
        fi
        if [ $(ls -1 ./OrderTransaction-W-TH* | wc -l) != 0 ]
                        then
						rm -f OrderTransaction-W-TH_tmp.csv OrderTransaction-W-TH_tmp2.csv
						for file in $(ls -1 ./OrderTransaction-W-TH*); do 
							#TH
							tail -n+2 $file | head -n -1 >> OrderTransaction-W-TH_tmp.csv;
							tail -n-1 $file | rev | sed 's/^//' | rev >> OrderTransaction-W-TH_tmp.csv;
						done
                        sed 's/$/,"31"/' OrderTransaction-W-TH_tmp.csv > OrderTransaction-W-TH_tmp2.csv
        else
                        echo "there aren't any Thyme files to process"
        fi
        #Concatenate all
    sed 's/$/,'${data}'/' ./OrderTransaction*tmp2.csv > ordertransaction.csv
    #Remove temporary
    rm -f ./OrderTransaction-W*
        echo "files concatenated successfully"
else
        echo "there aren't any files from any brand to process"
fi
