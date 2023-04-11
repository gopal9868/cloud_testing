import oct_config,oct_output,db_connection
sf_con_prd = db_connection.sf_con_prd
sf_con_prd.cursor().execute("USE warehouse lgcy_lg")
sf_con_prd.cursor().execute("USE  stg1p.lgcy_agt_stg")
script_path='C:/project/cloud/history load/'
file_path = 'C:/Temp/'
with open('C:/Testing/Input/Hist_Table_List.txt', 'r') as tabf1:
    for line in tabf1:
        tablist = line.split('|')
        tabschema = "'" + tablist[0] + "'"
        tabname =  tablist[0].rstrip('\n')
        file_name=tablist[0].rstrip('\n')+'.CSV'
        table_script = tablist[0].rstrip('\n')

        remove_query1 = """
                             remove @edw_int_stage_csv/{file_name3}
                             """.format(file_name3=file_name)
        #print(remove_query1)
        put_query1 = """
                            put file://{file_name1}  @edw_int_stage_csv
                          """.format(file_name1=file_path + file_name)
        #print(put_query1)
        truncate_query=''
        with open(script_path + 'Hist_Truncate_Scripts/' + table_script + '.txt', 'r')as f1:
            for line in f1:
                truncate_query = truncate_query + line
        copy_query = ''
        with open(script_path+'Hist_Copy_Scripts/'+table_script+'.txt','r')as f1:
            for line in f1:
                copy_query=copy_query +line


        sf_con_prd.cursor().execute(put_query1)
        sf_con_prd.cursor().execute(truncate_query)
        sf_con_prd.cursor().execute(copy_query)
    #print(sf_query.remove_query1)
    #print(sf_query.remove_query2)

    #print(sf_query.ins_query2)
#sf_con_qa.close()
sf_con_prd.close()
