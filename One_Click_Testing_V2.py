#Developed by Gopal Kumar Roy
from datetime import datetime
import ibm_db
import oct_config,oct_output,metadata_sql,sf_query,db_connection
run_ts = datetime.now()
config_path = oct_config.config_path
output_path = oct_config.output_path
input_path = oct_config. input_path
user_id=oct_config.user_id
onprem_cnt_query = oct_output.onprem_cnt_query
cloud_cnt_query = oct_output.cloud_cnt_query
onprem_cnt_result = oct_output.onprem_cnt_result
cloud_cnt_result = oct_output.cloud_cnt_result
with open(output_path + onprem_cnt_result, 'w') as f22,open(output_path + cloud_cnt_result, 'w') as f23:
    pass
#sf_con_qa = db_connection.sf_con_qa
sf_con_prd = db_connection.sf_con_prd
connection_edw =db_connection.connection_edw
with open(input_path + 'Table_List.txt', 'r') as tabf1:
    next(tabf1)
    for line in tabf1:
        print(line)
        tablist = line.split('|')
        tabschema = "'" + tablist[0] + "'"
        tabname = "'" + tablist[1] + "'"
        valyear = tablist[2]
        valtype = "'"+tablist[3]+ "'"
        fname = "'"+tablist[4].rstrip('\n')+ "'"
        valtype_query = tablist[3]
        fname_query = tablist[4].rstrip('\n')
        filter_min_value_input=tablist[5]
        filter_max_value_input = tablist[6]
        filter_value_input=tablist[7]
        select_column_input = tablist[8]
        filter_fields_input=tablist[9].rstrip('\n')
        # print(tabschema,tabname,valyear,valtype)
        metadata_count_sql =metadata_sql.metadata_count_sql(tabschema,tabname)
        #print(metadata_count_sql)
        sf_con_prd.cursor().execute("USE warehouse lgcy_lg")
        #sf_con_prd.cursor().execute("USE  stg1p.lgcy_agt_stg")
        # con.cursor().execute("put file://C:\Project\Cloud\Data\d_date_2.csv  @edw_int_stage_csv")
        for (ONPREM_DB_NAME, onprem_schema_name, onprem_table_name, CLOUD_DB_NAME, CLOUD_SCHEMA_NAME, CLOUD_TABLE_NAME,appname) in sf_con_prd.cursor().execute(metadata_count_sql):
            with open(output_path + onprem_cnt_query, 'w') as premf1, open(output_path + cloud_cnt_query, 'w') as cf1:
                #print(validation_year)

                prem_query_line='SELECT '+ select_column_input+' VALUE FROM ' + ONPREM_DB_NAME + '.' + onprem_schema_name + '.' + onprem_table_name + ' where ' + filter_fields_input + '>=' + "'" + filter_min_value_input + "'" + ' and ' + filter_fields_input + '<' + "'" + filter_max_value_input + "'"+' and '+filter_value_input
                premf1.write(prem_query_line)
             #   print(prem_query_line)
                cloud_query_line='SELECT '+ select_column_input+' VALUE FROM ' + CLOUD_DB_NAME + '.' + CLOUD_SCHEMA_NAME + '.' + CLOUD_TABLE_NAME + ' where ' + filter_fields_input + '>=' + "'" + filter_min_value_input + "'" + ' and ' + filter_fields_input + '<' + "'" + filter_max_value_input + "'"+' and '+filter_value_input
                cf1.write(cloud_query_line)
                   # print('SELECT COUNT(1) COUNT_RECORD FROM ' + CLOUD_DB_NAME + '.' + CLOUD_SCHEMA_NAME + '.' + CLOUD_TABLE_NAME+' where '+filter_fields+'>='+"'"+filter_min_value+"'"+' and '+filter_fields+'<'+"'"+filter_max_value+"'")
            query_str_onprem = ''
            query_str_cloud = ''
            with open(output_path + onprem_cnt_query, 'r') as uf2:
                for line_prem in uf2:
                  #  print(line_prem)
                    query_str_onprem = query_str_onprem + line_prem
            #print(query_str_onprem)
            with open(output_path + cloud_cnt_query, 'r') as uf3:
                for line_cloud in uf3:
                   # print(line_cloud)
                    query_str_cloud = query_str_cloud + line_cloud
            #print(query_str_cloud)
            stmt = ibm_db.exec_immediate(connection_edw, query_str_onprem)
            data = ibm_db.fetch_assoc(stmt)
            #print(data)
            with open(output_path + onprem_cnt_result, 'a+') as f2:
                f2.write(appname + '|' + CLOUD_DB_NAME + '|' + CLOUD_SCHEMA_NAME + '|' + CLOUD_TABLE_NAME + '|' + str(
                    data['VALUE']) + '|' + str(valyear) + '|' + str(run_ts) +'|'+valtype_query +'|'+fname_query+'|'+
                         filter_min_value_input+'|'+filter_max_value_input+'|'+filter_value_input+'|'+filter_fields_input+'\n')
            sf_con_prd.cursor().execute("USE warehouse lgcy_lg")
            for (COUNT_RECORD) in sf_con_prd.cursor().execute(query_str_cloud):
             with open(output_path + cloud_cnt_result, 'a+') as cfc1:
                    cfc1.write(
                        appname + '|' + CLOUD_DB_NAME + '|' + CLOUD_SCHEMA_NAME + '|' + CLOUD_TABLE_NAME + '|' + str(
                            COUNT_RECORD[0]) + '|' + str(valyear) + '|' + str(run_ts) +'|'+valtype_query+'|'+fname_query+'|'+
                            filter_min_value_input+'|'+filter_max_value_input+'|'+filter_value_input+'|'+filter_fields_input+'\n')
            sf_con_prd.cursor().execute("USE  stg1p.lgcy_agt_stg")

    sf_con_prd.cursor().execute(sf_query.remove_query1)
    #print(sf_query.remove_query1)
    sf_con_prd.cursor().execute(sf_query.remove_query2)
    #print(sf_query.remove_query2)
    sf_con_prd.cursor().execute(sf_query.put_query1)
    #print(sf_query.put_query1)
    sf_con_prd.cursor().execute(sf_query.put_query2)
    #print(sf_query.put_query2)
    sf_con_prd.cursor().execute(sf_query.delete_query1)
    #print(sf_query.delete_query1)
    sf_con_prd.cursor().execute(sf_query.ins_query1)
    #print(sf_query.ins_query1)
    sf_con_prd.cursor().execute(sf_query.ins_query2)
    #print(sf_query.ins_query2)
#sf_con_qa.close()
sf_con_prd.close()
ibm_db.close(connection_edw)