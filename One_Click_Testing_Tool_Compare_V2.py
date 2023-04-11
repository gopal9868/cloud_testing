from datetime import datetime
import ibm_db
import json,ast
import re
import oct_config,oct_output,metadata_sql,sf_query,db_connection
run_ts = datetime.now()
config_path = oct_config.config_path
output_path = oct_config.output_path
input_path = oct_config. input_path
user_id=oct_config.user_id
onprem_com_query = oct_output.onprem_com_query
cloud_com_query = oct_output.cloud_com_query
onprem_com_result = oct_output.onprem_com_result
cloud_com_result = oct_output.cloud_com_result
file_name_com=oct_config.output_path+oct_output.onprem_com_result
file_name_com_2=oct_output.onprem_com_result

#sf_con_qa = db_connection.sf_con_qa
sf_con_prd = db_connection.sf_con_prd
connection_edw =db_connection.connection_edw
with open(input_path + 'Table_List_Compare.txt', 'r') as tabf1:
    next(tabf1)
    for line in tabf1:
        tablist = line.split('|')
        tabschema = "'" + tablist[0] + "'"
        tabname = "'" + tablist[1] + "'"
        print(tabname)
        valyear = tablist[2]
        valtype_input = tablist[3]
        filter_min_value_input = tablist[4]
        filter_max_value_input = tablist[5]
        filter_value_input = tablist[6]
        select_column_input = tablist[7].rstrip(',')
        #print(select_column_input)
        filter_fields_input = tablist[8]
        primary_key_input = tablist[9]
        #print(primary_key_input)
        no_of_rows_input = tablist[10]
        order_by_input = tablist[11].rstrip('\n')
        onprem_db_name=tablist[12].rstrip('\n')
        onprem_schema_name=tablist[13].rstrip('\n')
        onprem_table_name=tablist[14].rstrip('\n')
        cloud_db_name=tablist[15].rstrip('\n')
        col_list_input = primary_key_input + ',' + select_column_input
        tabschema_input = tablist[0]
        tabname_input = tablist[1]
        #print(col_list_input)
        col_list_dict = col_list_input.split(',')
        #print(col_list_dict)
        y = len(col_list_dict)
        i = 0
        col_list = ''
        while (i < y):
            col_list = col_list + 'col_' + str(i + 1) + ','
            i = i + 1
        #print(col_list1
        pk_col_cnt = len(primary_key_input.split(','))
        pk_col_i = 0
        pk_col_l = ''
        while (pk_col_i < pk_col_cnt):
            if (pk_col_i == (pk_col_cnt - 1)):
                pk_col_l = pk_col_l + 'col_' + str(pk_col_i + 1)
            else:
                pk_col_l = pk_col_l + 'col_' + str(pk_col_i + 1) + ','
            pk_col_i = pk_col_i + 1
        new_select_column_input1 = ''
        new_primary_key_input1 = ''
        new_select_column_input = ''
        new_primary_key_input = ''
        si = 0
        pi = 0
        a_dict = select_column_input.split(',')
        with open(output_path + onprem_com_result, 'w') as f22, open(output_path + cloud_com_result, 'w') as f23:
            pass
        #print(a_dict)
        while (si < len(a_dict)):
            if '_TS' in a_dict[si]:
                new_select_column_input1 = new_select_column_input1 + "substr(" + a_dict[si] + ",1,10)||' '||substr(" + \
                                           a_dict[si] + ",12,2)||':'||substr(" + a_dict[si] + ",15,2)||':'||substr(" + \
                                           a_dict[si] + ",18,2) " + a_dict[si] + ','
                new_select_column_input = new_select_column_input + "substr(" + a_dict[si] + ",1,10)||' '||substr(" + \
                                          a_dict[si] + ",12,2)||':'||substr(" + a_dict[si] + ",15,2)||':'||substr(" + \
                                          a_dict[si] + ",18,2)" + ','
            else:
                new_select_column_input1 = new_select_column_input1 + a_dict[si] + ','
                new_select_column_input = new_select_column_input + a_dict[si] + ','
            si = si + 1
        #print(new_select_column_input1)
        #print(new_select_column_input)
        b_dict = primary_key_input.split(',')
        #print(b_dict)
        while (pi < len(b_dict)):
            if '_TS' in b_dict[pi]:
                new_primary_key_input1 = new_primary_key_input1 + "substr(" + b_dict[pi] + ",1,10)||' '||substr(" + \
                                         b_dict[pi] + ",12,2)||':'||substr(" + b_dict[pi] + ",15,2)||':'||substr(" + \
                                         b_dict[pi] + ",18,2) " + b_dict[pi] + ','
                new_primary_key_input = new_primary_key_input + "substr(" + b_dict[pi] + ",1,10)||' '||substr(" + \
                                        b_dict[pi] + ",12,2)||':'||substr(" + b_dict[pi] + ",15,2)||':'||substr(" + \
                                        b_dict[pi] + ",18,2)" + ','
            else:
                new_primary_key_input1 = new_primary_key_input1 + b_dict[pi] + ','
                new_primary_key_input = new_primary_key_input + 'TRIM('+b_dict[pi]+')' + ','
            pi = pi + 1
        #print(new_primary_key_input1)
        #print(new_primary_key_input)
        #wip
        #print(metadata_count_sql)
        #sf_con_prd.cursor().execute("USE warehouse lgcy_md")
        # sf_con_qa.cursor().execute("USE  stg1p.lgcy_agt_stg")
        # con.cursor().execute("put file://C:\Project\Cloud\Data\d_date_2.csv  @edw_int_stage_csv")
        #for (ONPREM_DB_NAME, onprem_schema_name, onprem_table_name, CLOUD_DB_NAME, CLOUD_SCHEMA_NAME, CLOUD_TABLE_NAME,appname) in sf_con_prd.cursor().execute(metadata_com_sql):
         #   with open(output_path + onprem_com_query, 'w') as premf1, open(output_path + cloud_com_query, 'w') as cf1:
                #print(validation_year)
        #with open(output_path + onprem_com_query, 'w') as premf1, open(output_path + cloud_com_query, 'w') as cf1:
        prem_query_line='SELECT '+ new_primary_key_input1+new_select_column_input1.rstrip(',')+' FROM ' + onprem_db_name + '.' + onprem_schema_name + '.' + onprem_table_name + ' where ' + filter_fields_input + '>=' + "'" + filter_min_value_input + "'" + ' and ' + filter_fields_input + '<' + "'" + filter_max_value_input + "'"+' and '+filter_value_input+' order by '+order_by_input+' fetch first '+str(no_of_rows_input)+' rows only'
         #   premf1.write(prem_query_line)
        print(prem_query_line)
        cloud_query_line = 'SELECT ' + new_primary_key_input1 + new_select_column_input1 + "'" + tabschema_input + "','" + tabname_input + "'," + "'" + file_name_com_2 + "','" + primary_key_input + "'," + " current_timestamp::timestamp_ntz FROM " + cloud_db_name + '.' + tabschema_input + '.' + tabname_input + ' where ' + filter_fields_input + '>=' + "'" + filter_min_value_input + "'" + ' and ' + filter_fields_input + '<' + "'" + filter_max_value_input + "'" + ' and ' + filter_value_input  +' order by '+order_by_input+' limit '+str(no_of_rows_input)

                #cloud_query_line = 'SELECT ' + new_primary_key_input1  + new_select_column_input1 +"'"+CLOUD_SCHEMA_NAME+"','"+CLOUD_TABLE_NAME+"',"+"'"+file_name_com_2+"','"+primary_key_input+"',"+" current_timestamp::timestamp_ntz FROM "  + CLOUD_DB_NAME + '.' + CLOUD_SCHEMA_NAME + '.' + CLOUD_TABLE_NAME + ' where ' + filter_fields_input + '>=' + "'" + filter_min_value_input + "'" + ' and ' + filter_fields_input + '<' + "'" + filter_max_value_input + "'" + ' and ' + filter_value_input + ' and (' + new_primary_key_input.rstrip(',') + ') in (select ' + pk_col_l + " from cns1p.LGCY_IRM_DM.TESTING_DATA_SOURCE where file_name='" + file_name_com_2 + "' )"
            #cf1.write(cloud_query_line)
                   # print('SELECT COUNT(1) COUNT_RECORD FROM ' + CLOUD_DB_NAME + '.' + CLOUD_SCHEMA_NAME + '.' + CLOUD_TABLE_NAME+' where '+filter_fields+'>='+"'"+filter_min_value+"'"+' and '+filter_fields+'<'+"'"+filter_max_value+"'")
            #query_str_onprem = ''
            #query_str_cloud = ''
            #with open(output_path + onprem_com_query, 'r') as uf2:
            #    for line_prem in uf2:
                  #  print(line_prem)
             #       query_str_onprem = query_str_onprem + line_prem
            #print(query_str_onprem)
            #with open(output_path + cloud_com_query, 'r') as uf3:
              #  for line_cloud in uf3:
                   # print(line_cloud)
              #      query_str_cloud = query_str_cloud + line_cloud

            #print(query_str_cloud)
        
           # onprem_write=appname + '|' + CLOUD_DB_NAME + '|' + CLOUD_SCHEMA_NAME + '|' + CLOUD_TABLE_NAME + col_list_exp+'\n'
            #print(onprem_write)
          #  col_1=col_list_dict[0],col_2=col_list_dict[1],col_3=col_list_dict[2],col_4=col_list_dict[3],col_5=col_list_dict[4],col_6=col_list_dict[5],col_7=col_list_dict[6]
           # col_8 = col_list_dict[7],col_9=col_list_dict[8],col_10=col_list_dict[9],col_11=col_list_dict[10],col_12=col_list_dict[11],col_12=col_list_dict[12]
           # print(col_4)
        stmt = ibm_db.exec_immediate(connection_edw, prem_query_line)
        data = ibm_db.fetch_assoc(stmt)
        while data !=False:
                  #print(data)
                  #data1=list(data.values())
                  #print(data1)
                  #data_l=len(data)
                  #data_cnt=0
                  data_line=''
                  lcnt = len(col_list_dict)
                  lincr = 0
                  #print(col_list_dict[0])
                  while(lincr<len(col_list_dict)):
                      data_val=data[col_list_dict[lincr].rstrip(' ').lstrip(' ')]
                      #print(data_val)
                      lincr = lincr+1
                      data_line = data_line + str(data_val).lstrip(' ').rstrip(' ')+'|'
                  #while(data_cnt<data_l):
                   #     data_col=data['BILL_PRTY_KEY']
                    #    print(data_col)
                     #   data_cnt=data_cnt+1
                      #  data_line=data_line+str(data_col)+'|'
                  #print(data_line)
                  with open(output_path + onprem_com_result, 'a+') as f2:
                      f2.write(tabschema_input+'|'+tabname_input+'|'+primary_key_input+'|'+data_line+'\n')
                  data =ibm_db.fetch_assoc(stmt)
                  #print(data)

        prem_com_put_query1 = """
                        put file://{file_name1}  @S_EDW_INT_TESTING
                      """.format(file_name1=file_name_com)
        prem_com_remove_query1 = """
                         remove @S_EDW_INT_TESTING/{file_name3}
                         """.format(file_name3=file_name_com)

        prem_ins_query1 = """
                      Insert into cns1p.LGCY_IRM_DM.TESTING_DATA_SOURCE
                      select $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,'{file_name3}',current_timestamp::timestamp_ntz from 
                      @S_EDW_INT_TESTING/{file_name3}""".format(file_name3=file_name_com_2)
        #print(prem_com_put_query1)
        prem_del_query1 = """
                         delete from cns1p.LGCY_IRM_DM.TESTING_DATA_SOURCE where file_name='{file_name3}'""".format(file_name3=file_name_com_2)
        cloud_del_query1 = """
                         delete from cns1p.LGCY_IRM_DM.TESTING_DATA_TARGET where file_name='{file_name3}'""".format(file_name3=file_name_com_2)
        #query_str_cloud ='SELECT '+ primary_key_input+','+select_column_input+",'"+CLOUD_SCHEMA_NAME+"','"+CLOUD_TABLE_NAME+"',"+"'"+file_name_com_2+"','"+primary_key_input+"',"+" current_timestamp::timestamp_ntz FROM " + CLOUD_DB_NAME + '.' + CLOUD_SCHEMA_NAME + '.' + CLOUD_TABLE_NAME + ' where ' + filter_fields_input + '>=' + "'" + filter_min_value_input + "'" + ' and ' + filter_fields_input + '<' + "'" + filter_max_value_input + "'"+' and '+filter_value_input+' order by ' + order_by_input + ' limit ' + str(no_of_rows_input)
        primary_key_join = 'a,b'
        cloud_ins_query1 = """
                      INSERT INTO cns1p.LGCY_IRM_DM.TESTING_DATA_TARGET (
                      {col_list1} SCHEMA_NAME,TABLE_NAME,file_name,PRIMARY_KEY_COLUMN,PROCESS_TS ) {query_str_cloud1}""".format(col_list1=col_list,query_str_cloud1=cloud_query_line)
        cloud_del_query1 = """
                             delete from cns1p.LGCY_IRM_DM.TESTING_DATA_TARGET where file_name='{file_name3}'""".format(
        file_name3=file_name_com_2)
        sf_con_prd.cursor().execute("USE warehouse lgcy_lg")
        sf_con_prd.cursor().execute("USE schema stg1p.lgcy_agt_stg")
        #print(cloud_ins_query1)
        #print(prem_ins_query1)
        sf_con_prd.cursor().execute(prem_com_remove_query1)
        sf_con_prd.cursor().execute(prem_com_put_query1)
        sf_con_prd.cursor().execute(prem_del_query1)
        sf_con_prd.cursor().execute(prem_ins_query1)
        sf_con_prd.cursor().execute(cloud_del_query1)
        sf_con_prd.cursor().execute(cloud_ins_query1)
        pk_col_list = primary_key_input.split(',')
        pk_join = ''
        pk_concat = ''
        pk_concat2=''
        a = 0
        field_cnt = len(pk_col_list)
        while (a < field_cnt):
          if (a < (field_cnt - 1)):
            pk_join = pk_join + 'a.col_' + str(a + 1) + '=b.col_' + str(a + 1) + ' and '
            pk_concat = pk_concat + 'a.col_' + str(a + 1) + "||'~'||"
            pk_concat2 = pk_concat2 + 'b.col_' + str(a + 1) + "||'~'||"
          else:
            pk_join = pk_join + 'a.col_' + str(a + 1) + '=b.col_' + str(a + 1)
            pk_concat = pk_concat + 'a.col_' + str(a + 1)
            pk_concat2 = pk_concat2 + 'b.col_' + str(a + 1)
          a = a + 1
        cnt = 1
        missing_del_query1= """
                             delete from CNS1p.LGCY_IRM_DM.TESTING_RESULT_COMPARE where validation_field_name like '%MISSING'
                             and table_name='{table_name1}'""".format(table_name1=tabname_input)
        sf_con_prd.cursor().execute(missing_del_query1)
        compare_query_ins1 = """insert into CNS1p.LGCY_IRM_DM.TESTING_RESULT_COMPARE
                      select * from (SELECT 'ALL' col1,'{schema_name1}' col2,'{table_name1}' col3,'{primary_key_input1}' col4,{pk_concat1} col5,'{val_year1}' col6,current_timestamp::timestamp_ntz col7,a.col_{cnt1} src_col,
                      'CLOUD_MISSING' tgt_col ,'CLOUD_MISSING' col9,'COMPARE' col10,
                      '{filter_fields_input1}' col11,'{filter_value_input1}' col12,'{filter_min_value_input1}' col13,'{filter_max_value_input1}' col14 FROM
                      CNS1p.LGCY_IRM_DM.TESTING_DATA_SOURCE a  LEFT join CNS1p.LGCY_IRM_DM.TESTING_DATA_target b on 
                      a.TABLE_NAME=b.TABLE_NAME and a.SCHEMA_NAME=b.SCHEMA_NAME   AND {pk_join1} and
                      a.file_name=b.file_name  WHERE B.COL_1 IS NULL and a.file_name='{file_name3}') a 
                      """.format(schema_name1=tabschema_input, table_name1=tabname_input,
                                            primary_key_input1=primary_key_input, val_year1=valyear,
                                            filter_fields_input1=filter_fields_input,
                                            filter_value_input1=filter_value_input,file_name3=file_name_com_2,
                                            filter_min_value_input1=filter_min_value_input,
                                            filter_max_value_input1=filter_max_value_input, pk_join1=pk_join,pk_concat1=pk_concat,cnt1=cnt)
        #print(compare_query_ins1)
        sf_con_prd.cursor().execute(compare_query_ins1)

        compare_query_ins_prem_mis = """insert into CNS1p.LGCY_IRM_DM.TESTING_RESULT_COMPARE
                                    select * from (SELECT 'ALL' col1,'{schema_name1}' col2,'{table_name1}' col3,'{primary_key_input1}' col4,{pk_concat12} col5,'{val_year1}' col6,current_timestamp::timestamp_ntz col7,a.col_{cnt1} src_col,
                                    'EDW_MISSING' tgt_col ,'EDW_MISSING' col9,'COMPARE' col10,
                                    '{filter_fields_input1}' col11,'{filter_value_input1}' col12,'{filter_min_value_input1}' col13,'{filter_max_value_input1}' col14 FROM
                                       CNS1p.LGCY_IRM_DM.TESTING_DATA_target b LEFT JOIN CNS1p.LGCY_IRM_DM.TESTING_DATA_SOURCE a on 
                                    a.TABLE_NAME=b.TABLE_NAME and a.SCHEMA_NAME=b.SCHEMA_NAME   AND {pk_join1} 
                                    and a.file_name=b.file_name  WHERE  a.COL_1 IS NULL and b.file_name='{file_name3}' ) a 
                                    """.format(schema_name1=tabschema_input, table_name1=tabname_input,
                                               primary_key_input1=primary_key_input, val_year1=valyear,
                                               filter_fields_input1=filter_fields_input,
                                               filter_value_input1=filter_value_input,
                                               filter_min_value_input1=filter_min_value_input,
                                               filter_max_value_input1=filter_max_value_input, pk_join1=pk_join,
                                               pk_concat12=pk_concat2, cnt1=cnt,file_name3=file_name_com_2)
        #print(compare_query_ins_prem_mis)
        sf_con_prd.cursor().execute(compare_query_ins_prem_mis)
        while (cnt <= y):
       # print(y)
           vf_name = col_list_dict[cnt - 1]
           compare_query_ins2 = """insert into CNS1p.LGCY_IRM_DM.TESTING_RESULT_COMPARE
                  select * from (SELECT 'ALL' col1,'{schema_name1}' col2,'{table_name1}' col3,'{primary_key_input1}' col4,{pk_concat1} col5,'{val_year1}' col6,current_timestamp::timestamp_ntz col7,a.col_{cnt1} src_col,
                  b.col_{cnt1} tgt_col ,'{vf_name1}' col9,'COMPARE' col10,
                  '{filter_fields_input1}' col11,'{filter_value_input1}' col12,'{filter_min_value_input1}' col13,'{filter_max_value_input1}' col14 FROM
                  CNS1p.LGCY_IRM_DM.TESTING_DATA_SOURCE a  join CNS1p.LGCY_IRM_DM.TESTING_DATA_target b on 
                  a.TABLE_NAME=b.TABLE_NAME and a.SCHEMA_NAME=b.SCHEMA_NAME and a.file_name=b.file_name and a.file_name='{file_name3}'   AND {pk_join1} ) a where 
                  coalesce(src_col,'a')<>coalesce(tgt_col,'a')
                  """.format(schema_name1=tabschema_input, table_name1=tabname_input,
                                            primary_key_input1=primary_key_input, val_year1=valyear,
                                            filter_fields_input1=filter_fields_input,
                                            filter_value_input1=filter_value_input,
                                            filter_min_value_input1=filter_min_value_input,
                                            filter_max_value_input1=filter_max_value_input, pk_join1=pk_join, cnt1=cnt,
                                            vf_name1=vf_name,pk_concat1=pk_concat,
                                            file_name3=file_name_com_2)
           delete_query_result_1 = """
                                delete from CNS1p.LGCY_IRM_DM.TESTING_RESULT_COMPARE where  SCHEMA_NAME='{schema_name1}' and 
                                TABLE_NAME='{table_name1}' and VALIDATION_FIELD_NAME='{vf_name1}' and FILTER_MIN_VALUE='{filter_min_value_input1}' and FILTER_MAX_VALUE='{filter_max_value_input1}'
                                """.format(schema_name1=tabschema_input,
                                                                                       table_name1=tabname_input,
                                                                                       filter_min_value_input1=filter_min_value_input,
                                                                                       filter_max_value_input1=filter_max_value_input,
                                                                                       vf_name1=vf_name)

           #print(compare_query_ins2)
           #print(delete_query_result_1)
           cnt = cnt + 1
           sf_con_prd.cursor().execute(delete_query_result_1)
           sf_con_prd.cursor().execute(compare_query_ins2)

    #print(prem_del_query1)
   # print(cloud_ins_query1)
hist_ins_query1 = """
                    insert into CNS1p.LGCY_IRM_DM.TESTING_RESULT_COMPARE_history select * from CNS1p.LGCY_IRM_DM.TESTING_RESULT_COMPARE
                    where (SCHEMA_NAME,TABLE_NAME,VALIDATION_FIELD_NAME,VALIDATION_TS) not in 
                    (select SCHEMA_NAME,TABLE_NAME,VALIDATION_FIELD_NAME,VALIDATION_TS from CNS1p.LGCY_IRM_DM.TESTING_RESULT_COMPARE_history  )
                """
#sf_con_prd.cursor().execute(hist_ins_query1)
#sf_con_prd.close()
sf_con_prd.close()
ibm_db.close(connection_edw)