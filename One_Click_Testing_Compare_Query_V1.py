from datetime import datetime
import ibm_db
import re
import oct_config,oct_output,db_connection
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
with open(output_path + onprem_com_result, 'w') as f2:
    pass
#sf_con_qa = db_connection.sf_con_qa
sf_con_prd = db_connection.sf_con_prd
connection_edw =db_connection.connection_edw
sf_query=''
edw_query=''
primary_key_input=''
with open(input_path + 'SF_Query.txt', 'r') as tabf1:
    for line in tabf1:
        sf_query=sf_query+line
#print(sf_query)
with open(input_path + 'EDW_Query.txt', 'r') as tabf1:
    for line in tabf1:
        edw_query=edw_query+line
#print(edw_query)
with open(input_path + 'Column_List.txt', 'r') as tabf1:
            for line in tabf1:
                tablist = line.split('|')
                primary_key_input=tablist[2].rstrip('\n')
                tabschema_input=tablist[0]
                tabname_input=tablist[1]
                col_list_input=tablist[3].rstrip('\n')
#print(primary_key_input)
pk_col_cnt = len(primary_key_input.split(','))
col_list_dict=col_list_input.split(',')
#print(pk_col_cnt)
pk_col_i = 0
pk_col_l = ''
while (pk_col_i < pk_col_cnt):
    if (pk_col_i == (pk_col_cnt - 1)):
        pk_col_l = pk_col_l + 'col_' + str(pk_col_i + 1)
    else:
        pk_col_l = pk_col_l + 'col_' + str(pk_col_i + 1) + ','
    pk_col_i = pk_col_i + 1

#print(pk_col_l)
full_col=primary_key_input+','+col_list_input
#print(full_col)
full_col_list=full_col.split(',')
len_col_list=len(full_col_list)
y=len(full_col_list)
#print(full_col_list)
stmt = ibm_db.exec_immediate(connection_edw, edw_query)
data = ibm_db.fetch_assoc(stmt)
data1=data
#print(data.keys())
#print(data[data.keys()[0]])
#print(next(iter(data)))
col_list_v=''
inc_1=0
while(inc_1<len_col_list):
     col_list_v=col_list_v+'col_'+str(inc_1+1)+','
     inc_1=inc_1+1
col_list=col_list_v.rstrip(',')
#print(col_list)
while data !=False:


    #print(data[0])
    data_line = ''
    lcnt = len(data)
    lincr = 0
    # print(col_list_dict[0])
    while (lincr < len_col_list):
        data_line = data_line + str(data[full_col_list[lincr]]).lstrip(' ').rstrip(' ') + '|'
        lincr = lincr + 1
    #print(data_line)
    with open(output_path + onprem_com_result, 'a+') as f2:
        f2.write(tabschema_input + '|' + tabname_input + '|' + primary_key_input + '|' + data_line + '\n')
    data =ibm_db.fetch_assoc(stmt)
ibm_db.close(connection_edw)
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
# print(prem_com_put_query1)
prem_del_query1 = """
                    delete from cns1p.LGCY_IRM_DM.TESTING_DATA_SOURCE where file_name='{file_name3}'""".format(file_name3=file_name_com_2)
cloud_del_query1 = """
                    delete from cns1p.LGCY_IRM_DM.TESTING_DATA_TARGET where file_name='{file_name3}'""".format(file_name3=file_name_com_2)
# query_str_cloud ='SELECT '+ primary_key_input+','+select_column_input+",'"+CLOUD_SCHEMA_NAME+"','"+CLOUD_TABLE_NAME+"',"+"'"+file_name_com_2+"','"+primary_key_input+"',"+" current_timestamp::timestamp_ntz FROM " + CLOUD_DB_NAME + '.' + CLOUD_SCHEMA_NAME + '.' + CLOUD_TABLE_NAME + ' where ' + filter_fields_input + '>=' + "'" + filter_min_value_input + "'" + ' and ' + filter_fields_input + '<' + "'" + filter_max_value_input + "'"+' and '+filter_value_input+' order by ' + order_by_input + ' limit ' + str(no_of_rows_input)
primary_key_join = 'a,b'
cloud_ins_query1 = """
                    INSERT INTO cns1p.LGCY_IRM_DM.TESTING_DATA_TARGET (
                   {col_list1} TABLE_NAME ) {query_str_cloud1}""".format(col_list1=col_list_v,query_str_cloud1=sf_query)
cloud_upd_query1 = """
                             update cns1p.LGCY_IRM_DM.TESTING_DATA_TARGET set SCHEMA_NAME='{tabschema_input}',PRIMARY_KEY_COLUMN='{primary_key_input}',PROCESS_TS=current_timestamp::timestamp_ntz,FILE_NAME='{file_name3}'  where TABLE_name='{TABLE_name3}'""".format(TABLE_name3=tabname_input,primary_key_input=primary_key_input,tabschema_input=tabschema_input,file_name3=file_name_com_2)

sf_con_prd.cursor().execute("USE warehouse lgcy_lg")
sf_con_prd.cursor().execute("USE schema stg1p.lgcy_agt_stg")
#print(cloud_upd_query1)
#print(prem_ins_query1)
sf_con_prd.cursor().execute(prem_com_remove_query1)
sf_con_prd.cursor().execute(prem_com_put_query1)
sf_con_prd.cursor().execute(prem_del_query1)
sf_con_prd.cursor().execute(prem_ins_query1)
sf_con_prd.cursor().execute(cloud_del_query1)
sf_con_prd.cursor().execute(cloud_ins_query1)
sf_con_prd.cursor().execute(cloud_upd_query1)
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
missing_del_query1 = """
                         delete from CNS1p.LGCY_IRM_DM.TESTING_RESULT_COMPARE where table_name='{table_name1}'""".format(table_name1=tabname_input)
sf_con_prd.cursor().execute(missing_del_query1)
valyear=0
filter_fields_input=''
filter_value_input_use=''
filter_min_value_input=''
filter_max_value_input=''
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
                                            filter_value_input1=filter_value_input_use,file_name3=file_name_com_2,
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
                                               filter_value_input1=filter_value_input_use,
                                               filter_min_value_input1=filter_min_value_input,
                                               filter_max_value_input1=filter_max_value_input, pk_join1=pk_join,
                                               pk_concat12=pk_concat2, cnt1=cnt,file_name3=file_name_com_2)
#print(compare_query_ins_prem_mis)
sf_con_prd.cursor().execute(compare_query_ins_prem_mis)

while (cnt <= y):
    # print(y)
    vf_name = full_col_list[cnt - 1]
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
                           filter_value_input1=filter_value_input_use,
                           filter_min_value_input1=filter_min_value_input,
                           filter_max_value_input1=filter_max_value_input, pk_join1=pk_join, cnt1=cnt,
                           vf_name1=vf_name, pk_concat1=pk_concat,
                           file_name3=file_name_com_2)

    #print(compare_query_ins2)
    # print(delete_query_result_1)
    cnt = cnt + 1
    sf_con_prd.cursor().execute(compare_query_ins2)
hist_ins_query1 = """
                    insert into CNS1p.LGCY_IRM_DM.TESTING_RESULT_COMPARE_history select * from CNS1p.LGCY_IRM_DM.TESTING_RESULT_COMPARE
                    where (SCHEMA_NAME,TABLE_NAME,VALIDATION_FIELD_NAME,VALIDATION_TS) not in 
                    (select SCHEMA_NAME,TABLE_NAME,VALIDATION_FIELD_NAME,VALIDATION_TS from CNS1p.LGCY_IRM_DM.TESTING_RESULT_COMPARE_history  )
                """
sf_con_prd.cursor().execute(hist_ins_query1)
sf_con_prd.close()
