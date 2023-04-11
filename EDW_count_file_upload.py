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
#sf_con_qa = db_connection.sf_con_qa
sf_con_prd = db_connection.sf_con_prd
connection_edw =db_connection.connection_edw
sf_con_prd.cursor().execute("USE warehouse lgcy_lg")
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