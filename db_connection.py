import ibm_db
import oct_login
import oct_config
import snowflake.connector
#sf_qa_user='test'
sf_prd_user='test'
edw_prd_user=oct_config.user_id
#sf_con_qa = snowflake.connector.connect(
#    user=sf_qa_user,
#    password=oct_login.sfq,
#    account='clouddev.us-east-1'
#)
sf_con_prd = snowflake.connector.connect(
    user='test',
    password=oct_login.sfp,
    account='cloudprod.us-east-1'
)
connection_edw = ibm_db.connect('DATABASE=analytics_prod;'
                                'HOSTNAME=analytics_host_prod;'
                                'PORT=4700;'
                                'PROTOCOL=TCPIP;'
                                'UID='+edw_prd_user+';'
                                'PWD=' + oct_login.myp + ';', '', '')
