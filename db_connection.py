import ibm_db
import oct_login
import oct_config
import snowflake.connector
#sf_qa_user='ifsnlgbpmq'
sf_prd_user='ifsnlgbpmp'
edw_prd_user=oct_config.user_id
#sf_con_qa = snowflake.connector.connect(
#    user=sf_qa_user,
#    password=oct_login.sfq,
#    account='cloudtestdev.us-east-1'
#)
sf_con_prd = snowflake.connector.connect(
    user='ifsnlgbpmp',
    password=oct_login.sfp,
    account='cloudtestprod.us-east-1'
)
connection_edw = ibm_db.connect('DATABASE=edw_prod;'
                                'HOSTNAME=edw_ibm_prod;'
                                'PORT=4700;'
                                'PROTOCOL=TCPIP;'
                                'UID='+edw_prd_user+';'
                                'PWD=' + oct_login.myp + ';', '', '')
