
def metadata_count_sql(a,b):
     metadata_count_sql = """
         select  distinct ONPREM_DB_NAME,onprem_schema_name,onprem_table_name,CLOUD_DB_NAME,CLOUD_SCHEMA_NAME,CLOUD_TABLE_NAME,appname from CNS1p.LGCY_IRM_DM.TESTING_METADATA 
         where CLOUD_SCHEMA_NAME={tabschema1} and CLOUD_TABLE_NAME={tabname1} 
          """.format(tabschema1=a, tabname1=b)
     return(metadata_count_sql)