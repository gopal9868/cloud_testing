import oct_config
import oct_output
put_query1 = """
                    put file://{file_name1}  @S_EDW_INT_TESTING
                  """.format(file_name1=oct_config.output_path + oct_output.cloud_cnt_result)
put_query2 = """
                     put file://{file_name2}  @S_EDW_INT_TESTING
                    """.format(file_name2=oct_config.output_path + oct_output.onprem_cnt_result)
remove_query1 = """
                     remove @S_EDW_INT_TESTING/{file_name3}
                     """.format(file_name3=oct_output.onprem_cnt_result)
remove_query2 = """
                     remove @S_EDW_INT_TESTING/{file_name4}
                   """.format(file_name4=oct_output.cloud_cnt_result)
delete_query1 = """
                    DELETE FROM  cns1p.LGCY_IRM_DM.TESTING_RESULT WHERE (SCHEMA_NAME,TABLE_NAME,VALIDATION_YEAR,VALIDATION_TYPE,VALIDATION_FIELD_NAME,
                    FILTER_MIN_VALUE,FILTER_MAX_VALUE,FILTER_VALUE,FILTER_FIELDS) IN 
                   (SELECT $3,$4,$6,$8,$9,$10,$11,$12,$13 FROM @stg1p.LGCY_AGT_STG.S_EDW_INT_TESTING/{file_name_del}  )
                   """.format(file_name_del=oct_output.cloud_cnt_result)
ins_query1 = """
                   INSERT  INTO cns1p.LGCY_IRM_DM.TESTING_RESULT SELECT A.$1,A.$3,B.$4,A.$6,A.$7,B.$5,A.$5,A.$9,A.$8,A.$13,A.$12,A.$10,A.$11 FROM
                   @stg1p.LGCY_AGT_STG.S_EDW_INT_TESTING/{file_name1} A
                   FULL OUTER JOIN  @stg1p.LGCY_AGT_STG.S_EDW_INT_TESTING/{file_name2} B ON A.$3=B.$3 AND A.$4=B.$4 AND A.$6=B.$6 AND A.$7=B.$7 
                    AND A.$9=B.$9 AND A.$10=B.$10 AND A.$8=B.$8 AND A.$11=B.$11 AND A.$12=B.$12 AND A.$13=B.$13
                    """.format(file_name1=oct_output.cloud_cnt_result, file_name2=oct_output.onprem_cnt_result)
# print(ins_query1)
ins_query2 = """
                   INSERT  INTO cns1p.LGCY_IRM_DM.TESTING_RESULT_HISTORY SELECT A.$1,A.$3,B.$4,A.$6,A.$7,B.$5,A.$5,A.$9,A.$8,A.$13,A.$12,A.$10,A.$11  FROM
                   @stg1p.LGCY_AGT_STG.S_EDW_INT_TESTING/{file_name1} A
                   FULL OUTER JOIN  @stg1p.LGCY_AGT_STG.S_EDW_INT_TESTING/{file_name2} B ON A.$3=B.$3 AND A.$4=B.$4 AND A.$6=B.$6 AND A.$7=B.$7 
                   AND A.$9=B.$9 AND A.$10=B.$10 AND A.$8=B.$8 AND A.$11=B.$11 AND A.$12=B.$12  AND A.$13=B.$13
                  """.format(file_name1=oct_output.cloud_cnt_result, file_name2=oct_output.onprem_cnt_result)
