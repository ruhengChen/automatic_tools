#-*- coding=utf-8 -*-

DSN = 'YATOPDB'
DSNUSER = 'appinst'
DSNPWD = 'appinst'
ZONE = '金华'
TABSPACE = 'TS_APP'
IP = '158.125.31.147'
dwmmdb = 'YATOPDB'
dwmmuser = 'appinst'
dwmmpwd = 'appinst'
edwdb = 'YATOPDB'
edwuser = 'appinst'
edwpwd = 'appinst'



# path
job_schedule_path = '/etl/etldata/script/yatop_update/{date}/job_schedule.SQL'
delta_tables_path = '/etl/etldata/script/yatop_update/{date}/delta_tables.ddl'
alter_table_path = '/etl/etldata/script/yatop_update/{date}/alter_table.sql'
his_tables_path = '/etl/etldata/script/yatop_update/{date}/his_tables.ddl'
ods_tables_path = '/etl/etldata/script/yatop_update/{date}/ods_tables.ddl'
compare_generate_path = '/etl/etldata/script/yatop_update/{date}/compare_generate.log'
read_me_path = '/etl/etldata/script/yatop_update/{date}/README.txt'

apsql_path = '/etl/etldata/script/yatop_update/{date}/AP/{APNAME}'
apsql_ods_path = '/etl/etldata/script/odssql/{APNAME}'

# backup_path

backup_path = '/etl/etldata/script/yatop_update/{date}/backup/'
job_metadata_path = '/etl/etldata/script/yatop_update/{date}/backup/JOB_METADATA.del'
job_seq_path = '/etl/etldata/script/yatop_update/{date}/backup/JOB_SEQ.del'

