#-*- coding=utf-8 -*-

import os
import config
# import pyodbc
import ibm_db
import datetime
import sys
import shutil
import re
import json
import traceback
import subprocess

conn = ibm_db.connect("DATABASE=YATOPDB;HOSTNAME=101.37.36.131;PORT=62000;PROTOCOL=TCPIP;UID=appinst;PWD=appinst;", "", "")

def validate_date(date):
    """判断确认此日期大于job_log的最大批量日期"""
    sql = "SELECT MAX(DATA_PRD)DATA_PRD FROM ETL.JOB_LOG"
    stmt = ibm_db.exec_immediate(conn, sql);
    rows = ibm_db.fetch_tuple(stmt)
    # cursor_dw.execute(sql)
    # rows = cursor_dw.fetchone()
    
    log_date = rows[0]
    
    if str(log_date) == "0000-00-00":
        print("ETL.JOB_LOG is empty, continue")
        log_date = "00000000"
    else:
        log_date = datetime.datetime.strftime(log_date, '%Y%m%d')
    
    print("log_date:%s" %log_date)
    
    if not date > log_date:
        print("\033[1;31;40minput_date is not valid, system exit...\033[0m")
        return -1
        
    return 0

def backup_ap(input_date):
    """备份服务器上已存在的AP,若已备份则不备份"""
    return_dict = {}
    return_dict["returnCode"] = 200
    return_dict["returnMsg"] = ""
    print("backup ap...")
    APlist = os.listdir(config.apsql_path.format(date=input_date, APNAME=""))
    print(APlist)
    
    ##判断/etl/etldata/script/odssql/路径是否存在
    if not os.path.exists(config.apsql_ods_path.format(APNAME="")):
        try:
            os.makedirs(config.apsql_ods_path.format(APNAME=""))
        except Exception:
            return_dict["returnCode"] = 400
            exc_type, exc_value, exc_traceback = sys.exc_info()
            return_dict["returnMsg"] = repr(traceback.format_exception(exc_type, exc_value, exc_traceback))
            return return_dict
    
    for AP in APlist:
        try:
            if not os.path.exists(config.apsql_ods_path.format(APNAME=AP)):   # 若原ods目录下没有对应的AP,复制AP到此目录
                print("load %s" %AP)
          
                shutil.copy(config.apsql_path.format(date=input_date, APNAME=AP), config.apsql_ods_path.format(APNAME=""))
                
            else:   # 若已存在对应的AP,复制AP到backup
                if os.path.exists(config.backup_path.format(date=input_date)+AP): 
                # 判断backup目录下是否已有对应的AP,若有,则不复制
                    print("backup exists %s" %AP)
                else:
                    print("backup %s" %AP)
                    shutil.copy(config.apsql_ods_path.format(APNAME=AP), config.backup_path.format(date=input_date))
                    
                    print("load %s" %AP)
                    shutil.copy(config.apsql_path.format(date=input_date, APNAME=AP), config.apsql_ods_path.format(APNAME=""))
        except FileNotFoundError:
            return_dict["returnCode"] = 400
            exc_type, exc_value, exc_traceback = sys.exc_info()
            return_dict["returnMsg"] = repr(traceback.format_exception(exc_type, exc_value, exc_traceback))
            return return_dict
    return return_dict

    
def get_backup_table(input_date):
    """找出需要备份的所有表"""
    return_dict = {}
    return_dict["returnCode"] = 200
    return_dict["returnMsg"] = ""
    with open(config.alter_table_path.format(date=input_date), 'r') as f:
        data = f.read()
    
    reg = re.compile("alter table (\w+\.\w+)", re.IGNORECASE)
    backup_table_list = re.findall(reg, data)
    
    reg = re.compile("rename table (\w+\.\w+)", re.IGNORECASE)
    backup_table_list.extend(re.findall(reg, data))
    
    with open(config.delta_tables_path.format(date=input_date), 'r') as f:
        data = f.read()
        
    reg = re.compile("DROP TABLE (\w+\.\w+)", re.IGNORECASE)
    backup_table_list.extend(re.findall(reg, data))
    
    print("backup_table_list:", backup_table_list)
    
    try:
        backup_tables(input_date, backup_table_list)
    except FileNotFoundError:
        return_dict["returnCode"] = 400
        exc_type, exc_value, exc_traceback = sys.exc_info()
        return_dict["returnMsg"] = repr(traceback.format_exception(exc_type, exc_value, exc_traceback))
        return return_dict
    return return_dict
    
    
def backup_tables(input_date, backup_table_list):
    """备份需要的表结构:包括"""
    """DELTA中需要 DROP 的表、 ALTER TABLE 中需要 ALTER 和 RENAME 的表,若已备份则不备份"""
    
    for table in backup_table_list:
        print("backup table %s" %table)
        schema, tablename = table.split('.')
        backup_path = config.backup_path.format(date=input_date)+table+".ddl.bak"
        print(backup_path)
        if os.path.exists(backup_path):
            print("backup exists %s" %table)
        else:
            cmd = "db2look -d {edwdb} -i {edwuser} -w {edwpwd} -z {schema} -e -t {tablename} -nofed -o /etl/etldata/script/yatop_update/{date}/backup/{table}.ddl.bak".format(edwdb=config.edwdb, edwuser=config.edwuser, edwpwd=config.edwpwd, schema=schema,tablename=tablename,date=input_date,table=table)
            status, output = subprocess.getstatusoutput(cmd)
            if status:
                print("\033[1;31;40mcreate ddl error %s\033[0m" %table)
                print(output)
                sys.exit(-1)
    
def backup_schedule(input_date):
    """备份调度用的JOB_METADATA,和 JOB_SEQ"""
    
    for table in ["JOB_METADATA", "JOB_SEQ"]:
        if table == "JOB_METADATA":
            path = config.job_metadata_path
        elif table == "JOB_SEQ":
            path = config.job_seq_path
            
        if os.path.exists(path.format(date=input_date)):
            print("backup exists %s" %table)
        else:
            print("export %s..." %table)
            cmd = 'db2 connect to {dwmmdb} user {dwmmuser} using {dwmmpwd} && db2 "export to /etl/etldata/script/yatop_update/{date}/backup/{table}.del of del select * from ETL.{table}"'.format(dwmmdb=config.dwmmdb, dwmmuser=config.dwmmuser, dwmmpwd=config.dwmmpwd, date=input_date, table=table)
            
            print(cmd)
            
            status, output = subprocess.getstatusoutput(cmd)
            
            if status:
                print("\033[1;31;40mexport %s error\033[0m" % table)
                print(output)
                return -1
    return 0
    
def load_schedule(input_date):
    """LOAD JOB_METADATA,和 JOB_SEQ"""
    
    print("load JOB_METADATA...")
    cmd = 'db2 connect to {dwmmdb} user {dwmmuser} using {dwmmpwd} && db2 "load from /etl/etldata/script/yatop_update/{date}/backup/JOB_METADATA.del of del modified by identityoverride replace into ETL.JOB_METADATA"'.format(dwmmdb=config.dwmmdb, dwmmuser=config.dwmmuser, dwmmpwd=config.dwmmpwd, date=input_date)
    print(cmd)
    status, output = subprocess.getstatusoutput(cmd)
    if status:
        print("\033[1;31;40mload JOB_METADATA error, cat JOB_METADATA.error see detail \033[0m")
        with open('JOB_METADATA.error','w') as f:
            f.write(output)
        return -1
    
    print("load JOB_SEQ...")
    cmd = 'db2 connect to {dwmmdb} user {dwmmuser} using {dwmmpwd} && db2 "load from /etl/etldata/script/yatop_update/{date}/backup/JOB_SEQ.del of del replace into ETL.JOB_SEQ"'.format(dwmmdb=config.dwmmdb, dwmmuser=config.dwmmuser, dwmmpwd=config.dwmmpwd, date=input_date)
    print(cmd)
    status, output = subprocess.getstatusoutput(cmd)
    if status:
        print("\033[1;31;40mload JOB_SEQ error, cat JOB_SEQ.error see detail \033[0m")
        with open('JOB_SEQ.error','w') as f:
            f.write(output)
        return -1
    return 0
    
def execute(date, file_name):
    print("execute %s" %file_name)
    cmd = "db2 -tvf /etl/etldata/script/yatop_update/"+date+"/"+file_name
    print(cmd)
    
    status, output = subprocess.getstatusoutput(cmd)
    
    
    if status:
        print("\033[1;31;40m execute %s error, cat %s.error to see detail \033[0m" % (file_name, file_name))
        with open(file_name+'.error', 'w') as f:
            f.write(output)
        return -1
    return 0
        
def main():
    return_dict = {}
    sql = "SELECT DISTINCT CHANGE_DATE FROM DSA.ORGIN_TABLE_DETAIL ORDER BY CHANGE_DATE DESC"
    stmt = ibm_db.exec_immediate(conn, sql);
    rows = ibm_db.fetch_tuple(stmt)
    input_date = rows[0]
        
        
    if str(input_date) == '00000000':
        print("\033[1;31;40mdate is not valid,sys exit...\033[0m")
        return_dict["returnCode"] = 400
        return_dict["returnMsg"] = "no data in database"
        return json.dumps(return_dict)
    
    print("input_date:%s" %input_date)
    
    ret = validate_date(input_date)
    if ret:
        return_dict["returnCode"] = 400
        return_dict["returnMsg"] = "the date is < max(date) in job_log"
        return json.dumps(return_dict)
        
    if not os.path.exists(config.backup_path.format(date=input_date)):
        os.makedirs(config.backup_path.format(date=input_date))
    
    return_dict = backup_ap(input_date)   # 备份AP
    if return_dict.get("returnCode") != 200:
        return json.dumps(return_dict)
    
    return_dict = get_backup_table(input_date)  # 备份表结构
    if return_dict.get("returnCode") != 200:
        return json.dumps(return_dict)
    
    ret = backup_schedule(input_date)   # 备份调度表
    if ret:
        return_dict["returnCode"] = 400
        return_dict["returnMsg"] = "backup_schedule error!"
        return json.dumps(return_dict)
    
    ret = load_schedule(input_date) # 重新LOAD调度表
    if ret:
        return_dict["returnCode"] = 400
        return_dict["returnMsg"] = "load_schedule error!"
        return json.dumps(return_dict)
    
    execute_list = ["delta_tables.ddl", "ods_tables.ddl", "his_tables.ddl", "alter_table.sql", "job_schedule.SQL"] 

    for file in execute_list:
        ret = execute(input_date, file)
        if ret:
            return_dict["returnCode"] = 400
            return_dict["returnMsg"] = "execute {} error!".format(file)
            return json.dumps(return_dict)
            
    return json.dumps({"returnCode":200, "returnMsg":"execute OK,"})
                
if __name__=="__main__":
    resp = main()
    print(resp)
    
    
    
    
    
    
    
    
    
    
        
    
    
    