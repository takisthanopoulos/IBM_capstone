import ibm_db
from numpy import int64
import pandas as pd
import ibm_db_dbi
import time
from prettytable import PrettyTable
import prettytable


def pretty_df(df):    
    table = PrettyTable([''] + list(df.columns))
    for row in df.itertuples():
        table.add_row(row)
        str(table)
    print(table)
    return 

def get_db_connection(dsn_driver, dsn_database, dsn_hostname, dsn_port, dsn_protocol, dsn_uid, dsn_pwd):
    

    dsn = "ATTACH=FALSE;"
    dsn += ("DRIVER={0};"
            "DATABASE={1};"
            "HOSTNAME={2};"
            "PORT={3};"
            "PROTOCOL={4};"
            "UID={5};"
            "PWD={6};").format(dsn_driver, dsn_database, dsn_hostname, dsn_port, dsn_protocol, dsn_uid, dsn_pwd)

    try:
        conn = ibm_db.connect(dsn, "", "")
        print("Connected to database: ", dsn_database, "as user: ", dsn_uid, "on host: ", dsn_hostname)

    except:
        print("Unable to connect: ", ibm_db.conn_errormsg())

    return (conn)




def ins_df(df,conn,db2_table):
    sql_cnt = 0
    start = time.time()


    def prep(column):
        return ('"' + str(column) + '"')

    table = '"{}"'.format(db2_table)
    print("Inserting data to table ",table,".")
    sql_columns = tuple(df.columns.values)
    sql_columns = ', '.join(map(prep, sql_columns))
    records = df.to_records(index=False)
    rows = tuple(records)
    values = ((', '.join(map(str, rows))))

    sql = "INSERT INTO {} ({}) VALUES {}".format(table,sql_columns, values)
    stmt = ibm_db.exec_immediate(conn, sql)
    end = time.time()
    timer=round((end - start),3)
    rows_cnt= (ibm_db.num_rows(stmt))
    average_t_per_record=round((timer/rows_cnt),3)
    print (rows_cnt," rows inserted in ",timer," secs. ",average_t_per_record,' seconds per record.')

def table_from_df(df,conn,tablename):
    df_datatypes=pd.DataFrame(df.dtypes,columns=["Column_type"])
    df_datatypes.reset_index(inplace=True)
    df_datatypes.index=df_datatypes.index+1
    df_datatypes.columns=['Columns','Column_type']

    df_datatypes.loc[df_datatypes['Column_type']=='object',['Column_type']]='VARCHAR(100)'
    df_datatypes.loc[df_datatypes['Column_type']=='int64',['Column_type']]='INT'
    df_datatypes.loc[df_datatypes['Column_type']=='float64',['Column_type']]='FLOAT'
    print ("The following columns will be created:\n")
    pretty_df(df_datatypes)
    df_datatypes['stmt']='"'+df_datatypes['Columns']+'"'+' '+df_datatypes['Column_type']
    stmt=tuple(df_datatypes['stmt'])
    stmt_1 = ((', '.join(map(str, stmt))))
    try:
        sql = "CREATE TABLE \"{}\" ({})  ".format(tablename,stmt_1)
        stmt = ibm_db.exec_immediate(conn, sql)
        print ("Operation succesful. Table created!")
    except:
        print ("Operation error...","\n\n",ibm_db.stmt_errormsg(),"\n")




    
