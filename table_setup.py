import collections
import uuid
import gpudb

db_conn_str="http://172.30.50.78:9191"
db_user = "no_cred"
db_pass = "no_cred"

TBL_NAME_toll_stream = "tolling_and_weather"

TBL_SCHEMA_toll_stream = [
    ["startzoneid", "int", "int16"],
    ["endzoneid", "int", "int16"],
    ["direction", "string", "char8"],

    ["toll_prev_1", "float"],
    ["toll_prev_2", "float"],

    ["humidity", "float"],
    ["temperature", "float"],
    ["windspeed", "float"],
    ["weather", "string"]
    ]

def test_insert():

    test_insert_a = [
        (3100, 3100, "E", 0.00, 1.87, 46, 33.9, 1.34, "medium"),
        (3100, 3100, "E", 1.42, 1.87, 46, 33.9, 1.34, "heavy")]
    test_insert_b = [
        (3200, 3210, "E", 0.00, 1.87, 146, 33.9, 1.34, "medium"),
        (3200, 3330, "E", 1.42, 1.87, 146, 33.9, 1.34, "heavy")]

    try:
        if db_user == 'no_cred' or db_pass == 'no_cred':
            db=gpudb.GPUdb(encoding='BINARY',
                           host=db_conn_str)
        else:
            db=gpudb.GPUdb(encoding='BINARY',
                           host=db_conn_str,
                           username=db_user,
                           password=db_pass)

        insertables = []
        for (sz, ez, direction, tp1, tp2, humd, tempr, wnd, wthr) in test_insert_a:
            insertable = collections.OrderedDict()
            insertable["startzoneid"] = sz
            insertable["endzoneid"] = ez
            insertable["direction"] = direction
            insertable["toll_prev_1"] = tp1
            insertable["toll_prev_2"] = tp2
            insertable["humidity"] = humd
            insertable["temperature"] = tempr
            insertable["windspeed"] = wnd
            insertable["weather"] = wthr
            insertables.append(insertable)

        sink_table = gpudb.GPUdbTable( name = TBL_NAME_toll_stream, db = db)
        insert_status=sink_table.insert_records(insertables)

        print(insert_status)
        print('\tSample records inserted ')

    except gpudb.gpudb.GPUdbException as e:
        
        print(e)


def main():
    try:
        if db_user == 'no_cred' or db_pass == 'no_cred':
            db=gpudb.GPUdb(encoding='BINARY',
                           host=db_conn_str)
        else:
            db=gpudb.GPUdb(encoding='BINARY',
                           host=db_conn_str,
                           username=db_user,
                           password=db_pass)

        if db.has_table(table_name=TBL_NAME_toll_stream)['table_exists']:
            print('Table exists {0:s}'.format(TBL_NAME_toll_stream))

        else:
            status = gpudb.GPUdbTable(_type=TBL_SCHEMA_toll_stream,
                                      name=TBL_NAME_toll_stream,
                                      db=db,
                                      options={'collection_name': 'traffic'})
            print(status)
            print('\tTable created {0:s}'.format('Table created'))          

        test_insert()

    except gpudb.gpudb.GPUdbException as e:
        print(e)

if __name__ == "__main__":
    # execute only if run as a script
    main()    