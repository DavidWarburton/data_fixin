def encode_column(cursor, in_table, in_column, out_table=None, out_column=None, code_table_name=None):
    if not out_column:
        out_column = "{in_column}_encoded".format(in_column=in_column)
    if not out_table:
        out_table = "{in_table}_layer_3".format(in_table=in_table)
    if not code_table_name:
        code_table_name = "decode"

    print("Create table")
    cursor.execute("CREATE TABLE IF NOT EXISTS {out_table} AS TABLE {in_table}".format(
        in_table=in_table,
        out_table=out_table,
    ))
    print("Add out column")
    cursor.execute("ALTER TABLE {out_table} ADD COLUMN IF NOT EXISTS {out_column} TEXT".format(
        out_table=out_table,
        out_column=out_column,
    ))

    print("get distinct")
    cursor.execute("SELECT {in_column} FROM {in_table} group by {in_column}".format(
        in_table=in_table,
        in_column=in_column,
    ))

    print("build out_column")
    code_table_data = []
    for i, value in enumerate(cursor.fetchall()):
        code_table_data.append((out_table, out_column, str(i), value[0]))
        cursor.execute("UPDATE {out_table} SET {out_column} = '{code}' WHERE {in_column} = '{value}'".format(
            out_table=out_table,
            out_column=out_column,
            in_column=in_column,
            value=value[0],
            code=str(i)
        ))
    print("Drop in column")
    cursor.execute("ALTER TABLE {out_table} DROP COLUMN {in_column}".format(out_table=out_table, in_column=in_column))

    print("update encode table")
    print(cursor.mogrify("%s", (x, )) for x in code_table_data)
    code_table_data_str = ','.join()
    cursor.execute("INSERT INTO {code_table_name} (table_name, column_name, code, value) VALUES {code_table_data}".format(
        code_table_name=code_table_name,
        code_table_data=code_table_data_str,
    ))

execute_sql(statement):
	conn = psycopg2.connect(database='db-19-g01', host='gaboury.popdata.bc.ca')
	cur = conn.cursor()
	
	cur.execute(statement)
	
	conn.close()
