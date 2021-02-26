# %%==============================
import pandas as pd
import sqlite3 as sq


def change_column_order(df, col_name, index):
    cols = df.columns.tolist()
    cols.remove(col_name)
    cols.insert(index, col_name)
    return df[cols]


def replace_value(df, name_col, str_old, str_new):
    try:
        df[name_col] = df[name_col].str.replace(str_old, str_new)
        print("df {} replace - ok==".format(name_col))
    except AttributeError:
        print("")
        print(AttributeError)
        print("df {} is not string column==".format(name_col))
    return df[name_col]


def info_df(df):
    print('')
    print('')
    print('==========info_df START==================')
    print(df.head())
    print('')
    print('----------------------------')
    print('')
    print(df.info())

    print('==========info_df END==================')
    print('')
    print('')


def add_prefix_number_name_col(df):
    old_name_col = df.columns.tolist()
    name_col_size = len(old_name_col)
    new_name_col = []
    df1 = df.copy()
    for i in range(name_col_size):
        new_name_col.append("{}_{}".format(i, old_name_col[i]))
    for i in range(name_col_size):
        df1.rename({old_name_col[i]: new_name_col[i]}, axis=1, inplace=True)

    print('rename OK')
    return df1


def save_sql(data, table, path_sql_file):
    conn = sq.connect(path_sql_file)
    cur = conn.cursor()
    cur.execute('''DROP TABLE IF EXISTS {}'''.format(table))
    data.to_sql(table, conn, if_exists='replace')  # - writes the pd.df to SQLIte DB
    conn.commit()
    conn.close()


def del_column(df, name_col):
    try:
        df.drop(name_col, inplace=True, axis=1)
    except Exception as e:
        print('==============================')
        print(e.__class__)
        print("Error del column = {}".format(name_col))


def count_uniq(df):
    # %%
    count_uniq = df.nunique().tolist()
    name_column = df.columns.tolist()
    dic_name_value = {}
    for i in range(len(name_column)):
        print('{} = {}'.format(name_column[i], count_uniq[i]))
        dic_name_value[name_column[i]] = count_uniq[i]
