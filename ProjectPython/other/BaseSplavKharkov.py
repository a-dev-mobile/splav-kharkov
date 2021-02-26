# %%
import json

import pandas as pd
import numpy as np

from UtilsPandas import *

pd.options.display.max_columns = None
pd.options.display.max_rows = None

pd.set_option('display.width', 1000)
# %%
path_chem = 'J:/DEV/CONTENT_DOWNLOAD/splav-kharkov.com/content/chem/article_all_1.csv'
path_info = 'J:/DEV/CONTENT_DOWNLOAD/splav-kharkov.com/content/info/article_all_2.csv'

df_info = pd.read_csv(path_info, sep=';')
df_chem = pd.read_csv(path_chem, sep=';')
col_df_info = df_info.columns.tolist()
# %%
info_df(df_info)
# %%
info_df(df_chem)
# %%
df_main = pd.merge(df_info, df_chem.rename(columns={'id': 'id'}), on='id', how='left')

df_main.head()
# %%
info_df(df_main)
# %%
del_column(df_main, 'Unnamed: 76')
del_column(df_main, 'Unnamed: 9')
del_column(df_main, 'Термообр.')
del_column(df_main, 'type_y')
del_column(df_main, 'name_y')
del_column(df_main, 'Зарубежные аналоги:')
del_column(df_main, 'без ограничений.')
del_column(df_main, 'Сортамент')
del_column(df_main, 'Напр.')
del_column(df_main, 'Размер')
del_column(df_main, 'name_x')
del_column(df_main, 'type_x')
#del_column(df_main, 'id')
# %%

# %%
count_uniq = df_main.nunique().tolist()
name_column = df_main.columns.tolist()
dic_name_value = {}
for i in range(len(name_column)):
    print('{} = {}'.format(name_column[i], count_uniq[i]))
    dic_name_value[name_column[i]] = count_uniq[i]
# %%
list_dic_name_value = list(dic_name_value.items())
list_dic_name_value.sort(key=lambda i: i[1])
# %%

print(list_dic_name_value.reverse())
print(list_dic_name_value)
# %%
for i in range(len(list_dic_name_value)):
    df_main = change_column_order(df_main, list_dic_name_value[i][0], i)

# %%
# df_main = change_column_order(df_main, 'id', 0)
df_main = change_column_order(df_main, 'Марка :', 0)
df_main = change_column_order(df_main, 'Классификация :', 1)
df_main = change_column_order(df_main, 'Дополнение:', 2)
df_main = change_column_order(df_main, 'Применение:', 3)
df_main = change_column_order(df_main, 'Заменитель:', 4)
df_main = change_column_order(df_main, 'Примесей', 5)

df_main.head()

# %%
for i in range(len(name_column)):
    for j in range(50):
        replace_value(df_main, name_column[i], '  ', ' ')
        replace_value(df_main, name_column[i], '&ordm;', 'º')
# %%
df_main.head()
# %%
# %%
df_main.nunique()
# %%
# %%
# %%
# %%

# %%
# %%
# %% поиск строки по значению
##print(df_main[df_main['name_x'].str.contains("АМг6")])

# %%

# %%

# %%

# %%

# %%
path_sql_file = 'd:\\a.dev.mobile_GoogleDisk\\NOT_change\\materials.db'
save_sql(df_main, "info", path_sql_file)

# %%
# %%
# %%
# %%
df_main.head()
# %%


df_main.nunique()



# %%
# df_main.to_csv('J:/DEV/CONTENT_DOWNLOAD/splav-kharkov.com/base.csv')
# %%
# df_main.to_excel('J:/DEV/CONTENT_DOWNLOAD/splav-kharkov.com/base.xlsx')

# %% #записать основную базу

path_JSON_file = 'd:\\a.dev.mobile_GoogleDisk\\NOT_change\\Firebase\\materials.json'


with open(path_JSON_file, 'w', encoding='utf-8') as file:
    df_main.apply(lambda x: [x.dropna()], axis=1).to_json(file, force_ascii=False, orient='index')

df_main.head()



# %%
#
# from pandas import DataFrame
#
# path_JSON_file = 'd:\\a.dev.mobile_GoogleDisk\\NOT_change\\Firebase\\materials_sub.json'
# data = {
#     'id': [111, 222, np.nan, 444],
#     'name': ['35БТ', '70ТМ-ВД', '16Х', 'БТЦ-ВД'],
#     'use': ['Для изготовления сверхпроводящих экранов', ' Для изготовления датчиков температуры',
#             'Для магнитопроводов различных систем управлени', ' Для изготовления сердечников магнитных']
# }
#
# df = DataFrame(data, columns=['id', 'name', 'use'])
# df.dropna()
#
# with open(path_JSON_file, 'w', encoding='utf-8') as file:
#     df.to_json(file, force_ascii=False, orient='index')
