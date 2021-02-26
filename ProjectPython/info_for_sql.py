# %%
import pandas as pd
import numpy as np

import chem_for_sql
from UtilsPandas import *

pd.options.display.max_columns = None
pd.options.display.max_rows = None

pd.set_option('display.width', 1000)
# %%
# path_info = 't:/DEV/Python/PYTHON_CONTENT/splav-kharkov.com/content/info/article_all_1.csv'
path_info = 'D:/trofimov/DEV/PYTHON/PYTHON_CONTENT/splav-kharkov.com/content/info/article_all_1.csv'

df_info = pd.read_csv(path_info, sep=';')
df_info.head(10)
# %%
# %%
del_column(df_info, 'Unnamed: 9')
del_column(df_info, 'type')
del_column(df_info, 'Марка :')
# %%
df_info.head(10)
# %%
df_info = change_column_order(df_info, 'id', 0)
df_info = change_column_order(df_info, 'Классификация :', 1)
df_info = change_column_order(df_info, 'name', 2)
df_info = change_column_order(df_info, 'Дополнение:', 3)
df_info = change_column_order(df_info, 'Применение:', 4)
df_info = change_column_order(df_info, 'Заменитель:', 5)

df_info.head()

# %%

# %%
df_info.nunique()
# %%
df_main_info = df_info.copy()

# %%
# %%
# %%
count_uniq = df_main_info.nunique().tolist()
name_column = df_main_info.columns.tolist()
dic_name_value = {}
for i in range(len(name_column)):
    print('{} = {}'.format(name_column[i], count_uniq[i]))
    dic_name_value[name_column[i]] = count_uniq[i]
for i in range(len(name_column)):
    for j in range(50):
        replace_value(df_main_info, name_column[i], '  ', ' ')
        replace_value(df_main_info, name_column[i], '&ordm;', 'º')
# %%
replace_value(df_main_info, 'Зарубежные аналоги:', 'Нет данных', '0')
replace_value(df_main_info, 'Зарубежные аналоги:', 'Известны', '1')
replace_value(df_main_info, 'Заменитель:', 'NaN', '0')
replace_value(df_main_info, 'id', 'http', '')
replace_value(df_main_info, 'id', '://splav-kharkov.com', '')
replace_value(df_main_info, 'id', '/mat_start.php?', '')
replace_value(df_main_info, 'id', 'name_id=', '')
replace_value(df_main_info, 'id', '?', '')
# %%
df_main_info["id"]
# %%
df_main_info.head()
# %%
df_main_info['Заменитель:'].unique()
# %%
df_main_info = df_main_info.rename(columns={
    "id": "id_",
    "Классификация :": "class_",
    "name": "name_",
    "Дополнение:": "add_",
    "Применение:": "use_",
    "Заменитель:": "replace_",
    "Зарубежные аналоги:": "analog_",

})
# %%

# %%
name_col = 'id_'
df_main_info[name_col] = df_main_info[name_col].astype(int)

# %%
df_main_info.info()
# %%

df_main_info.dtypes
# %%
# %%
# %%

import chem_for_sql
chem_for_sql.df_chem.head()
# %%
df_main_info.head()
# %%
df_main = df_main_info.join(chem_for_sql.df_chem[chem_for_sql.addingNameCol])
# %%
df_main.info()
del_column(df_main, 'analog_')
# %%
df_main.head()
# %%
df_class = df_main['class_'].drop_duplicates()
# %%
df_class.columns = ["class_"]
# %%
df_class.head()
# %%

df_main.reset_index()
# %%
path_main_json_file = 'd:\\trofimov\\DEV\PYTHON\\PYTHON_CONTENT\\splav-kharkov.com\\ProjectPython\\database\\materials.json'
path_sub_json_file = 'd:\\trofimov\\DEV\PYTHON\\PYTHON_CONTENT\\splav-kharkov.com\\ProjectPython\\database\\class.json'

df_main['class_'].drop_duplicates().to_json(path_sub_json_file, force_ascii=False, orient='index')

df_main.to_json(path_main_json_file, force_ascii=False, orient='records')
# %%

# with open(path_json_file, 'w', encoding='utf-8') as file:
    # df_main.apply(lambda x: [x.dropna()], axis=1).to_json(file, force_ascii=False, orient='index')
    # df_main.apply(lambda x: [x.dropna()], axis=1).to_json(file, force_ascii=False, orient='index')



# %%
from UtilsPandas import *
path_sql_file = 'c:\\trofimov\\CLOUD\\c.dev.mobile_GoogleDisk\\DEV\\materials3.db'
# save_sql(df_main, "info_chem", path_sql_file)
# %%
# df_main_info.to_csv(r'c:\\trofimov\\CLOUD\\c.dev.mobile_GoogleDisk\\DEV\\DATA_BASE\\materials.csv', index=False,
#                     header=True)
