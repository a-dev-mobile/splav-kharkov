# %%


# %%

import pandas as pd
import numpy as np

from Constant import *
from UtilsPandas import *

pd.options.display.max_columns = None
pd.options.display.max_rows = None

pd.set_option('display.width', 1000)
# %%

if Constant.isWork:
    path_info = 'D:/trofimov/DEV/PYTHON/PYTHON_CONTENT/splav-kharkov.com/content/info/article_all_1.csv'
else:
    path_info = 't:/DEV/Python/PYTHON_CONTENT/splav-kharkov.com/content/info/article_all_1.csv'

df_info = pd.read_csv(path_info, sep=';')
df_info.head(10)

# %%
del_column(df_info, 'Unnamed: 10')
# del_column(df_info, 'type')
del_column(df_info, 'Марка :')
del_column(df_info, 'Классификация :')
# %%
df_info.head(10)
# %%
df_info = change_column_order(df_info, 'id', 0)
df_info = change_column_order(df_info, 'category', 1)
df_info = change_column_order(df_info, 'type', 2)

df_info = change_column_order(df_info, 'name', 4)
df_info = change_column_order(df_info, 'Дополнение:', 5)
df_info = change_column_order(df_info, 'Применение:', 6)
df_info = change_column_order(df_info, 'Заменитель:', 7)

df_info.head()

# %%
df_info.nunique()
# %%
df_main_info = df_info.copy()

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
    "type": "type_",
    "category": "category_",
    "Дополнение:": "add_",
    "Применение:": "use_",
    "Заменитель:": "replace_",
    "Зарубежные аналоги:": "analog_",

})

# %%
name_col = 'id_'
df_main_info[name_col] = df_main_info[name_col].astype(int)

# %%
df_main_info.info()
# %%

df_main_info.dtypes
# %%


import sub_chem_for_sql

sub_chem_for_sql.df_chem.head()
# %%
df_main_info.head()
# %%
df_main = df_main_info.join(sub_chem_for_sql.df_chem[sub_chem_for_sql.addingNameCol])
# %%
df_main.info()
del_column(df_main, 'analog_')
# %%

# %%
df_type = df_main[['category_', 'type_']].copy()
df_type = df_type.drop_duplicates()
df_type.head()
# %%
df_category = df_main[['category_']].copy()
df_category = df_category.drop_duplicates()
df_category.head()
# %%
df_type.head(2000)

# %%
# %%
df_main.head()
# %%
df_main['use_'][94]
# %%
df_main['use_'][7]

# %%
from fuzzywuzzy import process, fuzz

str2Match = df_main['use_'][101]
strOptions = df_main['use_']
Ratios = process.extract(str2Match,strOptions)
print(Ratios)
# You can also select the string with the highest matching percentage
highest = process.extractOne(str2Match,strOptions)
print(highest)



# %%
from fuzzywuzzy import process, fuzz
Str1 = df_main['use_'][94]
Str2 = df_main['use_'][7]
Ratio = fuzz.ratio(Str1.lower(),Str2.lower())
Partial_Ratio = fuzz.partial_ratio(Str1.lower(),Str2.lower())
Token_Sort_Ratio = fuzz.token_sort_ratio(Str1,Str2)
Token_Set_Ratio = fuzz.token_set_ratio(Str1,Str2)
print(Ratio)
print(Partial_Ratio)
print(Token_Sort_Ratio)
print(Token_Set_Ratio)
# %%


if Constant.isWork:
    path_main_json_file = 'd:\\trofimov\\DEV\PYTHON\\PYTHON_CONTENT\\splav-kharkov.com\\ProjectPython\\database\\materials.json'
    path_type_json_file = 'd:\\trofimov\\DEV\PYTHON\\PYTHON_CONTENT\\splav-kharkov.com\\ProjectPython\\database\\class.json'
else:
    path_main_json_file = 't:\\DEV\Python\\PYTHON_CONTENT\\splav-kharkov.com\\ProjectPython\\database\\materials.json'
    path_type_json_file = 't:\\DEV\Python\\PYTHON_CONTENT\\splav-kharkov.com\\ProjectPython\\database\\type.json'
    path_category_json_file = 't:\\DEV\Python\\PYTHON_CONTENT\\splav-kharkov.com\\ProjectPython\\database\\category.json'

df_main.to_json(path_main_json_file, force_ascii=False, orient='records')
df_type.to_json(path_type_json_file, force_ascii=False, orient='records')
df_category.to_json(path_category_json_file, force_ascii=False, orient='records')



# %%

# with open(path_json_file, 'w', encoding='utf-8') as file:
# df_main.apply(lambda x: [x.dropna()], axis=1).to_json(file, force_ascii=False, orient='index')
# df_main.apply(lambda x: [x.dropna()], axis=1).to_json(file, force_ascii=False, orient='index')


# %%
from UtilsPandas import *

# path_sql_file = 'c:\\trofimov\\CLOUD\\c.dev.mobile_GoogleDisk\\DEV\\materials3.db'
# save_sql(df_main, "info_chem", path_sql_file)
# %%
# df_main_info.to_csv(r'c:\\trofimov\\CLOUD\\c.dev.mobile_GoogleDisk\\DEV\\DATA_BASE\\materials.csv', index=False,
#                     header=True)
