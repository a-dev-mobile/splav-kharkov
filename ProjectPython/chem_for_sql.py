# %%
from functools import reduce

import pandas as pd
import numpy as np

from UtilsPandas import *

pd.options.display.max_columns = None
pd.options.display.max_rows = None

pd.set_option('display.width', 10000)
# %%
# path_info = 't:/DEV/Python/PYTHON_CONTENT/splav-kharkov.com/content/chem/article_all_1.csv'
path_info = 'D:/trofimov/DEV/PYTHON/PYTHON_CONTENT/splav-kharkov.com/content/chem/article_all_1.csv'

df_info = pd.read_csv(path_info, sep=';')
df_info.head(10)
# %%
df_info.nunique()
# %%
# %%
del_column(df_info, 'Unnamed: 76')
del_column(df_info, '-')
del_column(df_info, 'F')
del_column(df_info, 'Размер')
del_column(df_info, 'Сортамент')
del_column(df_info, 'без ограничений.')
del_column(df_info, 'Напр.')
del_column(df_info, 'Термообр.')
del_column(df_info, 'type')
del_column(df_info, 'name')
# %%
df_info.head(10)
# %%
df_info = df_info.rename(columns={
    "Примесей": "impurity",
})
# %%
df_info = change_column_order(df_info, 'impurity', 66)

# %%
df_info_2 = df_info.copy()
count_uniq = df_info_2.nunique().tolist()
name_column = df_info_2.columns.tolist()
dic_name_value = {}
for s in range(len(name_column)):
    print('{} = {}'.format(name_column[s], count_uniq[s]))
    dic_name_value[name_column[s]] = count_uniq[s]
for s in range(len(name_column)):
    for j in range(50):
        replace_value(df_info_2, name_column[s], '  ', ' ')

        replace_value(df_info_2, name_column[s], ' - ', '-')
        replace_value(df_info_2, name_column[s], '&ordm;', 'º')

for s in range(len(name_column)):
    for j in range(10):
        replace_value(df_info_2, name_column[s], ' - ', '-')
        replace_value(df_info_2, name_column[s], '- ', '-')
        replace_value(df_info_2, name_column[s], ' -', '-')

replace_value(df_info_2, 'id', 'http', '')
replace_value(df_info_2, 'id', '://splav-kharkov.com', '')
replace_value(df_info_2, 'id', '/mat_start.php?', '')
replace_value(df_info_2, 'id', 'name_id=', '')
replace_value(df_info_2, 'id', '?', '')

# %%
df_info_2.head(1000)

# %%
df_info_3 = df_info_2.copy()
# %%
name_column = df_info_3.columns.tolist()
print(name_column[1:])

# %% добавить префикс исключая добавление к nan
for s in name_column[1:]:
    df_info_3.loc[df_info_3[s].notna(), s] = s + '_' + df_info_3.loc[
        df_info_3[s].notna(), s]

# %%
df_info_3.head()
# %%
df_info_3.nunique()


# %%сдвинуть влево значения если пустые


def squeeze_nan(x):
    original_columns = x.index.tolist()

    squeezed = x.dropna()
    squeezed.index = [original_columns[n] for n in range(squeezed.count())]

    return squeezed.reindex(original_columns, fill_value=np.nan)


df_info_3 = df_info_3.apply(squeeze_nan, axis=1)

# %% обьединяет значения колонок
sep = '|'
addingNameCol = 'chem_'
cols = name_column[1:]

df_info_3[addingNameCol] = df_info_3[cols].apply(lambda x: x.str.cat(sep=sep), axis=1)

# df_info_copy_copy[addingNameCol] = df_info_copy_copy[cols].apply(lambda row: sep.join(row.values.astype(str)), axis=1)
# %%

# replace_value(df_info_3, addingNameCol, np.NaN, '|')
# %%
df_info_3 = change_column_order(df_info_3, addingNameCol, 1)

# %%
df_info_3.head()
# %%
df_chem = df_info_3[['id', addingNameCol]]
# %%
df_chem.head()

# %%
