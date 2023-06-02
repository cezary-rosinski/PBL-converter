import pandas as pd
from glob import glob
from tqdm import tqdm

#%% main
path = r"C:\Users\Cezary\Downloads\pbl_retro/"
files = [f for f in glob(path + '*.csv', recursive=True)]

columns_ok = ['AUTOR', 'TYTUŁ', 'WSPÓŁAUTOR', 'RODZAJ_DZIEŁA_ZALEŻNEGO', 'CZASOPISMO', 'NUMER_CZASOPISMA', 'MIEJSCE_WYDANIA', 'ADNOTACJA', 'NUMER_WYDANIA', 'WYDAWNICTWO', 'STRONY', 'DATA_WYDANIA', 'SERIA']

ok_records = 0
records_list = []
# wrong_records = 0
for file in tqdm(files):
    # file = files[0]
    test_df = pd.read_csv(file)
    grouped = test_df.groupby('ID')
    for name, group in grouped:
        if isinstance(group['TYTUŁ'].to_list()[0], str):
            group = group[[e for e in group.columns if e in columns_ok]]
            if max(group.isnull().sum(axis = 1)) < 11:
                ok_index = [i for i, e in enumerate(group['RODZAJ_DZIEŁA_ZALEŻNEGO'].to_list()) if isinstance(e,str)]
                if ok_index:
                    ok_index = min(ok_index)
                    group = group.reset_index(drop=True)
                    group = group.loc[~group.index.isin(range(1,ok_index))]
                else: group = group.head(1)
                records_list.append(group)
                ok_records += group.shape[0]
        # else: wrong_records += group.shape[0]
    
# ok_groups --> 217005
# wrong_groups --> 112407

# ok_records --> 933850 | 400k po filtrowaniu
# wrong_records --> 701033

test = ok_records[2]
test = test[[e for e in test.columns if e in columns_ok]]




#%% notatki

df = pd.read_excel(r"C:\Users\Cezary\Downloads\processed_1986_t1.xlsx")

grouped = df.groupby('ID')

ok_groups = []
wrong_groups = 0
for name, group in grouped:
    # name = 1758
    # name = 19034
    # group = grouped.get_group(name)
    # group['RODZAJ_DZIEŁA_ZALEŻNEGO'] = group['RODZAJ_DZIEŁA_ZALEŻNEGO'].fillna(method='ffill')
    group = ok_groups[7]
    if isinstance(group['TYTUŁ'].to_list()[0], str):
        ok_index = [i for i, e in enumerate(group['RODZAJ_DZIEŁA_ZALEŻNEGO'].to_list()) if isinstance(e,str)]
        if ok_index:
            ok_index = min(ok_index)
            group = group.reset_index(drop=True)
            group = group.loc[~group.index.isin(range(1,ok_index))]
        else: group = group.head(1)
        
        ok_groups.append(group)
    else: wrong_groups += 1
        
ok_groups_1 = [e for e in ok_groups if e.shape[0] == 1]
