import pandas as pd
from glob import glob
from tqdm import tqdm

#%% main
path = r"C:\Users\Cezary\Downloads\pbl_retro/"
files = [f for f in glob(path + '*.csv', recursive=True)]

ok_groups = 0
wrong_groups = 0
for file in tqdm(files):
    # file = files[0]
    test_df = pd.read_csv(file)
    grouped = test_df.groupby('ID')
    for name, group in grouped:
        if isinstance(group['TYTUŁ'].to_list()[0], str):
            ok_groups += group.shape[0]
        else: wrong_groups += group.shape[0]
    
# ok_groups --> 217005
# wrong_groups --> 112407

# ok_records --> 933850
# wrong_records --> 701033

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
    if isinstance(group['TYTUŁ'].to_list()[0], str):
        ok_groups.append(group)
    else: wrong_groups += 1
        
ok_groups_1 = [e for e in ok_groups if e.shape[0] == 1]
