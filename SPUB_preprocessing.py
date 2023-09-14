#%% note
# plik, w którym przetwarzamy i ewentualnie wzbogadamy dane wejściowe otrzymane od MG

#%% import
import json
from concurrent.futures import ThreadPoolExecutor
from SPUB_additional_functions import get_wikidata_label, get_wikidata_coordinates, simplify_string, marc_parser_for_field, parse_mrk, parse_java, get_number
from tqdm import tqdm
import regex as re
from collections import ChainMap, Counter
import Levenshtein as lev
import hashlib
import pandas as pd

#%% def

def assign_places_to_publishers(x):
    temp = {}
    places = []
    for i,subf in enumerate(x):
        if list(subf.keys())[0] == '$a':
            places.append(list(subf.values())[0])
        elif list(subf.keys())[0] == '$b':
            temp.setdefault(subf['$b'], list()).extend(places)
            if i != len(x)-1 and list(x[i+1].keys())[0] == '$a':
                places = []
    return temp
#

def preprocess_places(data):
    for place in data:
        place['coordinates'] = place['fromWiki']['coordinates'] if place['fromWiki']['coordinates'] else ''
        del place['alterNames'], place['fromWiki'], place['roles'], place['alterLabelsInBiblioRec']
    wikidata_ids = set([e.get('wiki') for e in data if e.get('wiki')])
    with ThreadPoolExecutor() as executor:
        wikidata_response = list(tqdm(executor.map(lambda p: get_wikidata_label(p, ['pl', 'en']), wikidata_ids)))
    wikidata_labels = dict([(a[1:],c) for a,b,c in wikidata_response])
    wikidata_redirection = dict([(a[1:],b[1:]) for a,b,c in wikidata_response])
    data = [dict(e) for e in set([tuple({k:wikidata_labels.get(e.get('wiki'), v) if k == 'name' else v for k,v in e.items() if k != 'recCount'}.items()) for e in data])]
    data = [{k:wikidata_redirection.get(v, '') if k == 'wiki' else v for k,v in e.items()} for e in data]
    return data

def preprocess_people(data, biblio_data):
    # data = import_persons
    # biblio_data = import_biblio
    
    literature_nationalities = pd.read_excel('./additional_files/literature_nationalities.xlsx')
    literature_nationalities_dct = {}
    for idx, row in literature_nationalities.iterrows():
        for key in [row['dane oryginalne'], row['narodowosc'], row['PBL']]:
            if not isinstance(row['MD5 haseł osobowych'], float):
                literature_nationalities_dct[key.lower()] = row['MD5 haseł osobowych']
    
    persons_literatures_dct = {}
    for record in biblio_data:
        genre_major = record.get('genre_major', [])
        if len(genre_major) == 1 and 'Literature' in genre_major:
            for person in record.get('persons_with_roles', []):
                person_name = person.split('|')[0]
                person_role = person.split('|')[-1]
                if person_role == 'author:aut':
                    subjects = [literature_nationalities_dct.get(e.lower()) for e in record.get('subjects_str_mv', []) if e.lower() in literature_nationalities_dct]
                    persons_literatures_dct.setdefault(person_name, []).extend(subjects)          
    persons_literatures_dct = {k:Counter(v).most_common(1)[0][0] for k,v in persons_literatures_dct.items() if v}         
                 
    [e.update({'dateB': e.get('fromWiki', {}).get('dateB')}) for e in data]
    [e.update({'dateD': e.get('fromWiki', {}).get('dateD')}) for e in data]
    data = [{k:e.get('dateB') if k == 'yearBorn' and isinstance(e.get('dateB'), str) else v for k,v in e.items()} for e in data]
    data = [{k:e.get('dateD') if k == 'yearDeath' and isinstance(e.get('dateD'), str) else v for k,v in e.items()} for e in data]
    [e.update({'placeB': e.get('fromWiki', {}).get('placeB')}) for e in data]
    [e.update({'placeD': e.get('fromWiki', {}).get('placeD')}) for e in data]
    
    output = [{k:v for k,v in e.items() if k not in ['dateB', 'dateD', 'fromWiki', 'recCount']} for e in data]
    for elem in output:
        elem['person_heading'] = persons_literatures_dct.get(elem['name'])
        elem['id_'] = elem.get('wiki', '')
    
    return output

def preprocess_institutions(data, biblio_data):
    data = [{k:v for k,v in e.items() if k != 'recCount'} for e in data]
    #warunek 'fullrecord' in e do usunięcia, jeśli MG uwzględni to w eksporcie danych w Libri
    origin_data = [e for e in biblio_data if 'Book' in e.get('format_major') and 'fullrecord' in e and any(el in e.get('fullrecord') for el in ['264', '260'])]
        
    publishers = [[ele for sub in [el.get('264') for el in parse_mrk(e.get('fullrecord'))] for ele in sub][0] if [el.get('264') for el in parse_mrk(e.get('fullrecord'))][0] else [el.get('260')[0] for el in parse_mrk(e.get('fullrecord'))][0] for e in origin_data]

    # publishers = [[el for el in marc_parser_for_field(e, '\\$') if any(x in el for x in ['$a', '$b'])] for e in publishers]
    # set([''.join([ele for sub in [list(el.keys()) for el in e] for ele in sub]) for e in publishers])
    
    #warunek if '$b' in e do usunięcia, gdy dane Libri zostaną poprawione
    publishers = set([[el.get('$b') for el in marc_parser_for_field(e, '\\$') if '$b' in el][0] for e in publishers if '$b' in e])
    publishers = set([e[:-1] if e[-1] == ',' else e[:-2] if e[-2:] == ' :' else e[:-2] if e[-2:] == ' ;' else e[:-4] if re.findall(r'; \\1$', e) else e for e in publishers])
    
    data_names = set([e.get('name') for e in data])

    for publisher in publishers:
        if publisher not in data_names:
            data.append({'name': publisher, 'viaf': '', 'wiki': ''})
    return data

def preprocess_events(data):
    data = [{k:v for k,v in e.items() if k != 'recCount'} for e in data]
    event_dict = {
        'Konkursy': 'competition',
        'Nagrody polskie': 'prize',
        'Nagrody zagraniczne': 'prize',
        'Odznaczenia': 'decoration',
        'Plebiscyty': 'plebiscite',
        'Wystawy': 'exhibition',
        'Zjazdy, festiwale, sesje w Polsce': 'festival',
        'Zjazdy, festiwale, sesje za granicą': 'festival'
    }
    [e.update({'type': [el for el in event_dict if el in e.get('name')][0] if [el for el in event_dict if el in e.get('name')] else ''}) for e in data]
    data = [{k:v.replace(e.get('type')+', ','') if k=='name' and e.get('type') else v for k,v in e.items()} for e in data]
    data = [{k:event_dict.get(v) if k == 'type' and v else v for k,v in e.items()} for e in data]
    event_dict2 = {
        'doktorat honoris causa': 'honorary-doctorate',
        'festiwal': 'festival',
        'konferencja': 'conference',
        'konkurs': 'competition',
        'nagroda': 'prize',
        'odznaczenie': 'decoration',
        'plebiscyt': 'plebiscite',
        'spotkanie autorskie': 'authors-meeting',
        'wystawa': 'exhibition'
    }
    data = [{k:event_dict2.get([el for el in event_dict2 if el in e.get('name').lower()][0]) if k=='type' and [el for el in event_dict2 if el in e.get('name').lower()] else v for k,v in e.items()} for e in data]
    data = [{'type_' if k=='type' else k:v for k,v in e.items()} for e in data]
    return data

def preprocess_publishing_series(data):
    #warunek 'fullrecord' in e do usunięcia, jeśli MG uwzględni to w eksporcie danych w Libri
    data = [e for e in data if 'fullrecord' in e and '=490' in e.get('fullrecord')]
    data = [e.get('series') for e in data]
    data = set([ele for sub in [[' ; '.join([el.strip() for el in e[0].split(' ; ')][1:])] if len(e) == 1 and re.findall('\d+ \;', e[0]) else e for e in data] for ele in sub])
    data = set([[el.strip() for el in e.split(';')][0] for e in data])
    data = [{'title': e} for e in data]
    return data

def preprocess_creative_works(data):
    return [{'name': e.get('author')[0].split('|')[0], 'wiki': e.get('author')[0].split('|')[4], 'title': e.get('title').strip()} for e in data if 'Literature' in e.get('genre_major') and 'author' in e]

def preprocess_journals(biblio_data):

    # with open(r"F:\Cezary\Documents\IBL\Libri\dane z libri do pbl\2023-02-15\magazines.json", encoding='utf-8') as f:
    #     data2 = json.load(f)
        
    # data2 = [{k:v for k,v in e.items() if k != 'recCount'} for e in data2]
    # mg_titles = [e.get('name') for e in data2]
    
    biblio_data = [{k:v for k,v in e.items() if k in ['article_resource_str_mv', 'source_publication', 'article_issn_str', 'datesort_str_mv', 'article_resource_related_str_mv']} for e in biblio_data if e.get('format_major')[0] == 'Journal article']
           
    data = {}
    for el in tqdm(biblio_data):
        name = el.get('article_resource_str_mv')[0] if 'article_resource_str_mv' in el else el.get('source_publication')
        if name not in data:
            test_dict = {}
            test_dict['name'] = name
            test_dict['issn'] = el.get('article_issn_str')
            try:
                year = el.get('datesort_str_mv')[0]
            except TypeError:
                try:
                    year = re.findall('\d{4}', el.get('article_resource_related_str_mv')[0])[0]
                except IndexError:
                    year = '0'
            try:
                number = get_number(el.get('article_resource_related_str_mv')[0])
            except TypeError:
                number = get_number(el.get('article_issn_str'))
            test_dict['years'] = {year: set([number])}
            data[name] = test_dict
        else:
            try:
                year = el.get('datesort_str_mv')[0]
            except TypeError:
                try:
                    year = re.findall('\d{4}', el.get('article_resource_related_str_mv')[0])[0]
                except IndexError:
                    year = '0'
            try:
                number = get_number(el.get('article_resource_related_str_mv')[0])
            except TypeError:
                number = get_number(el.get('article_issn_str'))
            if year in data[name]['years']:       
                data[name]['years'][year].add(number)
            else:
                data[name]['years'].update({year: set([number])})
                
    return list(data.values())
    # titles = [e.get('name') for e in data] 

    # journals_data = [{e.get('article_resource_str_mv')[0] if 'article_resource_str_mv' in e else e.get('source_publication'):e.get('datesort_str_mv')[0]} for e in biblio_data if e.get('format_major')[0] == 'Journal article']

    # test = [e for e in biblio_data if e.get('article_resource_str_mv') and e.get('format_major')[0] == 'Journal article']

    # biblio_journals = {}
    # for e in journals_data:
    #     k,v = tuple(e.items())[0]
    #     if k not in biblio_journals:
    #         biblio_journals[k] = set([v])
    #     else:
    #         biblio_journals[k].add(v)

    # [e.update({'years': biblio_journals.get(e.get('name'))}) for e in data]
    # data = [{'title' if k == 'name' else k:v for k,v in e.items()} for e in data]

def preprocess_journal_items(origin_data):  
    java_record_types = parse_java(r".\additional_files\pbl_record_types.txt")
    java_cocreators = parse_java(r".\additional_files\pbl_co-creator_types.txt")
    
    with open(r".\additional_files\language_map_iso639-1.ini", encoding='utf-8') as f:
        language_codes = {e.split(' = ')[-1].strip(): e.split(' = ')[0].strip() for e in f.readlines() if e}
    
    with open('./additional_files/dbn2pbl.json', encoding='utf-8') as jfile_1, open('./additional_files/new_pbl_headings.json', encoding='utf-8') as jfile_2:
        dbn2pbl = json.load(jfile_1)
        new_pbl_headings = json.load(jfile_2)
    
    pbl_cocreators_mapping = pd.read_excel("./additional_files/co-creators_mapping.xlsx")
    pbl_cocreators_mapping = {row['to_map']:row['pbl_code'] for idx,row in pbl_cocreators_mapping.iterrows()}
    
    origin_data = [e for e in origin_data if 'Journal article' in e.get('format_major') and 'fullrecord' in e]
    
    # full record
    full_recs = {}
    for rec in origin_data:
        rec_id = rec.get('id')
        full = rec.get('fullrecord')
        full_recs[rec_id] = full
        
    # record subjects 650 and 655
    recs_subs = {}
    for rec in origin_data:
        rec_id = rec.get('id')
        subjects_from_rec = re.findall('(?<=\=65[05]  ).+?(?=\r\n)', rec.get('fullrecord'))
        recs_subs[rec_id] = subjects_from_rec
    
    # authors and cocreators
    authors = {}
    cocreators = {}
    for rec in origin_data:
        rec_id = rec.get('id')
        cocreators_temp = {}
        if (persons_with_roles := rec.get('persons_with_roles')):
            for person in persons_with_roles:
                person_type, person_role = person.split('|')[-1].split(':')
                if person_type == 'author':
                    auth_name = person.split('|')[0]
                    auth_id = person.split('|')[4]
                    authors.setdefault(rec_id, set()).add((auth_id, auth_name))
                elif person_type == 'author2':
                    if person_role in ('Unknown', 'aut'):
                        auth_name = person.split('|')[0]
                        auth_id = person.split('|')[4]
                        authors.setdefault(rec_id, set()).add((auth_id, auth_name))
                    else:
                        coauth_name = person.split('|')[0]
                        coauth_id = person.split('|')[4]
                        person_role = pbl_cocreators_mapping.get(person_role, '')
                        cocreators_temp.setdefault((coauth_id, coauth_name), set()).add(person_role)
        cocreators_temp = set([(*k, tuple(v)) for k,v in cocreators_temp.items()])
        cocreators[rec_id] = cocreators_temp
    authors = {k:list(v) for k,v in authors.items()}                                  
    cocreators = {k:list(v) for k,v in cocreators.items()}
    
    # headings
    # def get_heading(string, bn=True):
    #     output_headings = set()
    #     if bn:
    #         string = re.sub('^..\$a', '', string).replace('$2DBN', '')
    #         old_pbl_headings = dbn2pbl.get(string, [])
    #         for head in old_pbl_headings:
    #             old_pbl_key = head['first_str'].lower()
    #             if new_heads := new_pbl_headings.get(old_pbl_key):
    #                 for new_head in new_heads:
    #                     output_headings.add(new_head['hash'])
    #     return list(output_headings)
        
    # headings = {}
    # for rec in origin_data:
    #     rec_id = rec.get('id')
    #     headings_set = set()
    #     subjects_from_rec = re.findall('(?<=\=65[05]  ).+?(?=\r\n)', rec.get('fullrecord'))
    #     for sub in subjects_from_rec:
    #         headings_set.update(get_heading(sub))
    #     if headings_set:
    #         headings[rec_id] = list(headings_set)
    
    with open('./additional_files/headings650.json', encoding='utf-8') as jfile_1, \
        open('./additional_files/headings655.json', encoding='utf-8') as jfile_2, \
        open('./additional_files/new_pbl_headings.json', encoding='utf-8') as jfile_3:
        headings650 = json.load(jfile_1)
        headings655 = json.load(jfile_2)
        new_pbl_headings = json.load(jfile_3)
        
    oracle_to_postgresql_df = pd.read_excel('./additional_files/oracle_postgresql.xlsx').fillna('').astype(str)
    oracle_to_postgresql_dct = {}
    for idx,row in oracle_to_postgresql_df.iterrows():
        oracle_id = row['oracle']
        postgresql_id = postgresql_id = set([e for e in row['postgresql'].split('\n') if e.endswith('.')])
        if oracle_id and postgresql_id:
            oracle_to_postgresql_dct.setdefault(oracle_id, set()).update(postgresql_id)
    oracle_to_postgresql_dct = {k:[new_pbl_headings.get(e) for e in v if new_pbl_headings.get(e)] for k,v in oracle_to_postgresql_dct.items()}
    
    oracle_dzialy = pd.read_excel('./additional_files/oracle_dzialy.xlsx').fillna('').astype(str)
    oracle_dzialy = dict(zip(oracle_dzialy['DZ_NAZWA'].to_list(), oracle_dzialy['DZ_DZIAL_ID'].to_list()))
    
    elb_literatures = pd.read_excel('./additional_files/elb_literatures.xlsx').fillna('').astype(str)
    elb_literatures = dict(zip(elb_literatures['literature'].to_list(), elb_literatures['hash'].to_list()))
    
    def get_heading(string, descriptor=True):
        output_headings = set()
        if descriptor:
            if (string := re.search('(?<=..\$a).+?(?=\$2|$)', string)):
                string = string.group(0)
                for dct in (headings650, headings655):
                    heads = [(e['path_str'], ' - '.join([str(h[0]) for h in e['chain']])) for e in dct.get(string, [])]
                    output_headings.update(heads)
        return list(output_headings)
    
    headings = {}
    for rec in origin_data:
        rec_id = rec.get('id')
        headings_set = set()
        subjects_from_rec = re.findall('(?<=\=65[05]  ).+?(?=\r\n)', rec.get('fullrecord'))
        for elem in subjects_from_rec:
            
            if '$2ELB' in elem:
                elb_elem_clean = re.search('(?<=..\$a).+?(?=\$2|$)', elem)
                if elb_hash := elb_literatures.get(elb_elem_clean):
                    headings_set.add(elb_hash)
                continue
            
            # nazwa taka sama jak dzialy oracle
            if elem_clean := re.search('(?<=..\$a).+?(?=\$2|$)', elem):
                elem_clean = elem_clean.group(0)
                postgresql_heading_z_dzialu = oracle_dzialy.get(elem_clean)
                postgresql_heading_z_dzialu = oracle_to_postgresql_dct.get(postgresql_heading_z_dzialu)
                if postgresql_heading_z_dzialu:
                    postgresql_heading_z_dzialu = set([e['hash'] for e in postgresql_heading_z_dzialu])
                    headings_set.update(postgresql_heading_z_dzialu)
            
            if '$a' in elem and '$2' in elem or elem.count('$') == 1 and '$a' in elem: # JHP
                oracle_headings = get_heading(elem)
                oracle_headings = set([e[1].split(' - ')[0] for e in oracle_headings])
                postgresql_headings = [oracle_to_postgresql_dct.get(e) for e in oracle_headings if oracle_to_postgresql_dct.get(e)]
                postgresql_headings = [item for row in postgresql_headings for item in row]
                postgresql_headings = set([e['hash'] for e in postgresql_headings])
                headings_set.update(postgresql_headings)
            else: # deskryptor
                pass
        if headings_set:
            headings[rec_id] = list(headings_set)
    # headings end
    
    records_types = [{e.get('id'): [ele for sub in [el.get('655') for el in parse_mrk(e.get('fullrecord'))] for ele in sub] if [el.get('655') for el in parse_mrk(e.get('fullrecord'))][0] else [el.get('655') for el in parse_mrk(e.get('fullrecord'))]} for e in origin_data]
    records_types = {list(e.keys())[0]:list(e.values())[0] for e in records_types}
    records_types = {k:[[el.get('$a') for el in marc_parser_for_field(e, '\\$') if '$a' in el][0] if not isinstance(e, type(None)) and '$a' in e else e for e in v] for k,v in records_types.items()}    
    records_types = {k:[java_record_types.get(e) for e in java_record_types if any(e in el.lower() for el in v)] if not isinstance(v[0], type(None)) else [java_record_types.get('inne')] for k,v in records_types.items()}
    records_types = {k:v if v else ['other'] for k,v in records_types.items()}
    
    languages = {e.get('id'): [language_codes.get(el) for el in e.get('language') if language_codes.get(el)] for e in origin_data}
    
    linked_objects = {e.get('id'): [ele for sub in [el.get('856') for el in parse_mrk(e.get('fullrecord'))] for ele in sub] if [el.get('856') for el in parse_mrk(e.get('fullrecord'))][0] else [el.get('856') for el in parse_mrk(e.get('fullrecord'))] for e in origin_data}
    #tutaj wydobyć linki do libri
    linked_objects = {k:[[el.get('$u') for el in marc_parser_for_field(e, '\\$') if '$u' in el][0] if not isinstance(e, type(None)) else e for e in v] for k,v in linked_objects.items()}
    linked_objects = {k: v if v[0] else None for k,v in linked_objects.items()}
    
    sources_data = {e.get('id'):{k:v for k,v in e.items() if k in ['article_resource_str_mv', 'source_publication', 'article_issn_str', 'datesort_str_mv', 'article_resource_related_str_mv']} for e in origin_data if e.get('format_major')[0] == 'Journal article'}
    
    preprocessed_data = []
    for elem in tqdm(origin_data):
        elem_id = elem.get('id')
        try:
            year = elem.get('datesort_str_mv')[0]
            journal_year_str = sources_data[elem_id].get('datesort_str_mv')[0]
        except TypeError:
            try:
                year = re.findall('\d{4}', elem.get('article_resource_related_str_mv')[0])[0]
                journal_year_str = re.findall('\d{4}', sources_data[elem_id].get('article_resource_related_str_mv')[0])[0]
            except IndexError:
                year = '0'
                journal_year_str = '0'
        try:
            journal_number_str = get_number(elem.get('article_resource_related_str_mv')[0])
        except TypeError:
            journal_number_str = get_number(elem.get('article_issn_str'))
        temp_dict = {
            'id_': '',
            'record_types': records_types.get(elem_id),
            'languages': languages.get(elem_id),
            'linked_ids': linked_objects.get(elem_id),
            'authors': authors.get(elem_id),
            'cocreators': cocreators.get(elem_id),
            'title': elem.get('title'),
            'year': year,
            'elb_id': elem_id,
            'journal_str': sources_data[elem_id].get('article_resource_str_mv')[0] if 'article_resource_str_mv' in sources_data[elem_id] else sources_data[elem_id].get('source_publication'), 
            'journal_year_str': journal_year_str, 
            'journal_number_str': journal_number_str,
            'pages': re.search('(?<=s\. ).+$', sources_data[elem_id].get('article_resource_related_str_mv')[0]).group(0) if sources_data[elem_id].get('article_resource_related_str_mv') and re.search('(?<=s\. ).+$', sources_data[elem_id].get('article_resource_related_str_mv')[0]) else '',
            'headings': headings.get(elem_id),
            'genre_major': elem.get('genre_major'),
            'subject_persons': [(e.split('|')[4], e.split('|')[0]) for e in elem.get('subject_person_str_mv', [])],
            }
        preprocessed_data.append(temp_dict)
    
    return preprocessed_data

def preprocess_books(origin_data, pub_places_data):
    # path, pub_places_path = r".\elb_input\biblio.json", r".\elb_input\pub_places.json"
    # origin_data, pub_places_data = import_biblio, import_pub_places
    # origin_data = import_biblio
    java_record_types = parse_java(r".\additional_files\pbl_record_types.txt")
    java_cocreators = parse_java(r".\additional_files\pbl_co-creator_types.txt")
    
    with open(r".\additional_files\language_map_iso639-1.ini", encoding='utf-8') as f:
        language_codes = {e.split(' = ')[-1].strip(): e.split(' = ')[0].strip() for e in f.readlines() if e}
    
    pbl_cocreators_mapping = pd.read_excel("./additional_files/co-creators_mapping.xlsx")
    pbl_cocreators_mapping = {row['to_map']:row['pbl_code'] for idx,row in pbl_cocreators_mapping.iterrows()}
    
    origin_data = [e for e in origin_data if 'Book' in e.get('format_major') and 'fullrecord' in e and any(el in e.get('fullrecord') for el in ['264', '260'])]
    
    # full record
    full_recs = {}
    for rec in origin_data:
        rec_id = rec.get('id')
        full = rec.get('fullrecord')
        full_recs[rec_id] = full
        
    # record subjects 650 and 655
    recs_subs = {}
    for rec in origin_data:
        rec_id = rec.get('id')
        subjects_from_rec = re.findall('(?<=\=65[05]  ).+?(?=\r\n)', rec.get('fullrecord'))
        recs_subs[rec_id] = subjects_from_rec
    
    # authors and cocreators
    authors = {}
    cocreators = {}
    for rec in origin_data:
        rec_id = rec.get('id')
        cocreators_temp = {}
        if (persons_with_roles := rec.get('persons_with_roles')):
            for person in persons_with_roles:
                person_type, person_role = person.split('|')[-1].split(':')
                if person_type == 'author':
                    auth_name = person.split('|')[0]
                    auth_id = person.split('|')[4]
                    authors.setdefault(rec_id, set()).add((auth_id, auth_name))
                elif person_type == 'author2':
                    if person_role in ('Unknown', 'aut'):
                        auth_name = person.split('|')[0]
                        auth_id = person.split('|')[4]
                        authors.setdefault(rec_id, set()).add((auth_id, auth_name))
                    else:
                        coauth_name = person.split('|')[0]
                        coauth_id = person.split('|')[4]
                        person_role = pbl_cocreators_mapping.get(person_role, '')
                        cocreators_temp.setdefault((coauth_id, coauth_name), set()).add(person_role)
        cocreators_temp = set([(*k, tuple(v)) for k,v in cocreators_temp.items()])
        cocreators[rec_id] = cocreators_temp
    authors = {k:list(v) for k,v in authors.items()}                                  
    cocreators = {k:list(v) for k,v in cocreators.items()}
    
    # headings old
    # def get_heading(string, bn=True):
    #     output_headings = set()
    #     if bn:
    #         string = re.sub('^..\$a', '', string).replace('$2DBN', '')
    #         old_pbl_headings = dbn2pbl.get(string, [])
    #         for head in old_pbl_headings:
    #             old_pbl_key = head['first_str'].lower()
    #             if new_heads := new_pbl_headings.get(old_pbl_key):
    #                 for new_head in new_heads:
    #                     output_headings.add(new_head['hash'])
    #     return list(output_headings)
        
    # headings = {}
    # for rec in origin_data:
    #     rec_id = rec.get('id')
    #     headings_set = set()
    #     subjects_from_rec = re.findall('(?<=\=65[05]  ).+?(?=\r\n)', rec.get('fullrecord'))
    #     for sub in subjects_from_rec:
    #         headings_set.update(get_heading(sub))
    #     if headings_set:
    #         headings[rec_id] = list(headings_set)

    # headings new
    # 650/655 jest $a i $2 lub tylko $a - deskryptory
    # inna sytuacja - 
    
    # with open('./additional_files/dbn2pbl.json', encoding='utf-8') as jfile_1, open('./additional_files/new_pbl_headings_updated.json', encoding='utf-8') as jfile_2:
    #     dbn2pbl = json.load(jfile_1)
    #     new_pbl_headings = json.load(jfile_2)
    
    with open('./additional_files/headings650.json', encoding='utf-8') as jfile_1, \
        open('./additional_files/headings655.json', encoding='utf-8') as jfile_2, \
        open('./additional_files/new_pbl_headings.json', encoding='utf-8') as jfile_3:
        headings650 = json.load(jfile_1)
        headings655 = json.load(jfile_2)
        new_pbl_headings = json.load(jfile_3)
        
    oracle_to_postgresql_df = pd.read_excel('./additional_files/oracle_postgresql.xlsx').fillna('').astype(str)
    oracle_to_postgresql_dct = {}
    for idx,row in oracle_to_postgresql_df.iterrows():
        oracle_id = row['oracle']
        postgresql_id = postgresql_id = set([e for e in row['postgresql'].split('\n') if e.endswith('.')])
        if oracle_id and postgresql_id:
            oracle_to_postgresql_dct.setdefault(oracle_id, set()).update(postgresql_id)
    oracle_to_postgresql_dct = {k:[new_pbl_headings.get(e) for e in v if new_pbl_headings.get(e)] for k,v in oracle_to_postgresql_dct.items()}
    
    oracle_dzialy = pd.read_excel('./additional_files/oracle_dzialy.xlsx').fillna('').astype(str)
    oracle_dzialy = dict(zip(oracle_dzialy['DZ_NAZWA'].to_list(), oracle_dzialy['DZ_DZIAL_ID'].to_list()))
    
    elb_literatures = pd.read_excel('./additional_files/elb_literatures.xlsx').fillna('').astype(str)
    elb_literatures = dict(zip(elb_literatures['literature'].to_list(), elb_literatures['hash'].to_list()))
    
    def get_heading(string, descriptor=True):
        output_headings = set()
        if descriptor:
            if (string := re.search('(?<=..\$a).+?(?=\$2|$)', string)):
                string = string.group(0)
                for dct in (headings650, headings655):
                    heads = [(e['path_str'], ' - '.join([str(h[0]) for h in e['chain']])) for e in dct.get(string, [])]
                    output_headings.update(heads)
        return list(output_headings)
    
    headings = {}
    for rec in origin_data:
        rec_id = rec.get('id')
        headings_set = set()
        subjects_from_rec = re.findall('(?<=\=65[05]  ).+?(?=\r\n)', rec.get('fullrecord'))
        for elem in subjects_from_rec:
            
            if '$2ELB' in elem:
                elb_elem_clean = re.search('(?<=..\$a).+?(?=\$2|$)', elem)
                if elb_hash := elb_literatures.get(elb_elem_clean):
                    headings_set.add(elb_hash)
                continue
            
            # nazwa taka sama jak dzialy oracle
            if elem_clean := re.search('(?<=..\$a).+?(?=\$2|$)', elem):
                elem_clean = elem_clean.group(0)
                postgresql_heading_z_dzialu = oracle_dzialy.get(elem_clean)
                postgresql_heading_z_dzialu = oracle_to_postgresql_dct.get(postgresql_heading_z_dzialu)
                if postgresql_heading_z_dzialu:
                    postgresql_heading_z_dzialu = set([e['hash'] for e in postgresql_heading_z_dzialu])
                    headings_set.update(postgresql_heading_z_dzialu)
            
            if '$a' in elem and '$2' in elem or elem.count('$') == 1 and '$a' in elem: # JHP
                oracle_headings = get_heading(elem)
                oracle_headings = set([e[1].split(' - ')[0] for e in oracle_headings])
                postgresql_headings = [oracle_to_postgresql_dct.get(e) for e in oracle_headings if oracle_to_postgresql_dct.get(e)]
                postgresql_headings = [item for row in postgresql_headings for item in row]
                postgresql_headings = set([e['hash'] for e in postgresql_headings])
                headings_set.update(postgresql_headings)
            else: # deskryptor
                pass
        if headings_set:
            headings[rec_id] = list(headings_set)
    # end headings section
    
    
    pub_places_data = [{k:v for k,v in e.items() if k in ['name', 'wiki']} for e in pub_places_data]
    
    records_types = [{e.get('id'): [ele for sub in [el.get('655') for el in parse_mrk(e.get('fullrecord'))] for ele in sub] if [el.get('655') for el in parse_mrk(e.get('fullrecord'))][0] else [el.get('655') for el in parse_mrk(e.get('fullrecord'))]} for e in origin_data]
    # records_types = dict(ChainMap(*records_types))
    records_types = {list(e.keys())[0]:list(e.values())[0] for e in records_types}
    records_types = {k:[[el.get('$a') for el in marc_parser_for_field(e, '\\$') if '$a' in el][0] if not isinstance(e, type(None)) and '$a' in e else e for e in v] for k,v in records_types.items()}
    records_types = {k:[java_record_types.get(e) for e in java_record_types if any(e in el.lower() for el in v)] if not isinstance(v[0], type(None)) else [java_record_types.get('inne')] for k,v in records_types.items()}
    records_types = {k:v if v else ['other'] for k,v in records_types.items()}
    
    languages = {e.get('id'): [language_codes.get(el) for el in e.get('language') if language_codes.get(el)] for e in origin_data}
    
    linked_objects = {e.get('id'): [ele for sub in [el.get('856') for el in parse_mrk(e.get('fullrecord'))] for ele in sub] if [el.get('856') for el in parse_mrk(e.get('fullrecord'))][0] else [el.get('856') for el in parse_mrk(e.get('fullrecord'))] for e in origin_data}
    #tutaj wydobyć linki do libri
    linked_objects = {k:[[el.get('$u') for el in marc_parser_for_field(e, '\\$') if '$u' in el][0] if not isinstance(e, type(None)) else e for e in v] for k,v in linked_objects.items()}
    linked_objects = {k: v if v[0] else None for k,v in linked_objects.items()}

    publishers_data = {e.get('id'): [ele for sub in [el.get('264') for el in parse_mrk(e.get('fullrecord'))] for ele in sub][0] if [el.get('264') for el in parse_mrk(e.get('fullrecord'))][0] else [el.get('260')[0] for el in parse_mrk(e.get('fullrecord'))][0] for e in origin_data}
    publishers_data = {k:[el for el in marc_parser_for_field(v, '\\$') if any(x in el for x in ['$a', '$b'])] for k,v in publishers_data.items()}
    
    publishers_data = {k:assign_places_to_publishers(v) for k,v in publishers_data.items()}
    publishers_data = {k:{ka[:-1] if ka[-1] == ',' else ka[:-2] if ka[-2:] == ' :' else ka[:-2] if ka[-2:] == ' ;' else ka[:-4] if re.findall(r'; \\1$', ka) else ka:[max([el for el in pub_places_data], key=lambda x: lev.ratio(x.get('name'), e)) for e in va] for ka,va in v.items()} for k,v in tqdm(publishers_data.items())}
    
    publishers_data = {k:{hashlib.md5(bytes(str(tuple((k,tuple(tuple(e.values()) for e in v)))),'utf-8')).hexdigest():(k,v) for k,v in v.items()} for k,v in publishers_data.items()}
    
    physical_description_data = {e.get('id'): [ele for sub in [el.get('300') for el in parse_mrk(e.get('fullrecord'))] for ele in sub][0] if [el.get('300') for el in parse_mrk(e.get('fullrecord'))][0] else '' for e in origin_data}
    physical_description_data = {k:''.join([list(e.values())[0] for e in marc_parser_for_field(v, '\\$')]) for k,v in physical_description_data.items()}
    
    preprocessed_data = []
    for elem in origin_data:
        elem_id = elem.get('id')
        try:
            year = elem.get('datesort_str_mv')[0]
        except TypeError:
            try:
                year = re.findall('\d{4}', elem.get('article_resource_related_str_mv')[0])[0]
            except IndexError:
                year = '0'
        temp_dict = {
            'id_': '',
            'record_types': records_types.get(elem_id),
            'languages': languages.get(elem_id),
            'linked_ids': linked_objects.get(elem_id),
            'authors': authors.get(elem_id),
            'title': elem.get('title'),
            'year': year,
            'elb_id': elem_id,
            'publishers': publishers_data.get(elem_id),
            'physical_description': physical_description_data.get(elem_id),
            'cocreators': cocreators.get(elem_id),
            'headings': headings.get(elem_id),
            'genre_major': elem.get('genre_major'),
            'subject_persons': [(e.split('|')[4], e.split('|')[0]) for e in elem.get('subject_person_str_mv', [])],
            }
        preprocessed_data.append(temp_dict)
    return preprocessed_data


#%% retro preprocessing

def get_retro_authorities_sets(data, filename):
    headings_df = pd.read_excel(f'./retro_input/retro_headings/{filename}_headings.xlsx').rename(columns={'0': 'idx', '1': 'heading'})
    headings = dict(zip(headings_df['idx'].to_list(), headings_df['hasła osobowe'].to_list()))
    output_persons = set()
    output_places = set()
    output_journals = dict()
    output_institutions = set(['[wydawnictwo nieznane]'])
    for group, records in data.items():
        for idx,record in enumerate(records):
            
            if (heading_author := record.get('Heading')):
                if headings.get(int(group)) == 'x':
                    output_persons.add(heading_author)
            
            authors = record.get('AUTOR', [])
            coauthors = record.get('WSPÓŁAUTOR', [])
            authors_full = authors + coauthors
            output_persons.update(authors_full)
            
            places = record.get('MIEJSCE_WYDANIA', [])
            output_places.update(places)
            
            journals = record.get('CZASOPISMO', [])
            journals_nmbers = record.get('NUMER_CZASOPISMA', [])
            if journals:
                output_journals.setdefault(journals[0], set()).update(journals_nmbers)
            
            institutions = record.get('WYDAWNICTWO', [])
            output_institutions.update(institutions)
        
    output_journals = {k:set(['Brak informacji o numerze.']) if len(v)==0 else v for k,v in output_journals.items()}
    output_journals = tuple(output_journals.items())
    
    return output_persons, output_places, output_journals, output_institutions

def preprocess_retro(data, filename, year):
    preprocessed_retro_data = []
    headings_df = pd.read_excel(f'./retro_input/retro_headings/{filename}_headings.xlsx').rename(columns={'0': 'idx', '1': 'heading'})
    headings = dict(zip(headings_df['idx'].to_list(), headings_df['hasła osobowe'].to_list()))
    retro_forms_df = pd.read_excel(f'./retro_input/retro_forms/{filename}_forms.xlsx').fillna('')
    retro_forms = dict(zip(retro_forms_df['RODZAJ_DZIEŁA_ZALEŻNEGO'].to_list(), retro_forms_df['pbl_form'].to_list()))
    for group, records in tqdm(data.items()):
        for idx,record in enumerate(records):
            rec_identifier = f'retro_{group}_{idx}'
            if 'DATA_WYDANIA' in record:
                rec_type = 'KS'
            else: rec_type = 'ART'
            
            if idx != 0:
                rec_reference = f'retro_{group}_0'
            else: 
                rec_reference = ''
                
            # authors and coauthors
            rec_authors = record.get('AUTOR')
            if not rec_authors:
                if idx==0:
                    if headings.get(int(group)) == 'x':
                        rec_authors = record.get('Heading')
            if rec_authors:
                rec_authors = [('', e) for e in rec_authors]
                        
            rec_coauthors = record.get('WSPÓŁAUTOR')
            if rec_coauthors:
                rec_coauthors = [('', e, tuple()) for e in rec_coauthors]
            
            # title 
            # TYTUŁ lub TITLE_EXTRACTED jeśli TYTUŁ pusty lub go nie ma i RODZAJ_DZIEŁA_ZALEŻNEGO jest puste lub go nie ma
            if rec_type == 'KS':
                if not (rec_title := record.get('TYTUŁ')):
                    if not record.get('RODZAJ_DZIEŁA_ZALEŻNEGO'):
                        if record.get('TITLE_EXTRACTED'):
                            rec_title = record.get('TITLE_EXTRACTED')
                        elif record.get('new_title_from_rec_without_title'):
                            rec_title = record.get('new_title_from_rec_without_title')
                        else:
                            rec_title = '[bez tytułu]'
                    else:
                        rec_title = '[bez tytułu]'
            elif rec_type == 'ART':
                if record.get('RODZAJ_DZIEŁA_ZALEŻNEGO'):
                    rec_title = '[bez tytułu]'
                elif record.get('TYTUŁ'):
                    rec_title = record.get('TYTUŁ')
                elif record.get('TITLE_EXTRACTED'):
                    rec_title = record.get('TITLE_EXTRACTED')
                else:
                    rec_title = '[bez tytułu]'
            if isinstance(rec_title, list):
                rec_title = ' '.join(rec_title)
                    
            rec_pub_year = record.get('DATA_WYDANIA')
            if rec_pub_year:
                rec_pub_year = [int(re.search('\d{4}', year).group(0)) for year in rec_pub_year if re.search('\d{4}', year)]
                if rec_pub_year:
                    rec_pub_year = str(max(rec_pub_year))
                else:
                    rec_pub_year = year
            
            rec_pub_place = record.get('MIEJSCE_WYDANIA')
            if rec_pub_place:
                places = [{'name': place} for place in rec_pub_place]
                publisher_name = '[wydawnictwo nieznane]'
                publisher_id = hashlib.md5(bytes(str(tuple((publisher_name,tuple(tuple(e.values()) for e in places)))),'utf-8')).hexdigest()
                publishers = {publisher_id: (publisher_name, places)}
            else: publishers = None
            
            rec_book_issue = record.get('WYDANIE')
            rec_physical_desc = record.get('STRONY')
            if rec_physical_desc:
                rec_physical_desc = ' '.join(rec_physical_desc)
            
            rec_journal = record.get('CZASOPISMO', '')
            if rec_journal:
                rec_journal = rec_journal[0]
            
            rec_journal_issue = record.get('NUMER_CZASOPISMA', '')
            if rec_journal_issue:
                rec_journal_issue = rec_journal_issue[0]
            
            if rec_type == 'KS':
                if rec_authors: type_ = 'authorsBook'
                else: type_ = 'collectiveBook'   
            
            
            if rec_type == 'KS':
                rec_form = 'other'
            elif rec_type == 'ART':
                if (eval_poem_novel := record.get('eval_is_wiersz_proza')):
                    if eval_poem_novel == 'Wiersz':
                        rec_form = 'poem'
                    elif eval_poem_novel == 'Proza':
                        rec_form = 'prose'
                else:
                    if (rec_form := record.get('RODZAJ_DZIEŁA_ZALEŻNEGO')):
                        rec_form = retro_forms.get(rec_form[0], 'other')
                    else:
                        rec_form = 'other'
                         
            rec_heading = record.get('Heading', '')
            rec_annotation = f'Rekord pochodzi z automatycznego parsowania drukowanego tomu PBL. Oryginalna postać rekordu w druku: {record.get("original_rec")}'
            
            if rec_type == 'KS':
                record_dict = {
                    'id_': rec_identifier,
                    'rec_type': rec_type,
                    'authors': rec_authors,
                    'cocreators': rec_coauthors,
                    'title': rec_title,
                    'year': rec_pub_year,
                    'publishers': publishers,
                    'rec_book_issue': rec_book_issue,
                    'physical_description': rec_physical_desc,
                    'tags': rec_heading,
                    'annotation': rec_annotation,
                    'type_': type_,
                    'record_types': rec_form,
                    'rec_reference': rec_reference,
                    }
            elif rec_type == 'ART':
                record_dict = {
                    'id_': rec_identifier,
                    'rec_type': rec_type,
                    'authors': rec_authors,
                    'cocreators': rec_coauthors,
                    'title': rec_title,
                    'journal_str': rec_journal,
                    'journal_number_str': rec_journal_issue,
                    'journal_year_str': year, # rocznik pbl
                    'pages': rec_physical_desc,
                    'tags': rec_heading,
                    'annotation': rec_annotation,
                    'record_types': rec_form,
                    'rec_reference': rec_reference,
                    }
                
            record_dict = {k:v for k,v in record_dict.items() if v}
            preprocessed_retro_data.append(record_dict)
    return preprocessed_retro_data
            
    

# def preprocess_chapters(path):
#     path = r".\elb_input\biblio.json"
#     java_record_types = parse_java(r".\additional_files\pbl_record_types.txt")
#     java_cocreators = parse_java(r".\additional_files\pbl_co-creator_types.txt")
    
#     with open(r".\additional_files\language_map_iso639-1.ini", encoding='utf-8') as f:
#         language_codes = {e.split(' = ')[-1].strip(): e.split(' = ')[0].strip() for e in f.readlines() if e}
    
#     with open(path, encoding='utf-8') as f, open(pub_places_path, encoding='utf-8') as pf:
#         origin_data = json.load(f)
#         pub_places_data = json.load(pf)
    
#     pub_places_data = [{k:v for k,v in e.items() if k in ['name', 'wiki']} for e in pub_places_data]
#     origin_data = [e for e in origin_data if 'Book chapter' in e.get('format_major')]
#%% kartotek – ver 1
# !!!miejsca!!!

# #plik od MG, który ma miejsca jako tematy – MG ma wygenerować lepszej jakości plik
# with open(r"F:\Cezary\Documents\IBL\Libri\dane z libri do pbl\2023-02-15\pub_places.json", encoding='utf-8') as f:
#     data = json.load(f)

# # data = [{k:v.split('|')[0] if k=='name' else e.get('name').split('|')[-1] if k=='wiki' else v for k,v in e.items()} for e in data]

# wikidata_ids = set([e.get('wiki') for e in data if e.get('wiki')])

# with ThreadPoolExecutor() as executor:
#     wikidata_response = list(tqdm(executor.map(lambda p: get_wikidata_label(p, ['pl', 'en']), wikidata_ids)))
    
# # with ThreadPoolExecutor() as executor:
# #     wikidata_coordinates = list(tqdm(executor.map(get_wikidata_coordinates, wikidata_ids)))

# wikidata_labels = dict(zip(wikidata_ids, wikidata_response))
# # wikidata_coordinates = dict(zip(wikidata_ids, wikidata_coordinates))

# data = [dict(e) for e in set([tuple({k:wikidata_labels.get(e.get('wiki'), v) if k == 'name' else v for k,v in e.items() if k != 'recCount'}.items()) for e in data])]
# # data = [dict(e) for e in set([tuple({k:wikidata_coordinates.get(e.get('wiki'), v) if k == 'coordinates' else v for k,v in e.items() if k != 'recCount'}.items()) for e in data])]

# !!!osoby!!!
# with open(r"F:\Cezary\Documents\IBL\Libri\dane z libri do pbl\2023-02-08\persons.json", encoding='utf-8') as f:
#     data = json.load(f)

# [e.update({'dateB': e.get('fromWiki', {}).get('dateB')}) for e in data]
# [e.update({'dateD': e.get('fromWiki', {}).get('dateD')}) for e in data]
# data = [{k:e.get('dateB') if k == 'yearBorn' and isinstance(e.get('dateB'), str) else v for k,v in e.items()} for e in data]
# data = [{k:e.get('dateD') if k == 'yearDeath' and isinstance(e.get('dateD'), str) else v for k,v in e.items()} for e in data]
# [e.update({'placeB': e.get('fromWiki', {}).get('placeB')}) for e in data]
# [e.update({'placeD': e.get('fromWiki', {}).get('placeD')}) for e in data]

# #przejmujemy daty z wiki i nadpisujemy yearBorn i yearDeath, jak wiki puste, to zostaje to, co było

# #co jeśli jedna osoba (ten sam wiki id) ma kilka nazw? czy to w ogóle się zdarza?

# # len([e.get('wiki') for e in data if e.get('wiki')])
# # len(set([e.get('wiki') for e in data if e.get('wiki')]))
# data = [{k:v for k,v in e.items() if k not in ['dateB', 'dateD', 'fromWiki', 'recCount']} for e in data]

# !!!wydarzenia!!!

# with open(r"F:\Cezary\Documents\IBL\Libri\dane z libri do pbl\2023-02-15\events.json", encoding='utf-8') as f:
#     data = json.load(f)
    
# data = [{k:v for k,v in e.items() if k != 'recCount'} for e in data]

# event_dict = {
#     'Konkursy': 'competition',
#     'Nagrody polskie': 'prize',
#     'Nagrody zagraniczne': 'prize',
#     'Odznaczenia': 'decoration',
#     'Plebiscyty': 'plebiscite',
#     'Wystawy': 'exhibition',
#     'Zjazdy, festiwale, sesje w Polsce': 'festival',
#     'Zjazdy, festiwale, sesje za granicą': 'festival'
# }

# [e.update({'type': [el for el in event_dict if el in e.get('name')][0] if [el for el in event_dict if el in e.get('name')] else None}) for e in data]
# data = [{k:v.replace(e.get('type')+', ','') if k=='name' and e.get('type') else v for k,v in e.items()} for e in data]
# data = [{k:event_dict.get(v) if k == 'type' and v else v for k,v in e.items()} for e in data]

# event_dict2 = {
#     'doktorat honoris causa': 'honorary-doctorate',
#     'festiwal': 'festival',
#     'konferencja': 'conference',
#     'konkurs': 'competition',
#     'nagroda': 'prize',
#     'odznaczenie': 'decoration',
#     'plebiscyt': 'plebiscite',
#     'spotkanie autorskie': 'authors-meeting',
#     'wystawa': 'exhibition'
# }
# data = [{k:event_dict2.get([el for el in event_dict2 if el in e.get('name').lower()][0]) if k=='type' and [el for el in event_dict2 if el in e.get('name').lower()] else v for k,v in e.items()} for e in data]

# data = [{'type_' if k=='type' else k:v for k,v in e.items()} for e in data]

# !!!serie wydawnicze!!!

# path = r"F:\Cezary\Documents\IBL\Libri\dane z libri do pbl\2023-02-16\biblio.json"

# with open(path, encoding='utf-8') as f:
#     data = json.load(f)

# series_data = [e for e in data if '=490' in e.get('fullrecord')]
# series_data = [e.get('series') for e in series_data]
# series_data = set([ele for sub in [[' ; '.join([el.strip() for el in e[0].split(' ; ')][1:])] if len(e) == 1 and re.findall('\d+ \;', e[0]) else e for e in series_data] for ele in sub])
# series_data = set([[el.strip() for el in e.split(';')][0] for e in series_data])
# series_data = [{'title': e} for e in series_data]

# !!!utwory!!!

# path = r"F:\Cezary\Documents\IBL\Libri\dane z libri do pbl\2023-02-16\biblio.json"

# with open(path, encoding='utf-8') as f:
#     data = json.load(f)

# # test_data = [e for e in data if 'Literature' in e.get('genre_major') and 'author' in e]
# data = [{'name': e.get('author')[0].split('|')[0], 'wiki': e.get('author')[0].split('|')[4], 'title': e.get('title').strip()} for e in data if 'Literature' in e.get('genre_major') and 'author' in e]











