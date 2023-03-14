#%% note
# plik, w którym przetwarzamy i ewentualnie wzbogadamy dane wejściowe otrzymane od MG

#%% import
import json
from concurrent.futures import ThreadPoolExecutor
from SPUB_additional_functions import get_wikidata_label, get_wikidata_coordinates, simplify_string, marc_parser_for_field, parse_mrk, parse_java, get_number
from tqdm import tqdm
import regex as re
from collections import ChainMap
import Levenshtein as lev
import hashlib

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

def preprocess_places(*paths):
    data = []
    for path in paths:
        with open(path, encoding='utf-8') as f:
            data.extend(json.load(f))
    wikidata_ids = set([e.get('wiki') for e in data if e.get('wiki')])
    with ThreadPoolExecutor() as executor:
        wikidata_response = list(tqdm(executor.map(lambda p: get_wikidata_label(p, ['pl', 'en']), wikidata_ids)))
    wikidata_labels = dict(zip(wikidata_ids, wikidata_response))
    data = [dict(e) for e in set([tuple({k:wikidata_labels.get(e.get('wiki'), v) if k == 'name' else v for k,v in e.items() if k != 'recCount'}.items()) for e in data])]
    return data

def preprocess_people(path):
    with open(path, encoding='utf-8') as f:
        data = json.load(f)
    [e.update({'dateB': e.get('fromWiki', {}).get('dateB')}) for e in data]
    [e.update({'dateD': e.get('fromWiki', {}).get('dateD')}) for e in data]
    data = [{k:e.get('dateB') if k == 'yearBorn' and isinstance(e.get('dateB'), str) else v for k,v in e.items()} for e in data]
    data = [{k:e.get('dateD') if k == 'yearDeath' and isinstance(e.get('dateD'), str) else v for k,v in e.items()} for e in data]
    [e.update({'placeB': e.get('fromWiki', {}).get('placeB')}) for e in data]
    [e.update({'placeD': e.get('fromWiki', {}).get('placeD')}) for e in data]
    return [{k:v for k,v in e.items() if k not in ['dateB', 'dateD', 'fromWiki', 'recCount']} for e in data]

def preprocess_institutions(path, biblio_path):
    with open(path, encoding='utf-8') as f, open(biblio_path, encoding='utf-8') as g:
        data = json.load(f)
        biblio_data = json.load(g)
    data = [{k:v for k,v in e.items() if k != 'recCount'} for e in data]
    origin_data = [e for e in biblio_data if 'Book' in e.get('format_major')]
        
    publishers = [[ele for sub in [el.get('264') for el in parse_mrk(e.get('fullrecord'))] for ele in sub][0] if [el.get('264') for el in parse_mrk(e.get('fullrecord'))][0] else [el.get('260')[0] for el in parse_mrk(e.get('fullrecord'))][0] for e in origin_data]
    
    # publishers = [[el for el in marc_parser_for_field(e, '\\$') if any(x in el for x in ['$a', '$b'])] for e in publishers]
    # set([''.join([ele for sub in [list(el.keys()) for el in e] for ele in sub]) for e in publishers])
    
    publishers = set([[el.get('$b') for el in marc_parser_for_field(e, '\\$') if '$b' in el][0] for e in publishers])
    publishers = set([e[:-1] if e[-1] == ',' else e[:-2] if e[-2:] == ' :' else e[:-2] if e[-2:] == ' ;' else e[:-4] if re.findall(r'; \\1$', e) else e for e in publishers])

    for publisher in publishers:
        if publisher not in [e.get('name') for e in data]:
            data.append({'name': publisher, 'viaf': '', 'wiki': ''})
    return data

def preprocess_events(path):
    with open(path, encoding='utf-8') as f:
        data = json.load(f)
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
    [e.update({'type': [el for el in event_dict if el in e.get('name')][0] if [el for el in event_dict if el in e.get('name')] else None}) for e in data]
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

def preprocess_publishing_series(path):
    with open(path, encoding='utf-8') as f:
        data = json.load(f)
    data = [e for e in data if '=490' in e.get('fullrecord')]
    data = [e.get('series') for e in data]
    data = set([ele for sub in [[' ; '.join([el.strip() for el in e[0].split(' ; ')][1:])] if len(e) == 1 and re.findall('\d+ \;', e[0]) else e for e in data] for ele in sub])
    data = set([[el.strip() for el in e.split(';')][0] for e in data])
    data = [{'title': e} for e in data]
    return data

def preprocess_creative_works(path):
    with open(path, encoding='utf-8') as f:
        data = json.load(f)
    return [{'name': e.get('author')[0].split('|')[0], 'wiki': e.get('author')[0].split('|')[4], 'title': e.get('title').strip()} for e in data if 'Literature' in e.get('genre_major') and 'author' in e]

def preprocess_journals(path):
    
    with open(path, encoding='utf-8') as f:
        biblio_data = json.load(f)
    
    # with open(r"F:\Cezary\Documents\IBL\Libri\dane z libri do pbl\2023-02-15\magazines.json", encoding='utf-8') as f:
    #     data2 = json.load(f)
        
    # data2 = [{k:v for k,v in e.items() if k != 'recCount'} for e in data2]
    # mg_titles = [e.get('name') for e in data2]
    
    biblio_data = [{k:v for k,v in e.items() if k in ['article_resource_str_mv', 'source_publication', 'article_issn_str', 'datesort_str_mv', 'article_resource_related_str_mv']} for e in biblio_data if e.get('format_major')[0] == 'Journal article']
           
    data = {}
    for el in biblio_data:
        name = el.get('article_resource_str_mv')[0] if 'article_resource_str_mv' in el else el.get('source_publication')
        if name not in data:
            test_dict = {}
            test_dict['name'] = name
            test_dict['issn'] = el.get('article_issn_str')
            year = el.get('datesort_str_mv')[0]
            number = get_number(el.get('article_resource_related_str_mv')[0])
            test_dict['years'] = {year: set([number])}
            data[name] = test_dict
        else:
            year = el.get('datesort_str_mv')[0]
            number = get_number(el.get('article_resource_related_str_mv')[0])
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

def preprocess_journal_items(path):    
    java_record_types = parse_java(r".\additional_files\pbl_record_types.txt")
    java_cocreators = parse_java(r".\additional_files\pbl_co-creator_types.txt")
    
    with open(r".\additional_files\language_map_iso639-1.ini", encoding='utf-8') as f:
        language_codes = {e.split(' = ')[-1].strip(): e.split(' = ')[0].strip() for e in f.readlines() if e}
    
    with open(path, encoding='utf-8') as f:
        origin_data = json.load(f)
    
    origin_data = [e for e in origin_data if 'Journal article' in e.get('format_major')]
    
    records_types = [{e.get('id'): [ele for sub in [el.get('655') for el in parse_mrk(e.get('fullrecord'))] for ele in sub] if [el.get('655') for el in parse_mrk(e.get('fullrecord'))][0] else [el.get('655') for el in parse_mrk(e.get('fullrecord'))]} for e in origin_data]
    records_types = dict(ChainMap(*records_types))
    
    records_types = {k:[[el.get('$a') for el in marc_parser_for_field(e, '\\$') if '$a' in el][0] if not isinstance(e, type(None)) else e for e in v] for k,v in records_types.items()}
    
    records_types = {k:[java_record_types.get(e) for e in java_record_types if any(e in el.lower() for el in v)] if not isinstance(v[0], type(None)) else [java_record_types.get('inne')] for k,v in records_types.items()}
    records_types = {k:v if v else ['other'] for k,v in records_types.items()}
    
    languages = {e.get('id'): [language_codes.get(el) for el in e.get('language') if language_codes.get(el)] for e in origin_data}
    
    linked_objects = {e.get('id'): [ele for sub in [el.get('856') for el in parse_mrk(e.get('fullrecord'))] for ele in sub] if [el.get('856') for el in parse_mrk(e.get('fullrecord'))][0] else [el.get('856') for el in parse_mrk(e.get('fullrecord'))] for e in origin_data}
    #tutaj wydobyć linki do libri
    linked_objects = {k:[[el.get('$u') for el in marc_parser_for_field(e, '\\$') if '$u' in el][0] if not isinstance(e, type(None)) else e for e in v] for k,v in linked_objects.items()}
    linked_objects = {k: v if v[0] else None for k,v in linked_objects.items()}
    
    sources_data = {e.get('id'):{k:v for k,v in e.items() if k in ['article_resource_str_mv', 'source_publication', 'article_issn_str', 'datesort_str_mv', 'article_resource_related_str_mv']} for e in origin_data if e.get('format_major')[0] == 'Journal article'}
    
    preprocessed_data = []
    for elem in origin_data:
        elem_id = elem.get('id')
        temp_dict = {
            'id_': '',
            'types': records_types.get(elem_id),
            'languages': languages.get(elem_id),
            'linked_ids': linked_objects.get(elem_id),
            'author_name': elem.get('author')[0].split('|')[0] if elem.get('author') else '',
            'author_id': elem.get('author')[0].split('|')[4] if elem.get('author') else '',
            'title': elem.get('title'),
            'year': elem.get('datesort_str_mv')[0],
            'elb_id': elem_id,
            'journal_str': sources_data[elem_id].get('article_resource_str_mv')[0] if 'article_resource_str_mv' in sources_data[elem_id] else sources_data[elem_id].get('source_publication'), 
            'journal_year_str': sources_data[elem_id].get('datesort_str_mv')[0], 
            'journal_number_str': get_number(sources_data[elem_id].get('article_resource_related_str_mv')[0]),
            'pages': re.search('(?<=s\. ).+$', sources_data[elem_id].get('article_resource_related_str_mv')[0]).group(0) if sources_data[elem_id].get('article_resource_related_str_mv') and re.search('(?<=s\. ).+$', sources_data[elem_id].get('article_resource_related_str_mv')[0]) else ''
            }
        preprocessed_data.append(temp_dict)
    
    return preprocessed_data

def preprocess_books(path, pub_places_path):
    # path, pub_places_path = r".\elb_input\biblio.json", r".\elb_input\pub_places.json"
    java_record_types = parse_java(r".\additional_files\pbl_record_types.txt")
    java_cocreators = parse_java(r".\additional_files\pbl_co-creator_types.txt")
    
    with open(r".\additional_files\language_map_iso639-1.ini", encoding='utf-8') as f:
        language_codes = {e.split(' = ')[-1].strip(): e.split(' = ')[0].strip() for e in f.readlines() if e}
    
    with open(path, encoding='utf-8') as f, open(pub_places_path, encoding='utf-8') as pf:
        origin_data = json.load(f)
        pub_places_data = json.load(pf)
    
    pub_places_data = [{k:v for k,v in e.items() if k in ['name', 'wiki']} for e in pub_places_data]
    origin_data = [e for e in origin_data if 'Book' in e.get('format_major')]
    
    records_types = [{e.get('id'): [ele for sub in [el.get('655') for el in parse_mrk(e.get('fullrecord'))] for ele in sub] if [el.get('655') for el in parse_mrk(e.get('fullrecord'))][0] else [el.get('655') for el in parse_mrk(e.get('fullrecord'))]} for e in origin_data]
    records_types = dict(ChainMap(*records_types))
    
    records_types = {k:[[el.get('$a') for el in marc_parser_for_field(e, '\\$') if '$a' in el][0] if not isinstance(e, type(None)) else e for e in v] for k,v in records_types.items()}
    
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
    publishers_data = {k:{ka[:-1] if ka[-1] == ',' else ka[:-2] if ka[-2:] == ' :' else ka[:-2] if ka[-2:] == ' ;' else ka[:-4] if re.findall(r'; \\1$', ka) else ka:[max([el for el in pub_places_data], key=lambda x: lev.ratio(x.get('name'), e)) for e in va] for ka,va in v.items()} for k,v in publishers_data.items()}
    
    publishers_data = {k:{hashlib.md5(bytes(str(tuple((k,tuple(tuple(e.values()) for e in v)))),'utf-8')).hexdigest():(k,v) for k,v in v.items()} for k,v in publishers_data.items()}
    
    physical_description_data = {e.get('id'): [ele for sub in [el.get('300') for el in parse_mrk(e.get('fullrecord'))] for ele in sub][0] if [el.get('300') for el in parse_mrk(e.get('fullrecord'))][0] else '' for e in origin_data}
    physical_description_data = {k:''.join([list(e.values())[0] for e in marc_parser_for_field(v, '\\$')]) for k,v in physical_description_data.items()}
    
    preprocessed_data = []
    for elem in origin_data:
        elem_id = elem.get('id')
        temp_dict = {
            'id_': '',
            'types': records_types.get(elem_id),
            'languages': languages.get(elem_id),
            'linked_ids': linked_objects.get(elem_id),
            'author_name': elem.get('author')[0].split('|')[0] if elem.get('author') else '',
            'author_id': elem.get('author')[0].split('|')[4] if elem.get('author') else '',
            'title': elem.get('title'),
            'year': elem.get('datesort_str_mv')[0],
            'elb_id': elem_id,
            'publishers': publishers_data.get(elem_id),
            'physical_description': physical_description_data.get(elem_id)
            }
        preprocessed_data.append(temp_dict)
    return preprocessed_data

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











