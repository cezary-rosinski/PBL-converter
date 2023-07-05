# 1. uruchamiamy klasy w odpowiedniej kolejności
# 2. łączymy elementy tak, by skorzystać z wcześniej powołanych klas, np. miejsca wydarzeń początkowo są str, ale później stają się obiektem Place
# 3. generujemy XML z wzbogaconych klas


#UWAGA --> kartoteka miejsc -- fake id 1434 nie ma nazwy miejsca --> czemu?
#UWAGA2 --> pliki XML trzeba dzielić na pliki po 10000 rekordów każdy

#%% import
import xml.etree.cElementTree as ET
from datetime import datetime
import json
from tqdm import tqdm
import pandas as pd
import os

from SPUB_preprocessing import preprocess_places, preprocess_people, preprocess_institutions, preprocess_events, preprocess_publishing_series, preprocess_creative_works, preprocess_journal_items, preprocess_journals, preprocess_books, preprocess_retro, get_retro_authorities_sets
from SPUB_additional_functions import give_fake_id

# from SPUB_kartoteki_klasy import Place, Person, Event, PublishingSeries
from SPUB_files_place import Place
from SPUB_files_person import Person
from SPUB_files_institutions import Institution
from SPUB_fiels_event import Event
from SPUB_files_publishing_series import PublishingSeries
from SPUB_files_creative_work import CreativeWork
from SPUB_files_journal import Journal
from SPUB_records_journal_item import JournalItem
from SPUB_records_book import Book


#%% import data
with open(r".\elb_input\places.json", encoding='utf-8') as f:
    import_places = [e for e in json.load(f) if 'publication place' in e.get('roles') or 'event place' in e.get('roles')]
with open(r".\elb_input\persons.json", encoding='utf-8') as f:
    import_persons = json.load(f)
with open(r".\elb_input\corporates.json", encoding='utf-8') as f:
    import_corporates = json.load(f)
with open(r".\elb_input\events.json", encoding='utf-8') as f:
    import_events = json.load(f)
with open(r".\elb_input\biblio.json", encoding='utf-8') as f:
    import_biblio = json.load(f)
    
#%% preprocess data

places_data = preprocess_places(import_places)

person_data = preprocess_people(import_persons)

institutions_data = preprocess_institutions(import_corporates, import_biblio)

events_data = preprocess_events(import_events)

series_data = preprocess_publishing_series(import_biblio)

creative_works_data = preprocess_creative_works(import_biblio)

journals_data = preprocess_journals(import_biblio)

journal_items_data = preprocess_journal_items(import_biblio)

books_data = preprocess_books(import_biblio, import_places)

#%% create class

places = [Place.from_dict(e) for e in tqdm(places_data)]
last_number = give_fake_id(places)

persons = [Person.from_dict(e) for e in tqdm(person_data)]
last_number = give_fake_id(persons, last_number)
for person in tqdm(persons):
    person.connect_with_places(places)
    
institutions = [Institution.from_dict(e) for e in tqdm(institutions_data)]
last_number = give_fake_id(institutions, last_number)

events = [Event.from_dict(e) for e in tqdm(events_data)]
last_number = give_fake_id(events, last_number)
for event in tqdm(events):
    event.connect_with_places(places) 

publishing_series_list = [PublishingSeries.from_dict(e) for e in tqdm(series_data)]
last_number = give_fake_id(publishing_series_list, last_number)

creative_works = [CreativeWork.from_dict(e) for e in tqdm(creative_works_data)]
last_number = give_fake_id(creative_works, last_number)

persons_to_connect = {}
for p in persons:
    for name in p.names:
        persons_to_connect.update({name.value: p.id})
#UWAGA --> jeśli jest to samo nazewnictwo dla różnych id, to zachowujemy ostatnią parę
#NA PRZYSZŁOŚĆ --> zebrać wszystkie duplikaty nazewnictwa, zbierać w odrębnej zmiennej i rozwiązać ten problem inaczej
for creative_work in tqdm(creative_works):
    creative_work.connect_with_persons(persons_to_connect)
    
journals = [Journal.from_dict(e) for e in tqdm(journals_data)]
#UWAGA --> z powodu błędów w danych czasem year == 0
last_number = give_fake_id(journals, last_number)
    
journal_items = [JournalItem.from_dict(e) for e in tqdm(journal_items_data)]
last_number = give_fake_id(journal_items, last_number)

journals_to_connect = {}
for j in journals:
    for title in j.titles:
        journals_to_connect.update({title.value: j})

for journal_item in tqdm(journal_items):
    journal_item.connect_with_persons(persons_to_connect)
    journal_item.connect_with_journals(journals_to_connect)
    
books = [Book.from_dict(e) for e in tqdm(books_data)]
last_number = give_fake_id(books, last_number)

institutions_to_connect = {}
for i in institutions:
    for name in i.names:
        institutions_to_connect.update({name.value: i.id})

for book in tqdm(books):
    book.connect_with_persons(persons_to_connect)
    book.connect_publisher(places, institutions_to_connect)

#%% enrich classes


#%% export xml

# for place in places:
#     print(place.__dict__)

# slicing records lists
# plik zapisów po 50 000 rekordów, pliki kartoteki utworów po 50 000, pozostałe pliki kartotek po 10 000 rekordów
step = 5
output = [places[i:i + step] for i in range(0, len(places), step)]

# places
places_xml = ET.Element('pbl')
files_node = ET.SubElement(places_xml, 'files')
places_node = ET.SubElement(files_node, 'places')
for idx,place in tqdm(places):
    places_node.append(place.to_xml())
    
tree = ET.ElementTree(places_xml)
ET.indent(tree, space="\t", level=0)
tree.write('./xml_output/import_places.xml', encoding='UTF-8')

# persons
persons_xml = ET.Element('pbl')
files_node = ET.SubElement(persons_xml, 'files')
people_node = ET.SubElement(files_node, 'people')
for person in tqdm(persons):
    people_node.append(person.to_xml())

tree = ET.ElementTree(persons_xml)
ET.indent(tree, space="\t", level=0)
tree.write('./xml_output/import_people.xml', encoding='UTF-8')

# institutions
institutions_xml = ET.Element('pbl')
files_node = ET.SubElement(institutions_xml, 'files')
institutions_node = ET.SubElement(files_node, 'institutions')
for institution in tqdm(institutions):
    institutions_node.append(institution.to_xml())

tree = ET.ElementTree(institutions_xml)
ET.indent(tree, space="\t", level=0)
tree.write('./xml_output/import_institutions.xml', encoding='UTF-8')

# events
events_xml = ET.Element('pbl')
files_node = ET.SubElement(events_xml, 'files')
events_node = ET.SubElement(files_node, 'events')
for event in tqdm(events):
    events_node.append(event.to_xml())

tree = ET.ElementTree(events_xml)
ET.indent(tree, space="\t", level=0)
tree.write('./xml_output/import_events.xml', encoding='UTF-8')

# publishing series
publishing_series_list_xml = ET.Element('pbl')
files_node = ET.SubElement(publishing_series_list_xml, 'files')
publishing_series_list_node = ET.SubElement(files_node, 'publishing-series-list')
for publishing_series in tqdm(publishing_series_list):
    publishing_series_list_node.append(publishing_series.to_xml())
    
tree = ET.ElementTree(publishing_series_list_xml)
ET.indent(tree, space="\t", level=0)
tree.write('./xml_output/import_publishing_series_list.xml', encoding='UTF-8')

# creative works
creative_works_xml = ET.Element('pbl')
files_node = ET.SubElement(creative_works_xml, 'files')
creative_works_node = ET.SubElement(files_node, 'creative_works')
for creative_work in tqdm(creative_works):
    creative_works_node.append(creative_work.to_xml())

tree = ET.ElementTree(creative_works_xml)
ET.indent(tree, space="\t", level=0)
tree.write('./xml_output/import_creative_works.xml', encoding='UTF-8')

# journals
journals_xml = ET.Element('pbl')
files_node = ET.SubElement(journals_xml, 'files')
journals_node = ET.SubElement(files_node, 'journals')
journals_years_node = ET.SubElement(files_node, 'journal-years')
journals_numbers_node = ET.SubElement(files_node, 'journal-numbers')
for journal in tqdm(journals):
    journals_node.append(journal.to_xml())
    for year_xml in journal.years_to_xml():
        journals_years_node.append(year_xml)
    for number_xml in journal.numbers_to_xml():
        journals_numbers_node.append(number_xml)
  
tree = ET.ElementTree(journals_xml)
ET.indent(tree, space="\t", level=0)
tree.write('./xml_output/import_journals.xml', encoding='UTF-8')

#journal items & books
records_xml = ET.Element('pbl')
files_node = ET.SubElement(records_xml, 'records')
journal_items_node = ET.SubElement(files_node, 'journal-items')
for journal_item in tqdm(journal_items):
    journal_items_node.append(journal_item.to_xml())
books_node = ET.SubElement(files_node, 'books')
for book in tqdm(books):
    books_node.append(book.to_xml())

tree = ET.ElementTree(records_xml)
ET.indent(tree, space="\t", level=0)
tree.write('./xml_output/import_journal_items_and_books.xml', encoding='UTF-8')


# for book in books:
#     x = book.to_xml()
#     tree = ET.ElementTree(x)
#     ET.indent(tree, space="\t", level=0)
#     tree.write('./xml_output/import_journal_items_and_books.xml', encoding='UTF-8')


# [e for e in books_data if e.get('elb_id') == 'b1000000941897']


#%% RETRO
# przygotowuje pliki do manualnej selekcji rodzajów i działów

# preparing headings
for filename in tqdm(os.listdir('retro_input')[::-1]):
    if filename.startswith('1'):
        # filename = '1984_t2.json'
        retro_year = filename[:4]
        fname = filename[:-5]
        
        with open(f"./retro_input/{filename}", encoding='utf8') as f:
            retro_data = json.load(f)
            
        out = []
        for k,v in retro_data.items():
            if v:
                out.append((k, v[0].get('Heading')))
        df = pd.DataFrame(out)
        df.to_excel(f'./additional_files/retro_headings/{fname}_headings.xlsx', index=False)
    
        # preparing retro forms
        out_forms = set()
        for k,v in retro_data.items():
            for rec in v:
                if (form := rec.get('RODZAJ_DZIEŁA_ZALEŻNEGO')):
                    out_forms.update(form)
        df = pd.DataFrame(out_forms)
        df.to_excel(f'./additional_files/retro_forms/{fname}_forms.xlsx', index=False)
# prepare heading and form tables before continuation
   
#%%% count recs

# for filename in tqdm(os.listdir('retro_input')[::-1]):
#     if filename.startswith('1'):
#         retro_year = filename[:4]
#         fname = filename[:-5]

#         if os.path.exists(f'./retro_input/retro_headings/{fname}_headings.xlsx') and os.path.exists(f'./retro_input/retro_forms/{fname}_forms.xlsx'):
        
#             with open(f"./retro_input/{filename}", encoding='utf8') as f:
#                 retro_data = json.load(f)
        
#             retro_pre_persons, retro_pre_places, retro_pre_journals, retro_pre_institutions = get_retro_authorities_sets(retro_data, fname)
    
#             retro_places = [Place(id_='', lat='', lon='', name=e, annotation=annotation_auth_files) for e in tqdm(retro_pre_places)]
#             last_number = give_fake_id(retro_places)
    
#             retro_persons = [Person(id_='', viaf='', name=e, annotation=annotation_auth_files) for e in tqdm(retro_pre_persons)]
#             last_number = give_fake_id(retro_persons, last_number)
    
#             retro_institutions = [Institution(id_='', viaf='', name=e, annotation=annotation_auth_files) for e in tqdm(retro_pre_institutions)]
#             last_number = give_fake_id(retro_institutions, last_number)
    
#             retro_journals = [Journal(title=e[0], years_with_numbers_set=((retro_year, e[1]),), annotation=annotation_auth_files) for e in tqdm(retro_pre_journals)]
#             last_number = give_fake_id(retro_journals, last_number)
            
#             records_prep = preprocess_retro(retro_data, fname, retro_year)
            
#             auth_len = len(retro_places) + len(retro_persons) + len(retro_institutions) + len(retro_journals)
#             recs_len = len(records_prep)
#             full_len = auth_len + recs_len
            
#             today = datetime.today().strftime('%Y-%m-%d')
#             if not os.path.exists(f"./additional_files/retro_count_{today}.xlsx"):
#                 count_df = pd.DataFrame(columns=['filename', 'auth_len', 'recs_len', 'full_len'])
#                 count_df.to_excel(f"./additional_files/retro_count_{today}.xlsx", index=False)
            
#             count_df = pd.read_excel(f"./additional_files/retro_count_{today}.xlsx")
#             count_df.loc[len(count_df)] = [filename, auth_len, recs_len, full_len]
#             count_df.to_excel(f"./additional_files/retro_count_{today}.xlsx", index=False)

# print('\nauth_len: ', count_df['auth_len'].sum(), '\nrecs_len: ',count_df['recs_len'].sum(), '\nfull_len: ', count_df['full_len'].sum())

#%% retro loop
# przetwarza wszystkie pliki z retro, jeśli trzeba przetworzyć jeden plik, wystarczy podać jednoelementową listę do pętli
annotation_auth_files = 'Rekord powstał wskutek w pełni automatycznego parsowania drukowanego tomu PBL.'

for file in tqdm(os.listdir('retro_input')[::-1]):
    if file.startswith('1'):
        retro_year = file[:4]
        filename = file[:-5]

        if os.path.exists(f'./retro_input/retro_headings/{filename}_headings.xlsx') and os.path.exists(f'./retro_input/retro_forms/{filename}_forms.xlsx'):
            if not os.path.exists(f'./xml_output/retro/{filename}'):
                os.mkdir(f'./xml_output/retro/{filename}')
                
            with open(f"./retro_input/{filename}.json", encoding='utf8') as f:
                retro_data = json.load(f)   
            
            # authorities preprocessing
            retro_pre_persons, retro_pre_places, retro_pre_journals, retro_pre_institutions = get_retro_authorities_sets(retro_data, filename)
    
            retro_places = [Place(id_='', lat='', lon='', name=e) for e in tqdm(retro_pre_places)]
            last_number = give_fake_id(retro_places, retro=True, retro_filename=filename)
    
            retro_persons = [Person(id_='', viaf='', name=e, annotation=annotation_auth_files) for e in tqdm(retro_pre_persons)]
            last_number = give_fake_id(retro_persons, last_number, retro=True, retro_filename=filename)
    
            retro_institutions = [Institution(id_='', viaf='', name=e, annotation=annotation_auth_files) for e in tqdm(retro_pre_institutions)]
            last_number = give_fake_id(retro_institutions, last_number, retro=True, retro_filename=filename)
    
            retro_journals = [Journal(title=e[0], years_with_numbers_set=((retro_year, e[1]),), annotation=annotation_auth_files) for e in tqdm(retro_pre_journals)]
            last_number = give_fake_id(retro_journals, last_number, retro=True, retro_filename=filename)
            
            # records preprocessing
            records_prep = preprocess_retro(retro_data, filename, retro_year)
            without_title = [e for e in records_prep if not 'title' in e]
            if without_title:
                raise Exception('without_title not empty!')
    
            # ----------------------------------
            retro_persons_to_connect = {}
            for p in retro_persons:
                for name in p.names:
                    retro_persons_to_connect.update({name.value: p.id})
    
            retro_journals_to_connect = {}
            for j in retro_journals:
                for title in j.titles:
                    retro_journals_to_connect.update({title.value: j})
    
            retro_institutions_to_connect = {}
            for i in retro_institutions:
                for name in i.names:
                    retro_institutions_to_connect.update({name.value: i.id})
    
            retro_books = [Book.from_retro(e) for e in tqdm(records_prep) if e['rec_type']=='KS']
            last_number = give_fake_id(retro_books, last_number, retro=True, retro_filename=filename)
    
            for book in tqdm(retro_books):
                book.connect_with_persons(retro_persons_to_connect)
                book.connect_publisher(retro_places, retro_institutions_to_connect)
    
            retro_journal_items = [JournalItem.from_retro(e) for e in tqdm(records_prep) if e['rec_type']=='ART']
            last_number = give_fake_id(retro_journal_items, last_number, retro=True, retro_filename=filename)
    
            for journal_item in tqdm(retro_journal_items):
                journal_item.connect_with_persons(retro_persons_to_connect)
                journal_item.connect_with_journals(retro_journals_to_connect)
            
            # xml creation
            # places
            places_xml = ET.Element('pbl')
            files_node = ET.SubElement(places_xml, 'files')
            places_node = ET.SubElement(files_node, 'places')
            for place in tqdm(retro_places):
                places_node.append(place.to_xml())
    
            tree = ET.ElementTree(places_xml)
            ET.indent(tree, space="\t", level=0)
            tree.write(f'./xml_output/retro/{filename}/import_retro_places_{filename}.xml', encoding='UTF-8')
    
            # persons
            persons_xml = ET.Element('pbl')
            files_node = ET.SubElement(persons_xml, 'files')
            people_node = ET.SubElement(files_node, 'people')
            for person in tqdm(retro_persons):
                people_node.append(person.to_xml())
    
            tree = ET.ElementTree(persons_xml)
            ET.indent(tree, space="\t", level=0)
            tree.write(f'./xml_output/retro/{filename}/import_retro_people_{filename}.xml', encoding='UTF-8')
    
            # institutions
            institutions_xml = ET.Element('pbl')
            files_node = ET.SubElement(institutions_xml, 'files')
            institutions_node = ET.SubElement(files_node, 'institutions')
            for institution in tqdm(retro_institutions):
                institutions_node.append(institution.to_xml())
    
            tree = ET.ElementTree(institutions_xml)
            ET.indent(tree, space="\t", level=0)
            tree.write(f'./xml_output/retro/{filename}/import_retro_institutions_{filename}.xml', encoding='UTF-8')
    
            # journals
            journals_xml = ET.Element('pbl')
            files_node = ET.SubElement(journals_xml, 'files')
            journals_node = ET.SubElement(files_node, 'journals')
            journals_years_node = ET.SubElement(files_node, 'journal-years')
            journals_numbers_node = ET.SubElement(files_node, 'journal-numbers')
            for journal in tqdm(retro_journals):
                journals_node.append(journal.to_xml())
                for year_xml in journal.years_to_xml():
                    journals_years_node.append(year_xml)
                for number_xml in journal.numbers_to_xml():
                    journals_numbers_node.append(number_xml)
              
            tree = ET.ElementTree(journals_xml)
            ET.indent(tree, space="\t", level=0)
            tree.write(f'./xml_output/retro/{filename}/import_retro_journals_{filename}.xml', encoding='UTF-8')
    
            # journal items
            records_xml = ET.Element('pbl')
            files_node = ET.SubElement(records_xml, 'records')
            journal_items_node = ET.SubElement(files_node, 'journal-items')
            for journal_item in tqdm(retro_journal_items):
                journal_items_node.append(journal_item.to_xml())
    
            tree = ET.ElementTree(records_xml)
            ET.indent(tree, space="\t", level=0)
            tree.write(f'./xml_output/retro/{filename}/import_retro_journal_items_{filename}.xml', encoding='UTF-8')
    
            # books
            records_xml = ET.Element('pbl')
            files_node = ET.SubElement(records_xml, 'records')
            books_node = ET.SubElement(files_node, 'books')
            for book in tqdm(retro_books):
                books_node.append(book.to_xml())
                
            tree = ET.ElementTree(records_xml)
            ET.indent(tree, space="\t", level=0)
            tree.write(f'./xml_output/retro/{filename}/import_retro_books_{filename}.xml', encoding='UTF-8')

