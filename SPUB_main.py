# 1. uruchamiamy klasy w odpowiedniej kolejności
# 2. łączymy elementy tak, by skorzystać z wcześniej powołanych klas, np. miejsca wydarzeń początkowo są str, ale później stają się obiektem Place
# 3. generujemy XML z wzbogaconych klas

#%% import
import xml.etree.cElementTree as ET
from datetime import datetime
import json
from tqdm import tqdm

from SPUB_preprocessing import preprocess_places, preprocess_people, preprocess_institutions, preprocess_events, preprocess_publishing_series, preprocess_creative_works, preprocess_journal_items, preprocess_journals, preprocess_books
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
with open(r".\elb_input\pub_places.json", encoding='utf8') as f:
    import_pub_places = json.load(f)
with open(r".\elb_input\event_places.json", encoding='utf8') as f:
    import_event_places = json.load(f)
with open(r".\elb_input\persons.json", encoding='utf8') as f:
    import_persons = json.load(f)
with open(r".\elb_input\corporates.json", encoding='utf8') as f:
    import_corporates = json.load(f)
with open(r".\elb_input\events.json", encoding='utf8') as f:
    import_events = json.load(f)
with open(r".\elb_input\biblio.json", encoding='utf8') as f:
    import_biblio = json.load(f)
    
#%% preprocess data

places_data = preprocess_places(import_pub_places, import_event_places)

person_data = preprocess_people(import_persons)

institutions_data = preprocess_institutions(import_corporates, import_biblio)

events_data = preprocess_events(import_events)

series_data = preprocess_publishing_series(import_biblio)

creative_works_data = preprocess_creative_works(import_biblio)

journals_data = preprocess_journals(import_biblio)

journal_items_data = preprocess_journal_items(import_biblio)

books_data = preprocess_books(import_biblio, import_pub_places)

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

# places
places_xml = ET.Element('pbl')
files_node = ET.SubElement(places_xml, 'files')
places_node = ET.SubElement(files_node, 'places')
for place in tqdm(places):
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









