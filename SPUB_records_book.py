import json
import regex as re
from collections import ChainMap
from datetime import datetime
import xml.etree.cElementTree as ET

from SPUB_additional_functions import get_wikidata_label, get_wikidata_coordinates, simplify_string, marc_parser_for_field, parse_mrk, parse_java, get_number

# na późńiej --> książki przedmiotowe dostają typ 'other', to jest do ulepszenia

#%%
AuthorsList = list[tuple[str, str]]
CocreatorsList = list[tuple[str, str, tuple[str]]]

# dodac wydanie -> <edition>Jakiś tekst</edition>

class Book:
    def __init__(self, id_, title='', record_types=None, authors: AuthorsList|None = None, cocreators: CocreatorsList|None = None, languages=None, linked_ids=None, elb_id=None, physical_description='', publishers=None, year='', annotation='', tags=None, type_='authorsBook', collection=None, headings=None, genre_major=None, subject_persons=None, **kwargs):
        self.id = f"http://www.wikidata.org/entity/Q{id_}" if id_ else None
        self.creator = 'cezary_rosinski'
        self.status = 'published'
        self.date = str(datetime.today().date())
        self.publishing_date = self.date
        self.origin = ''
        self.flags = ''
        self.elb_id = elb_id
        self.title = self.BookTitle(title.strip())
        
        # authorsBook, collectiveBook, letters, anthology
        self.type = type_
        
        if record_types:
            if isinstance(record_types, str):
                record_types = [record_types]
            self.record_types = record_types
        else: self.record_types = []
            
        if authors:
            if isinstance(authors, str):
                authors = [authors]
            self.authors = [self.BookAuthor(author_id=auth_tuple[0], author_name=auth_tuple[1]) for auth_tuple in authors]
        else: self.authors = []
        
        if cocreators:
            if isinstance(cocreators, str):
                cocreators = [cocreators]
            self.cocreators = [self.BookCoCreator(cocreator_id=cocreat_tuple[0], cocreator_name=cocreat_tuple[1], cocreator_roles=cocreat_tuple[2]) for cocreat_tuple in cocreators]
        else: self.cocreators = []
        
        self.general_materials = 'false'
        
        if languages:
            self.languages = languages
        else: self.languages = []
        
        if headings:
            self.headings = headings
        else: self.headings = []
        
        if linked_ids:
            self.linked_objects = linked_ids
        else: self.linked_objects = []
        
        if publishers:
            self.publishers = [self.BookPublishingHouse(publisher_id=k, publisher_value=v) for k,v in publishers.items()]
        else: self.publishers = []
        
        if year:
            self.year = self.BookPublicationYear(year=year)
        else:
            self.year = ''
            
        self.physical_description = physical_description
        self.annotation = annotation
        
        if tags:
            self.tags = tags
        else:
            self.tags = []
        
        self.collection = collection
        
        self.genre_major = genre_major
        
        if subject_persons:
            self.subject_persons = [self.BookSubjectPerson(*sub_person) for sub_person in subject_persons]
        else: self.subject_persons = []
        
    class XmlRepresentation:
        
        def to_xml(self):
            match self.__class__.__name__:
                case 'BookAuthor':
                    return ET.Element('author', {'id': self.author_id, 'juvenile': self.juvenile, 'co-creator': self.co_creator, 'principal': self.principal})
                case 'BookCoCreator':
                    cocreator_xml = ET.Element('co-creator')
                    for tp in self.types:
                        cocreator_xml.append(ET.Element('type', {'code' : tp}))
                    cocreator_xml.append(ET.Element('person', {'id' : self.cocreator_id}))
                    return cocreator_xml
                case 'BookTitle':
                    title_xml = ET.Element('title', {'code': self.code, 'transliteration': self.transliteration, 'newest': self.newest})
                    title_xml.text = self.value
                    return title_xml
                case 'BookLinkedObejct':
                    pass
                case 'BookPublishingHouse':
                    publishing_house_xml = ET.Element('publishing-house', {'id': self.publisher_id})
                    if self.institution_id:
                        publishing_house_xml.append(ET.Element('institution', {'id': self.institution_id}))
                    if self.places:
                        places_xml = ET.Element('places')
                        for place in self.places:
                            places_xml.append(ET.Element('place', place))
                        publishing_house_xml.append(places_xml)  
                    return publishing_house_xml
                case 'BookPublicationYear':
                    publication_year_xml = ET.Element('publication-year', {'year': self.year, 'uncertain': self.uncertain, 'explanation': self.explanation, 'type': self.type})
                    return publication_year_xml
                    
    
    class BookAuthor(XmlRepresentation):
        
        def __init__(self, author_id, author_name):
            self.author_id = f"http://www.wikidata.org/entity/Q{author_id}" if author_id else ''
            self.juvenile = 'false'
            self.co_creator = 'false'
            self.principal = 'true'
            self.author_name = author_name
            self.headings = []

        def __repr__(self):
            return "BookAuthor('{}', '{}')".format(self.author_id, self.author_name)
        
    class BookCoCreator(XmlRepresentation):
        # rozwiazac problem typow wspoltworstwa
        def __init__(self, cocreator_id, cocreator_name, cocreator_roles=None):
            self.cocreator_id = f"http://www.wikidata.org/entity/Q{cocreator_id}" if cocreator_id else ''
            if cocreator_roles:
                self.types = [t for t in cocreator_roles]
            else:
                self.types = []
            self.cocreator_name = cocreator_name

        def __repr__(self):
            return "BookCoCreator('{}', '{}')".format(self.cocreator_id, self.cocreator_name)
        
    class BookSubjectPerson:
        
        def __init__(self, sub_person_id, sub_person_name):
            self.sub_person_id = f"http://www.wikidata.org/entity/Q{sub_person_id}" if sub_person_id else ''
            self.sub_person_name = sub_person_name
            self.headings = []

        def __repr__(self):
            return "BookSubjectPerson('{}', '{}')".format(self.sub_person_id, self.sub_person_name)
    
    class BookTitle(XmlRepresentation):
        
        def __init__(self, value):
            self.value = value
            self.code = 'base'
            self.newest = 'true'
            self.transliteration = 'false'
            
        def __repr__(self):
            return "BookTitle('{}')".format(self.value) 
        
    class BookLinkedObejct(XmlRepresentation):
        pass
    
    class BookPublishingHouse(XmlRepresentation):
        
        def __init__(self, publisher_id, publisher_value):
            self.publisher_id = publisher_id
            self.institution_name = publisher_value[0]
            self.institution_id = ''
            self.places = publisher_value[-1]  
            
        def __repr__(self):
            return "BookPublishingHouse('{}', '{}', '{}')".format(self.institution_id, self.institution_name, self.places)
    
    class BookPublicationYear(XmlRepresentation):
        
        def __init__(self, year):
            self.year = year
            self.uncertain = 'false'
            self.explanation = ''
            self.type = 'publishing-years'
            
        def __repr__(self):
            return "BookPublicationYear('{}')".format(self.year)
    
    @classmethod
    def from_dict(cls, book_dict):
        return cls(**book_dict, collection='polska-bibliografia-literacka-1989-')
    
    @classmethod
    def from_retro(cls, retro_book_dict):
        if retro_book_dict.get('tags'):
            retro_book_dict['tags'] = [retro_book_dict['tags']]
        return cls(**retro_book_dict, collection='polska-bibliografia-literacka-1944-1988')  
    
    def connect_with_places(self, publisher_instance, list_of_places_class):
        for i, place in enumerate(publisher_instance.places):
            if (wiki_id:=place.get('wiki')):
                correct_place = [e for e in list_of_places_class if f'http://www.wikidata.org/entity/Q{wiki_id}' == e.id]
            else:
                correct_place = [e for e in list_of_places_class if [el for el in e.periods if el.name == place.get('name')]]
            if correct_place:
                publisher_instance.places[i] = {'id': correct_place[0].id,
                                                'period': f'{correct_place[0].periods[0].date_from}❦{correct_place[0].periods[0].date_to}',
                                                'lang': correct_place[0].periods[0].lang}
    
    def connect_with_institutions(self, publisher_instance, institutions_to_connect):
        correct_institution = institutions_to_connect.get(publisher_instance.institution_name)
        if correct_institution:
            publisher_instance.institution_id = correct_institution
            
    def connect_publisher(self, list_of_places_class, institutions_to_connect):
        for publisher in self.publishers:
            self.connect_with_places(publisher, list_of_places_class)
            self.connect_with_institutions(publisher, institutions_to_connect)
    
    def add_authors_headings(self):
        if len(self.genre_major) == 1:
            if 'Literature' in self.genre_major:
                for auth in self.authors:
                    for auth_heading in auth.headings:
                        self.headings.append((auth_heading, auth.author_id))
            elif 'Secondary literature' in self.genre_major:
                for sub_person in self.subject_persons:
                    for sub_heading in sub_person.headings:
                        self.headings.append((auth_heading, sub_person.sub_person_id))
                
               
    def connect_with_persons(self, persons_to_connect):
        for author in self.authors:
            if not author.author_id:
                match_person = persons_to_connect.get(author.author_name)
                if match_person:
                    author.author_id = match_person.id
                    author.headings = match_person.headings 
        for cocreator in self.cocreators:
            if not cocreator.cocreator_id:
                match_person = persons_to_connect.get(cocreator.cocreator_name)
                if match_person:
                    cocreator.cocreator_id = match_person.id
        for sub_person in self.subject_persons:
            if not sub_person.sub_person_id:
                match_person = persons_to_connect.get(sub_person.sub_person_name)
                if match_person:
                    sub_person.sub_person_id = match_person.id
                    sub_person.headings = match_person.headings
        self.add_authors_headings()
        
        
    def to_xml(self):
        book_dict = {k:v for k,v in {'id': self.id, 'type': self.type, 'status': self.status, 'creator': self.creator, 'creation-date': self.date, 'publishing-date': self.publishing_date, 'origin': self.origin, 'flags': self.flags}.items() if v}
        book_xml = ET.Element('book', book_dict)
        
        if self.record_types:
            record_types_xml = ET.Element('record-types')
            for rec_type in self.record_types:
                record_types_xml.append(ET.Element('record-type', {'code': rec_type}))
            book_xml.append(record_types_xml)    
        
        book_xml.append(ET.Element('general-materials', {'value': 'true'}))
        
        if self.authors:
            authors_xml = ET.Element('authors', {'anonymous': 'false', 'author-company': 'false'})
            for author in self.authors:
                authors_xml.append(author.to_xml())
        else:
            authors_xml = ET.Element('authors', {'anonymous': 'true', 'author-company': 'false'})
        book_xml.append(authors_xml)
        
        if self.cocreators:
            cocreators_xml = ET.Element('co-creators')
            for cocreator in self.cocreators:
                cocreators_xml.append(cocreator.to_xml())
            book_xml.append(cocreators_xml)
        
        if self.title:
            titles_xml = ET.Element('titles')
            titles_xml.append(self.title.to_xml())
            book_xml.append(titles_xml)
            
        if self.languages:
            languages_xml = ET.Element('languages')
            for lang in self.languages:
                languages_xml.append(ET.Element('language', {'code': lang}))
            book_xml.append(languages_xml)
        
        if self.headings:
            headings_xml = ET.Element('headings')
            for heading_item in self.headings:
                if isinstance(heading_item, tuple):
                    heading = heading_item[0]
                    person_id = heading_item[1]
                    if person_id:
                        headings_xml.append(ET.Element('heading', {'id': heading, 'person-id': person_id}))
                else:
                    headings_xml.append(ET.Element('heading', {'id': heading_item}))
            book_xml.append(headings_xml)  
            
        if self.publishers:
            publishing_houses_xml = ET.Element('publishing-houses', {'by-author': 'false'})
            for publisher in self.publishers:
                publishing_houses_xml.append(publisher.to_xml())
            book_xml.append(publishing_houses_xml)
        
        if self.year:
            book_xml.append(self.year.to_xml())
            
        if self.physical_description:
            book_xml.append(ET.Element('physical-description', {'co-created': 'no', 'description': self.physical_description}))
        
        if self.annotation:
            annotation_xml = ET.Element('annotation')
            annotation_xml.text = self.annotation
            book_xml.append(annotation_xml)
            
        if self.tags:
            tags_xml = ET.Element('tags')
            for tag in self.tags:
                tag_xml = ET.Element('tag')
                tag_xml.text = tag
                tags_xml.append(tag_xml)
        
        if self.collection:
            book_xml.append(ET.Element('collection', {'code': self.collection}))        
        
        return book_xml

#%%

# books = [Book.from_dict(e) for e in books_data]
# give_fake_id(books)
# for book in books:
#     book.connect_with_persons(persons)
#     book.connect_publisher(places, institutions)

#%% schemat XML 
# <books>
# 			<!-- event.status = published|draft|prepared -->

#             <book id="authors-book-id-01" type="authorsBook" status="published" creator="a_margraf" creation-date="2022-12-01" publishing-date="2022-12-03" origin="B-src-id-01" flags="123">

#                 <record-types>
#                     <record-type code="poem"/>
#                     <record-type code="novel"/>
#                 </record-types>

#                 <general-materials value="true" />

#                 <authors anonymous="false" author-company="false">
#                     <author id="a0000001758844" juvenile="false" co-creator="true" principal="true"/>
#                     <author id="person-id-02" juvenile="false" co-creator="true" />
#                     <author id="person-id-03" juvenile="true" co-creator="false" />
#                 </authors>

#                 <titles>
#                     <title code="base" transliteration="false" newest="true">Pozycja w czasopiśmie @{"Kazimierzu-Przerwa Tetmajerze"|person_id="przerwa_id"}</title>
#                     <title code="original" transliteration="true">PC</title>
#                 </titles>

#                 <languages>
#                     <language code="pl"/>
#                 </languages>

#                 <co-creators>
#                     <co-creator>
#                         <type code="translation" />
#                         <type code="adaptation" />
#                         <person id="person-id-01" />
#                     </co-creator>

#                     <co-creator>
#                         <type code="adaptation" />
#                         <person id="person-id-02" />
#                     </co-creator>
#                 </co-creators>

#                 <headings>
#                     <heading id="7320f7ca8352d748b386ab4e4913e3ee"/>
#                     <heading id="a31385e80345e59e06b208e998bcaeab"/>
#                     <heading id="e99e5257f8af377ba21568d1fb73e368"/>
#                 </headings>

#                 <annotation>To jest tekst o @{"Kazimierzu-Przerwa Tetmajerze"|person_id="przerwa_id"} oraz o jego przyjacielu @{"Franku Kowalskim"|person_id="franciszek_kowalski_id"}. Pewnie możnaby tu jeszcze coś uzupełnić o @{"Oli Sroce"|person_id="ola_sroka_id"}. Ich firmat to @{"Firemka sp. z o.o."|institution_id="firemka_id"} właśnie.</annotation>

#                 <remark>Tutaj można wpisać tekst komentarza dla Creative works. Komentarze są niewidoczne na stronie</remark>


#                 <tags>
#                     <tag>#ji-nike</tag>
#                     <tag>#ji-nicMiNiePrzychodzi</tag>
#                     <!-- ... -->
#                 </tags>

#                 <links>
#                     <link access-date="12.05.2021" type="external-identifier">http://pbl.poznan.pl/</link>
#                     <link access-date="18.05.2021" type="broader-description-access">http://ibl.pbl.waw.pl/</link>
#                     <!-- ... -->
#                 </links>


#                 <linked-objects>
#                     <record-ref id="r-id-01"/>
#                     <!-- ... -->
#                     <creative-work-ref id="cw-id-01"/>
#                     <!-- ... -->
#                     <series-ref id="s-id-01"/>
#                     <!-- ... -->
#                     <institution-ref id="https://viaf.org/viaf/000000002/"/>
#                     <institution-ref id="https://viaf.org/viaf/000000003/"/>
#                     <!-- ... -->
#                     <event-ref id="e-id-01"/>
#                     <event-series-ref id="es-id-01"/>
#                     <!-- ... -->
#                     <place-ref id="p-id-01" period="" lang=""/>
#                     <!-- ... -->
#                     <journal-source-ref id="js-id"/>
#                     <!-- ... -->
#                 </linked-objects>


#                 <trigger-institutions>
#                     <institution id="https://viaf.org/viaf/000000002/"/>
#                     <institution id="https://viaf.org/viaf/000000003/"/>
#                 </trigger-institutions>

# 				<publishing-houses by-author="false">
# 				    <publishing-house id="ph-01">
#                         <institution id="institution-id-01"/>
# 				        <places>
# 					        <place id="https://www.wikidata.org/wiki/Q268" period="❦" lang="pl"/>  <!-- Poznań -->
# 					        <place id="https://www.wikidata.org/wiki/Q52842" lang="pl"/>  <!-- Kalisz -->
# 				        </places>
#                     </publishing-house>

# 					<publishing-house id="ph-02">
# 				        <places>
# 					        <place id="https://www.wikidata.org/wiki/Q1475264" period="2005-10-01❦" />  <!-- Obra -->
# 				        </places>
#                     </publishing-house>


# 					<publishing-house id="ph-03">
#                         <institution id="https://viaf.org/viaf/000000000/"/>
#                     </publishing-house>
# 				</publishing-houses>

#                 <mailings>
#                     <mailing>
#                         <sender>
#                             <author id="person-id-11" juvenile="false" co-creator="true" />
#                         </sender>

#                         <addressee>
#                             <institution id="institution-id-15" />
#                         </addressee>
#                         <description>Opis pierwszego mailingu</description>
#                     </mailing>
#                 </mailings>

#                 <interviews>
#                     <interview>
#                         <interviewer>
#                             <author id="person-id-11" juvenile="false" co-creator="true" />
#                         </interviewer>
#                         <interlocutor>
#                             <institution id="institution-id-15" />
#                         </interlocutor>
#                         <description>Opis pierwszego mailingu</description>
#                     </interview>
#                 </interviews>

#                 <publication-year year="1994" uncertain="false" explanation="Wyjasnienie" type="publishing-years" /> <!-- publishing-years | copyright-years | different-years -->

#                 <belonging-to-series series-number="23" sub-series-number="2323" publishing-series="pub-series-id-01" />

#                 <physical-description coCreated="yes" description="Opis tutaj">
#                     <authors-book id="ab-id-01" />
#                     <authors-book id="ab-id-02" />
#                     <!-- ... -->
#                     <anthology-book id="nb-id-01" />
#                     <!-- ... -->
#                     <collective-book id="cb-id-01" />
#                     <!-- ... -->
#                     <letters id="nb-id-02" />
#                     <!-- ... -->
#                 </physical-description>

#                 <co-edited-books>
#                     <book-ref id="b-id-01" />
#                     <book-ref id="b-id-02" />
#                 </co-edited-books>

#             </book>