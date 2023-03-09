import json
import regex as re
from collections import ChainMap
from datetime import datetime
import xml.etree.cElementTree as ET

from SPUB_additional_functions import get_wikidata_label, get_wikidata_coordinates, simplify_string, marc_parser_for_field, parse_mrk, parse_java, get_number

# na późńiej --> książki przedmiotowe dostają typ 'other', to jest do ulepszenia

#%%

class Book:
    
    def __init__(self, id_, title='', types=None, author_id='', author_name='', languages=None, linked_ids=None, elb_id=None, physical_description='', publishers=None, year='', **kwargs):
        self.id = f"http://www.wikidata.org/entity/Q{id_}"if id_ else None
        self.creator = 'cezary_rosinski'
        self.status = 'published'
        self.date = str(datetime.today().date())
        self.publishing_date = self.date
        self.origin = ''
        self.flags = ''
        self.elb_id = elb_id
        self.title = self.BookTitle(title.strip())
        
        self.type = 'authorsBook'
        
        if types:
            self.record_types = types
        else: self.record_types = []
            
        if author_name:
            self.authors = [self.BookAuthor(author_id=author_id, author_name=author_name)]
        else: self.authors = []
        
        self.general_materials = 'false'
        
        if languages:
            self.languages = languages
        else: self.languages = []
            
        self.headings = ['f56c40ddce1076f01ab157bed1da7c85']
        
        if linked_ids:
            self.linked_objects = linked_ids
        else: self.linked_objects = []
        
        if publishers:
            self.publishers = [self.BookPublishingHouse(publisher_id=k, publisher_value=v) for k,v in publishers.items()]
        else: self.publishers = []
        
        self.year = self.BookPublicationYear(year=year)
        self.physical_description = physical_description
        
    class XmlRepresentation:
        
        def to_xml(self):
            match self.__class__.__name__:
                case 'BookAuthor':
                    return ET.Element('author', {'id': self.author_id, 'juvenile': self.juvenile, 'co-creator': self.co_creator, 'principal': self.principal})
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

        def __repr__(self):
            return "BookAuthor('{}', '{}')".format(self.author_id, self.author_name)
    
    class BookTitle(XmlRepresentation):
        
        def __init__(self, value):
            self.value = value
            self.code = 'base'
            self.newest = 'true'
            self.transliteration = 'false'
            
        def __repr__(self):
            return "BookTitle('{}')".format(self.value) 
    #dodać później współautorów
        
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
    def from_dict(cls, journal_items_dict):
        return cls(**journal_items_dict)
    
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
    
    def connect_with_institutions(self, publisher_instance, list_of_institutions):
        correct_institution = [e for e in list_of_institutions if [el for el in e.names if el.value == publisher_instance.institution_name]]
        if correct_institution:
            publisher_instance.institution_id = correct_institution[0].id
            
    def connect_publisher(self, list_of_places_class, list_of_institutions_class):
        for publisher in self.publishers:
            self.connect_with_places(publisher, list_of_places_class)
            self.connect_with_institutions(publisher, list_of_institutions_class)
    
    def connect_with_persons(self, list_of_persons):
        for author in self.authors:
            if not author.author_id:
                match_person = [e for e in list_of_persons if author.author_name in [el.value for el in e.names]]
                if match_person:
                    author.author_id = match_person[0].id
    
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
            for heading in self.headings:
                headings_xml.append(ET.Element('heading', {'id': heading}))
            book_xml.append(headings_xml)  
            
        if self.publishers:
            publishing_houses_xml = ET.Element('publishing-houses', {'by-author': 'false'})
            for publisher in self.publishers:
                publishing_houses_xml.append(publisher.to_xml())
            book_xml.append(publishing_houses_xml)
        
        if self.year:
            book_xml.append(self.year.to_xml())
            
        if self.physical_description:
            book_xml.append(ET.Element('physical-description', {'coCreated': 'no', 'description': self.physical_description}))
            
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