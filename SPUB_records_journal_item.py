import json
import regex as re
from collections import ChainMap
from datetime import datetime
import xml.etree.cElementTree as ET

#%%
class JournalItem:
    
    def __init__(self, id_, title='', types=None, author_id='', authors='', languages=None, linked_ids=None, elb_id=None, journal_str='', journal_year_str='', journal_number_str='', pages='', **kwargs):
        self.id = f"http://www.wikidata.org/entity/Q{id_}"if id_ else None
        self.creator = 'cezary_rosinski'
        self.status = 'published'
        self.date = str(datetime.today().date())
        self.publishing_date = self.date
        self.origin = ''
        self.flags = ''
        self.elb_id = elb_id
        self.title = self.JournalItemTitle(title.strip())
        
        if types:
            self.record_types = types
        else: self.record_types = []
            
        if authors:
            if isinstance(authors, str):
                authors = [author_name]
            self.authors = [self.JournalItemAuthor(author_id=author_id, author_name=author_name) for author_name in authors]
        else: self.authors = []
        
        self.general_materials = 'false'
        
        if languages:
            self.languages = languages
        else: self.languages = []
            
        self.headings = ['f56c40ddce1076f01ab157bed1da7c85']
        
        if linked_ids:
            self.linked_objects = linked_ids
        else: self.linked_objects = []
        
        if journal_str and journal_number_str and journal_year_str:
            self.sources = [self.JournalItemSource(journal_str, journal_year_str, journal_number_str, pages=pages)]
        else:
           self.sources = [] 
        
    class XmlRepresentation:
        
        def to_xml(self):
            match self.__class__.__name__:
                case 'JournalItemAuthor':
                    return ET.Element('author', {'id': self.author_id, 'juvenile': self.juvenile, 'co-creator': self.co_creator, 'principal': self.principal})
                case 'JournalItemTitle':
                    title_xml = ET.Element('title', {'code': self.code, 'transliteration': self.transliteration, 'newest': self.newest})
                    title_xml.text = self.value
                    return title_xml
                case 'JournalItemSource':
                    source_xml = ET.Element('journal-source')
                    if self.journal_number_id:
                        source_xml.append(ET.Element('journal-number', {'id': self.journal_number_id}))
                    if self.journal_year_id:
                        source_xml.append(ET.Element('journal-year', {'id': self.journal_year_id}))
                    if self.journal_id:
                        source_xml.append(ET.Element('journal', {'id': self.journal_id}))
                    if self.pages:
                        pages_xml = ET.Element('pages')
                        pages_xml.text = self.pages
                        source_xml.append(pages_xml)
                    return source_xml
                case 'JournalItemLinkedObejct':
                    pass
    
    class JournalItemAuthor(XmlRepresentation):
        
        def __init__(self, author_id, author_name):
            self.author_id = f"http://www.wikidata.org/entity/Q{author_id}" if author_id else ''
            self.juvenile = 'false'
            self.co_creator = 'false'
            self.principal = 'true'
            self.author_name = author_name

        def __repr__(self):
            return "JournalItemAuthor('{}', '{}')".format(self.author_id, self.author_name)
    
    class JournalItemTitle(XmlRepresentation):
        
        def __init__(self, value):
            self.value = value
            self.code = 'base'
            self.newest = 'true'
            self.transliteration = 'false'
            
        def __repr__(self):
            return "JournalItemTitle('{}')".format(self.value) 
    #dodać później współautorów
    
    class JournalItemSource(XmlRepresentation):
        
        def __init__(self, journal_str ='', journal_year_str='', journal_number_str='', journal_id='', journal_year_id='', journal_number_id='', pages=''):
            self.journal_str = journal_str
            self.journal_year_str = journal_year_str
            self.journal_number_str = journal_number_str
            
            self.journal_id = journal_id
            self.journal_year_id = journal_year_id
            self.journal_number_id = journal_number_id
            
            self.pages = pages
            
        def __repr__(self):
            return "JournalItemSource('{}', '{}', '{}', '{}', '{}', '{}', '{}')".format(self.journal_str, self.journal_year_str, self.journal_number_str, self.journal_id, self.journal_year_id, self.journal_number_id, self.pages) 
        
    class JournalItemLinkedObejct(XmlRepresentation):
        pass
    
    @classmethod
    def from_dict(cls, journal_items_dict):
        return cls(**journal_items_dict)
    
    @classmethod
    def from_retro(cls, retro_journal_items_dict):
        return cls(**retro_journal_items_dict)
                    
    def connect_with_persons(self, persons_to_connect):
        for author in self.authors:
            if not author.author_id:
                match_person = persons_to_connect.get(author.author_name)
                if match_person:
                    author.author_id = match_person
    
    def connect_with_journals(self, journals_to_connect):
        for source in self.sources:
            if not source.journal_id:
                match_journal = journals_to_connect.get(source.journal_str)
                if match_journal:
                    source.journal_id = match_journal.id
                    match_year = [e for e in match_journal.years if e.year == source.journal_year_str]
                    if match_year:
                       source.journal_year_id = match_year[0].id
                       match_number = [e for e in match_year[0].numbers if e.number == source.journal_number_str]
                       if match_number:
                           source.journal_number_id = match_number[0].id
    
    def to_xml(self):
        journal_item_dict = {k:v for k,v in {'id': self.id, 'status': self.status, 'creator': self.creator, 'creation-date': self.date, 'publishing-date': self.publishing_date, 'origin': self.origin, 'flags': self.flags}.items() if v}
        journal_item_xml = ET.Element('journal-item', journal_item_dict)
        
        if self.record_types:
            record_types_xml = ET.Element('record-types')
            for rec_type in self.record_types:
                record_types_xml.append(ET.Element('record-type', {'code': rec_type}))
            journal_item_xml.append(record_types_xml)    
        
        journal_item_xml.append(ET.Element('general-materials', {'value': 'true'}))
        
        if self.authors:
            authors_xml = ET.Element('authors', {'anonymous': 'false', 'author-company': 'false'})
            for author in self.authors:
                authors_xml.append(author.to_xml())
        else:
            authors_xml = ET.Element('authors', {'anonymous': 'true', 'author-company': 'false'})
        journal_item_xml.append(authors_xml)
        
        if self.title:
            titles_xml = ET.Element('titles')
            titles_xml.append(self.title.to_xml())
            journal_item_xml.append(titles_xml)
            
        if self.languages:
            languages_xml = ET.Element('languages')
            for lang in self.languages:
                languages_xml.append(ET.Element('language', {'code': lang}))
            journal_item_xml.append(languages_xml)
        
        if self.headings:
            headings_xml = ET.Element('headings')
            for heading in self.headings:
                headings_xml.append(ET.Element('heading', {'id': heading}))
            journal_item_xml.append(headings_xml)  
            
        if self.sources:
            source_origin_xml = ET.Element('source-origin')
            for source in self.sources:
                source_origin_xml.append(source.to_xml())
            journal_item_xml.append(source_origin_xml)
        
        return journal_item_xml
    
#%%

# test = [JournalItem.from_dict(e) for e in journal_items_data]
# test[1].__dict__
# test[1].connect_with_journals(journals)
# test_xml = test[1].to_xml()
# from xml.dom import minidom
# xmlstr = minidom.parseString(ET.tostring(test_xml)).toprettyxml(indent="   ")
# print(xmlstr)

#%% schemat XML

# <journal-item id="journal-item-id-01" status="published" creator="a_margraf" creation-date="2022-12-01" publishing-date="2022-12-03" origin="CW-src-id-01" flags="123">

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
#                     <title code="incipit" transliteration="true">PC</title>
#                 </titles>

#                 <languages>
#                     <language code="pl"/>
#                 </languages>

#                 <anthology-description>Opis antologii</anthology-description>

#                 <!--
#                 <anthology-descriptions>

#                 </anthology-descriptions>
#                 -->



#                 <mailings>
#                     <mailing>
#                         <sender>
#                             <author id="person-id-11" juvenile="false" co-creator="true" />
#                             <author id="person-id-12" juvenile="false" co-creator="true" />
#                             <author id="person-id-13" juvenile="true" co-creator="false" />

#                             <institution id="institution-id-11" />
#                             <institution id="institution-id-12" />
#                         </sender>

#                         <addressee>
#                             <author id="person-id-14" juvenile="false" co-creator="true" />

#                             <institution id="institution-id-13" />
#                             <institution id="institution-id-14" />
#                             <institution id="institution-id-15" />
#                         </addressee>
#                         <description>Opis pierwszego mailingu</description>
#                     </mailing>

#                     <mailing>
#                         <sender>
#                             <author id="person-id-21" juvenile="false" co-creator="true" />
#                         </sender>
#                         <addressee>
#                             <institution id="institution-id-22" />
#                         </addressee>
#                         <description>Opis drugiego mailingu</description>
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
#                     <!-- ... -->
#                     <place-ref id="p-id-01" period="" lang=""/>
#                     <!-- ... -->
#                     <journal-source-ref id="js-id"/>
#                     <!-- ... -->
#                 </linked-objects>
