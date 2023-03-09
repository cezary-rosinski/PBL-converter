import json
from datetime import datetime
from SPUB_additional_functions import give_fake_id
import xml.etree.cElementTree as ET


class CreativeWork:
    
    def __init__(self, id_='', author_id='', author_name='', title=''):
        self.id = f"http://www.wikidata.org/entity/Q{id_}"if id_ else None
        self.creator = 'cezary_rosinski'
        self.status = 'published'
        self.date = str(datetime.today().date())
        self.publishing_date = self.date
        self.origin = ''
        self.flags = ''
        
        self.author_id = author_id
        self.title = title
        
        self.authors = [self.CreativeWorkAuthor(author_id=author_id, author_name=author_name)]
        self.titles = [self.CreativeWorkTitle(value=title)]
        
        self.headings = ['f56c40ddce1076f01ab157bed1da7c85']
        
    class XmlRepresentation:
        
        def to_xml(self):
            match self.__class__.__name__:
                case 'CreativeWorkAuthor':
                    return ET.Element('author', {'id': self.author_id, 'juvenile': self.juvenile, 'co-creator': self.co_creator, 'principal': self.principal})
                case 'CreativeWorkTitle':
                    title_xml = ET.Element('title', {'code': self.code, 'transliteration': self.transliteration, 'newest': self.newest})
                    title_xml.text = self.value
                    return title_xml
    
    class CreativeWorkAuthor(XmlRepresentation):
        
        def __init__(self, author_id, author_name):
            self.author_id = f"http://www.wikidata.org/entity/Q{author_id}"if author_id else None
            self.juvenile = 'false'
            self.co_creator = 'false'
            self.principal = 'true'
            self.author_name = author_name

        def __repr__(self):
            return "CreativeWorkAuthor('{}', '{}')".format(self.author_id, self.author_name)
    
    class CreativeWorkTitle(XmlRepresentation):
        
        def __init__(self, value):
            self.value = value
            self.code = 'base'
            self.newest = 'true'
            self.transliteration = 'false'
            
        def __repr__(self):
            return "CreativeWorkTitle('{}')".format(self.value)
        
    @classmethod
    def from_dict(cls, creative_work_dict):
        author_name = creative_work_dict.get('name')
        author_id = creative_work_dict.get('wiki')
        title = creative_work_dict.get('title')
        return cls(author_id=author_id, author_name=author_name, title=title)
    
    def connect_with_persons(self, list_of_persons_class):
        for author in self.authors:
            if not author.author_id:
                match_person = [e for e in list_of_persons_class if author.author_name in [el.value for el in e.names]]
                if match_person:
                    author.author_id = match_person[0].id    
                    
    def to_xml(self):
        creative_work_dict = {k:v for k,v in {'id': self.id, 'status': self.status, 'creator': self.creator, 'creation-date': self.date, 'publishing-date': self.publishing_date, 'origin': self.origin, 'flags': self.flags}.items() if v}
        creative_work_xml = ET.Element('creative-work', creative_work_dict)
        
        
        titles_xml = ET.Element('titles')
        for title in self.titles:
            titles_xml.append(title.to_xml()) 
        creative_work_xml.append(titles_xml)
        
        if self.authors:
            authors_xml = ET.Element('authors', {'anonymous': 'false', 'author-company': 'false'})
            for author in self.authors:
                authors_xml.append(author.to_xml())
        else: authors_xml = ET.Element('authors', {'anonymous': 'true', 'author-company': 'false'})
        creative_work_xml.append(authors_xml)
        
        headings_xml = ET.Element('headings')
        for heading in self.headings:
            headings_xml.append(ET.Element('heading', {'id': heading}))
        creative_work_xml.append(headings_xml)
        
        return creative_work_xml


    
# #print tests
# test_xml = creative_works[0].to_xml()

# from xml.dom import minidom
# xmlstr = minidom.parseString(ET.tostring(test_xml)).toprettyxml(indent="   ")
# print(xmlstr)



#%% schemat
# <creative-work id="creative-work-id-01" status="published" creator="a_margraf" creation-date="2022-12-01" publishing-date="2022-12-03" origin="CW-src-id-01" flags="123">

#                 <authors anonymous="false" author-company="false">
#                     <author id="a0000001758844" juvenile="false" co-creator="true" principal="true"/>
#                     <author id="person-id-02" juvenile="false" co-creator="true" />
#                     <author id="person-id-03" juvenile="true" co-creator="false" />
#                 </authors>

#                 <titles>
#                     <title code="base" transliteration="false" newest="true">Utwór o @{"Kazimierzu-Przerwa Tetmajerze"|person_id="przerwa_id"}</title>
#                     <title code="other" transliteration="false">UTW</title>
#                     <title code="other" transliteration="true">Τεϒμαιερ</title>
#                 </titles>

#                 <headings>
#                     <heading id="7320f7ca8352d748b386ab4e4913e3ee"/>
#                     <heading id="a31385e80345e59e06b208e998bcaeab"/>
#                     <heading id="e99e5257f8af377ba21568d1fb73e368"/>
#                 </headings>

#                 <annotation>To jest tekst o @{"Kazimierzu-Przerwa Tetmajerze"|person_id="przerwa_id"} oraz o jego przyjacielu @{"Franku Kowalskim"|person_id="franciszek_kowalski_id"}. Pewnie możnaby tu jeszcze coś uzupełnić o @{"Oli Sroce"|person_id="ola_sroka_id"}. Ich firmat to @{"Firemka sp. z o.o."|institution_id="firemka_id"} właśnie.</annotation>

#                 <remark>Tutaj można wpisać tekst komentarza dla Creative works. Komentarze są niewidoczne na stronie</remark>


#                 <tags>
#                     <tag>#cw-nike</tag>
#                     <tag>#cw-nicMiNiePrzychodzi</tag>
#                     <!-- ... -->
#                 </tags>

#                 <links>
#                     <link access-date="12.05.2021" type="external-identifier">http://pbl.poznan.pl/</link>
#                     <link access-date="18.05.2021" type="broader-description-access">http://ibl.pbl.waw.pl/</link>
#                     <!-- ... -->
#                 </links>



#             </creative-work>