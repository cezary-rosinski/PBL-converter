import json
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import xml.etree.cElementTree as ET
from datetime import datetime
from SPUB_additional_functions import give_fake_id, get_wikidata_label


#%% main

class Person:
    
    def __init__(self, id_, viaf, name='', birth_date='', death_date='', birth_place='', death_place=''):
        self.id = f"http://www.wikidata.org/entity/Q{id_}"if id_ else None
        self.viaf = f"https://viaf.org/viaf/{viaf}" if viaf else None
        self.creator = 'cezary_rosinski'
        self.status = 'published'
        self.date = str(datetime.today().date())
        self.publishing_date = self.date
        self.sex = None
        self.headings = ['f56c40ddce1076f01ab157bed1da7c85']
        
        self.names = [self.PersonName(value=name)]
        
        self.birth_date_and_place = self.PersonDateAndPlace(date_from=birth_date, place_id=birth_place) if birth_date else None
        self.death_date_and_place = self.PersonDateAndPlace(date_from=death_date, place_id=death_place) if death_date else None
        
        self.links = []
        for el in [self.id, self.viaf]:
            self.add_person_link(el, 'external-identifier')
    
    class XmlRepresentation:
        
        def to_xml(self):
            match self.__class__.__name__:
                case 'PersonName':
                    name_xml = ET.Element('name', {'transliteration': self.transliteration, 'code': self.code})
                    name_xml.text = self.value
                    return name_xml
                case 'PersonDateAndPlace':
                    date_xml = ET.Element('date', {'from': self.date_from, 'from-bc': self.date_from_bc, 'uncertain': self.date_uncertain})
                    place_dict = {'id': self.place_id, 'period': self.place_period, 'lang': self.place_lang}
                    place_dict = {k:v for k,v in place_dict.items() if v}
                    place_xml = ET.Element('place', place_dict)
                    return date_xml, place_xml
                case 'PersonLink':
                    link_xml = ET.Element('link', {'access-date': self.access_date, 'type': self.type})
                    link_xml.text = self.link
                    return link_xml
    
    class PersonName(XmlRepresentation):
        
        def __init__(self, value):
            self.value = value
            self.transliteration = 'no'
            self.code = 'main-name'
            
        def __repr__(self):
            return "PersonName('{}')".format(self.value)
        
    class PersonDateAndPlace(XmlRepresentation):
        
        def __init__(self, date_from='', date_from_bc='', date_to='', date_to_bc='', date_uncertain='', date_in_words='', place_id='', place_period='', place_lang=''):
            self.date_from = date_from
            self.date_from_bc = 'false' if self.date_from else date_from_bc
            self.date_to = date_to
            self.date_to_bc = date_to_bc
            self.date_uncertain = 'false' if self.date_from else date_uncertain
            self.date_in_words = date_in_words
            self.place_id = f"http://www.wikidata.org/entity/{place_id}" if place_id else None
            self.place_period = place_period
            self.place_lang = place_lang
            #place zależy od rozwoju kartoteki miejsc – dodanie miejsc wiki
            
        def __repr__(self):
            return "PersonDate(date_from='{}', place_id='{}', place_period='{}')".format(self.date_from, self.place_id, self.place_period)
        
    class PersonLink(XmlRepresentation):
        
        def __init__(self, person_instance, link, type_):
            self.access_date = person_instance.date
            self.type = type_
            self.link = link
            
        def __repr__(self):
            return "PersonLink('{}', '{}', '{}')".format(self.access_date, self.type, self.link)
        
    @classmethod
    def from_dict(cls, person_dict):
        id_ = person_dict.get('wiki')
        viaf = person_dict.get('viaf')
        name = person_dict.get('name')
        birth_date = person_dict.get('yearBorn')
        death_date = person_dict.get('yearDeath')
        birth_place = person_dict.get('placeB')
        death_place = person_dict.get('placeD')
        return cls(id_, viaf, name, birth_date, death_date, birth_place, death_place)
    
    def add_person_link(self, person_link, type_):
        if person_link:
            self.links.append(self.PersonLink(person_instance=self, link=person_link, type_=type_))
            
    def connect_with_places(self, list_of_places_class):
        for place in [self.birth_date_and_place, self.death_date_and_place]:
            if place:
                match_place = [e for e in list_of_places_class if place.place_id == e.id]
                if match_place:
                    place.place_period = f'{match_place[0].periods[0].date_from}❦{match_place[0].periods[0].date_to}'
                    place.place_lang = match_place[0].periods[0].lang
                    if 'fake' not in place.place_id:
                        self.add_person_link(place.place_id, 'other')
            
    def to_xml(self):
        person_dict = {k:v for k,v in {'id': self.id, 'status': self.status, 'creator': self.creator, 'creation-date': self.date, 'publishing-date': self.publishing_date, 'viaf': self.viaf}.items() if v}
        person_xml = ET.Element('person', person_dict)
        for element, el_name in zip([self.names, self.links], ['names', 'links']):
            if element:
                higher_node = ET.Element(el_name)
                for node in element:
                    higher_node.append(node.to_xml())
                person_xml.append(higher_node)
        
        if self.sex:
            person_xml.append(ET.Element('sex', {'value': self.sex}))
        if self.birth_date_and_place:
            birth_xml = ET.Element('birth')
            for el in self.birth_date_and_place.to_xml():
                birth_xml.append(el)
            person_xml.append(birth_xml)
        if self.death_date_and_place:
            death_xml = ET.Element('death')
            for el in self.death_date_and_place.to_xml():
                death_xml.append(el)
            person_xml.append(death_xml)
        headings_xml = ET.Element('headings')
        for heading in self.headings:
            headings_xml.append(ET.Element('heading', {'id': heading}))
        person_xml.append(headings_xml)
        return person_xml



# #print tests
# test_xml = persons[0].names[0].to_xml()
# test_xml = persons[0].birth_date.to_xml()
# test_xml = persons[0].death_date.to_xml()
# test_xml = persons[0].links[0].to_xml()
# test_xml = persons[1].to_xml()

# from xml.dom import minidom
# xmlstr = minidom.parseString(ET.tostring(test_xml)).toprettyxml(indent="   ")
# print(xmlstr)

#%% schemat

# <person id="TuJestZewnetrznyId" viaf="13373997" status="published|draft|prepared" createor="c_rosinski" creation-date="2021-05-25" publishing-date="2021-05-25" origin="IdentyfikatorŹródła">
# 				<names>
# 					<!-- codes: main-name, family-name, other-last-name-or-first-name, monastic-name, codename, alias, group-alias, -->
# 					<name code="main-name" transliteration="no" presentation-name="yes">Jan Kowalski</name>
# 					<name code="codename">J.K.</name>
# 					<name code="alias" main-name="yes">Jasiu</name>
# 					<name code="alias">Janek</name>
# 					<!-- ... -->
# 				</names>
# 				<!-- male, female, unknown, null-->
# 				<sex value="male"/>
# 				<!-- list below can be empty - without heading-->
# 				<headings>
# 					<heading id="lit-pol"/>
# 					<heading id="teor-lit"/>
# 					<!-- ... -->
# 				</headings>
# 				<birth>
# 					<date from="2021-05-25" from-bc="True|False" to="" to-bc="True|False" uncertain="True|False" in-words=""/>
# 					<place id="" period="1111" lang=""/>

# 				</birth>
# 				<death>
# 					<date from="2021-05-25" from-bc="True|False" to="" to-bc="True|False" uncertain="True|False" in-words=""/>
# 					<place id="" period="1111" lang=""/>
# 				</death>
# 				<annotation>To jest jakaś adnotacja</annotation>
# 				<remark>To jest jakiś komentarz</remark>
# 				<tags>
# 					<tag>#gwiadkowicz</tag>
# 					<tag>#nicMiNiePrzychodzi</tag>
# 					<!-- ... -->
# 				</tags>
# 				<links>
# 					<link access-date="2021-05-12" type="external-identifier|broader-description-access|online-access">http://pl.wikipedia.org/jan_kowalski</link>
# 					<link access-date="2021-05-18" type="...">http://viaf.org/viaf/13373997/#Kowalski,_Jan_(1930-2018)</link>
# 					<!-- ... -->
# 				</links>
# 			</person>

















