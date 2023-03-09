from datetime import datetime
import xml.etree.cElementTree as ET

class Event:
    
    def __init__(self, id_='', viaf='', name='', year='', place='', type_=''):
        self.id = f"http://www.wikidata.org/entity/Q{id_}"if id_ else None
        self.viaf = f"https://viaf.org/viaf/{viaf}" if viaf else None
        self.creator = 'cezary_rosinski'
        self.status = 'published'
        self.date = str(datetime.today().date())
        self.publishing_date = self.date
        self.origin = ''
        self.headings = ['f56c40ddce1076f01ab157bed1da7c85']
        
        self.names = [self.EventName(value=name)]
        self.type = type_
        
        self.year = year
        self.place = place
        self.date_and_place = self.EventDateAndPlace(date_from=year)
        
    class XmlRepresentation:
        
        def to_xml(self):
            match self.__class__.__name__:
                case 'EventName':
                    name_xml = ET.Element('name', {'code': self.code, 'transliteration': self.transliteration, 'newest': self.newest})
                    name_xml.text = self.value
                    return name_xml
                case 'EventLink':
                    link_xml = ET.Element('link', {'access-date': self.access_date, 'type': self.type})
                    link_xml.text = self.link
                    return link_xml
                case 'EventDateAndPlace':
                    date_and_place_xml = ET.Element('date-and-place')
                    date_and_place_xml.append(ET.Element('date', {'from': self.date_from, 'from-bc': self.date_from_bc, 'date-uncertain': self.date_uncertain}))
                    if self.places:
                        places_xml = ET.SubElement(date_and_place_xml, 'places')
                        for place in self.places:
                            places_xml.append(ET.Element('place', place))
                    return date_and_place_xml
                    
    
    class EventName(XmlRepresentation):
        
        def __init__(self, value):
            self.value = value
            self.code = 'base'
            self.newest = 'true'
            self.transliteration = 'false'
            
        def __repr__(self):
            return "EventName('{}')".format(self.value)
        
    class EventDateAndPlace(XmlRepresentation):
        
        def __init__(self, date_from='', date_from_bc='', date_to='', date_to_bc='', date_uncertain='', date_in_words=''):
            self.date_from = date_from
            self.date_from_bc = 'false' if self.date_from else date_from_bc
            self.date_to = date_to
            self.date_to_bc = date_to_bc
            self.date_uncertain = 'false' if self.date_from else date_uncertain
            self.date_in_words = date_in_words
            self.places = []
                 
            #place zależy od rozwoju kartoteki miejsc – dodanie miejsc wiki
            
        def __repr__(self):
            return "EventDateAndPlace(date_from='{}', date_from_bc='{}', date_uncertain='{}', places='{}')".format(self.date_from, self.date_from_bc, self.date_uncertain, self.places)
        
    class EventLink(XmlRepresentation):
        
        def __init__(self, event_instance, link):
            self.access_date = event_instance.date
            self.type = 'external-identifier'
            self.link = link
            
        def __repr__(self):
            return "EventLink('{}', '{}', '{}')".format(self.access_date, self.type, self.link)
        
        #TUTAJ
        #date and place
        
        #przy większej liczbie danych najpierw musi powstać kartoteka cykli wydarzeń
        
    @classmethod
    def from_dict(cls, event_dict):
        # title = journal_dict.get('name')
        # issn = journal_dict.get('issn')
        # years = journal_dict.get('years')
        # return cls(title=title, issn=issn, years=years)
        return cls(**event_dict)
    
    def connect_with_places(self, list_of_places_class):
        if self.place:
            correct_place = [e for e in list_of_places_class if [el for el in e.periods if el.name == self.place]]
            if correct_place:
                #jeśli dump z eventami będzie miał miejsca wprowadzone w taki sposób jak persons.json, to wtedy zamiast po nazwie, będziemy łączyć kartoteki po identyfikatorze
                #docelowo potrzebna funkcja do wskazywania, w którym periodzie mieści się podana data wydarzenia zamiast hardcodowania indeksu 0
                self.date_and_place.places.append({'id': correct_place[0].id,
                                    'period': f'{correct_place[0].periods[0].date_from}❦{correct_place[0].periods[0].date_to}',
                                    'lang': correct_place[0].periods[0].lang})
                
    def to_xml(self):
        event_dict = {k:v for k,v in {'id': self.id, 'status': self.status, 'creator': self.creator, 'creation-date': self.date, 'publishing-date': self.publishing_date, 'origin': self.origin}.items() if v}
        event_xml = ET.Element('event', event_dict)
        
        event_xml.append(ET.Element('type', {'code': self.type}))
        
        names_xml = ET.Element('names')
        for name in self.names:
            names_xml.append(name.to_xml())
        event_xml.append(names_xml)
        
        event_xml.append(self.date_and_place.to_xml())
        
        headings_xml = ET.Element('headings')
        for heading in self.headings:
            headings_xml.append(ET.Element('heading', {'id': heading}))
        event_xml.append(headings_xml)
        return event_xml

# # schemat XML
#             <event id="https://www.wikidata.org/wiki/XXXXXXX01" status="published" creator="c_rosinski" creation-date="2022-06-21" publishing-date="2022-06-23" origin="IdentyfikatorŹródła">

#                 <!-- honorary-doctorate | festival | conference | competition | prize | decoration | plebiscite | authors-meeting | exhibition-->
#                 <type code="authors-meeting"/>

#                 <names>
#                     <name code="base" transliteration="true" newest="true">Literacka Nagroda Gazety Innej "Wiktoria"</name>
#                 </names>

# 				<date-for-period>
# 					<edition>XXI</edition>
# 					<date from="2021-05-25" from-bc="False" uncertain="False" in-words=""/>
#                 </date-for-period>


#                 <date-and-place>
# 					<date from="2021-05-25" from-bc="False" to="" to-bc="False" uncertain="False" in-words=""/>
# 					<places>
# 						<place id="https://www.wikidata.org/wiki/Q268" period="❦" lang="pl"/>  <!-- Poznań -->
# 						<place id="https://www.wikidata.org/wiki/Q52842" lang="pl"/>  <!-- Kalisz -->
# 						<place id="https://www.wikidata.org/wiki/Q1475264" period="2005-10-01❦" />  <!-- Obra -->
# 					</places>
# 				</date-and-place>


#                 <event-series-list>
#                     <event-series id="id_serii_wydarzeń_1"/>
#                     <event-series id="id_serii_wydarzeń_2"/>
#                 </event-series-list>


#                 <parent id="https://viaf.org/viaf/000000000/">
#                     <date from="1925-10-01" from-bc="false" uncertain="false" in-words="" name="begin"/>
#                     <date from="2015-05-25" from-bc="false" uncertain="false" in-words="" name="end"/>
#                 </parent>


#                 <subordinates>
#                     <subordinate-event id="https://viaf.org/viaf/000000001/"/>
#                     <subordinate-event id="https://viaf.org/viaf/000000002/"/>
#                     <!-- etc -->
#                 </subordinates>


#                 <previous id="https://viaf.org/viaf/000000000/"/>

#                 <next id="https://viaf.org/viaf/000000000/">
#                     <date from="1919-10-01" from-bc="false" uncertain="false" in-words="" name="begin"/>
#                     <date from="2015-05-25" from-bc="false" uncertain="false" in-words="" name="end"/>
#                 </next>


#                 <organizers>
#                     <person id="a0000002977579"/>
#                     <person id="a0000001090958"/>
#                     <person id="a0000001180340"/>
#                     <!-- ... -->

#                     <institution id="https://viaf.org/viaf/000000000/"/>
#                     <institution id="https://viaf.org/viaf/000000001/"/>
#                     <institution id="https://viaf.org/viaf/000000002/"/>
#                     <institution id="https://viaf.org/viaf/000000003/"/>
#                     <!-- ... -->

#                     <journal id="https://viaf.org/viaf/000000000/"/>
#                     <journal id="https://viaf.org/viaf/000000000/"/>
#                     <!-- ... -->
#                 </organizers>

#                 <!-- list below can be empty - without heading-->
#                 <headings>
#                     <heading id="7320f7ca8352d748b386ab4e4913e3ee"/>
#                     <heading id="a31385e80345e59e06b208e998bcaeab"/>
#                     <heading id="e99e5257f8af377ba21568d1fb73e368"/>
#                 </headings>

#                 <annotation>To jest tekst o @{"Kazimierzu-Przerwa Tetmajerze" | person_id="przerwa_id"} oraz o jego przyjacielu @{"Franku Kowalskim" | person_id="franciszek_kowalski_id"}.Pewnie można by tu jeszcze coś uzupełnić o @{"Oli Sroce"|person_id="ola_sroka_id"}. Ich firmat to @{"Firemka sp. z o.o."|institution_id="firemka_id"} właśnie.</annotation>

#                 <remark>Tutaj można wpisać tekst komentarza. Komentarze są niewidoczne na stronie</remark>


#                 <tags>
#                     <tag>#nike</tag>
#                     <tag>#nicMiNiePrzychodzi</tag>
#                     <!-- ... -->
#                 </tags>

#                 <links>
#                     <link access-date="12.05.2021" type="external-identifier">http://pbl.poznan.pl/</link>
#                     <link access-date="18.05.2021" type="other">http://ibl.pbl.waw.pl/</link>
#                     <link access-date="12.05.2021" type="broader-description-access">http://pbl.poznan.pl/</link>
#                     <link access-date="12.05.2021" type="online-access">http://pbl.poznan.pl/</link>
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

#                     <event-series-ref id="e-id-01"/>
#                     <!-- ... -->

#                     <place-ref id="p-id-01" period="" lang=""/>
#                     <!-- ... -->

#                     <journal-source-ref id="js-id"/>
#                     <!-- ... -->

#                     <publishing-series-ref id="ps-id"/>
#                     <!-- ... -->

#                 </linked-objects>

#                 <awards>
#                     <award id="" description="dfsfd @{ID_MICKIEWICZA|Adam Mickiewicz}">
#                         <record-ref id=""/>
#                         <!-- ... -->
#                         <creative-work-ref id=""/>
#                         <!-- ... -->
#                     </award>

#                     <!-- ... -->
#                 </awards>