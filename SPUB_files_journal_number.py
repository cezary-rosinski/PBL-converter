import xml.etree.cElementTree as ET
from datetime import datetime

#%% main
class JournalNumber:
    
    def __init__(self, number, journal_year_id=''):
        self.number = number
        self.removed = 'false'
        self.origin = ''
        self.journal_year_id = journal_year_id
        self.id = f'{self.journal_year_id}_{self.number}'.replace(' ', '-')
        self.headings = ['f56c40ddce1076f01ab157bed1da7c85']
        self.links = []
        
    class XmlRepresentation:
        
        def to_xml(self):
            match self.__class__.__name__:
                case 'JournalNumberLink':
                    link_xml = ET.Element('link', {'access-date': self.access_date, 'type': self.type})
                    link_xml.text = self.link
                    return link_xml
                
    class JournalNumberLink(XmlRepresentation):
        
        def __init__(self, journal_number_instance, link):
            self.access_date = str(datetime.today().date())
            self.type = 'external-identifier'
            self.link = link
            
        def __repr__(self):
            return "JournalNumberLink('{}', '{}', '{}')".format(self.access_date, self.type, self.link)
    
    def __repr__(self):
        return "JournalNumber('{}', '{}')".format(self.number, self.journal_year_id)
    
    def add_journal_number_link(self, journal_number_link):
        if journal_number_link:
            self.links.append(self.JournalLink(journal_number_instance=self, link=journal_number_link))
                
    def to_xml(self):
        journal_number_dict = {k:v for k,v in {'removed': self.removed, 'id': self.id, 'origin': self.origin}.items() if v}
        journal_number_xml = ET.Element('journal-number', journal_number_dict)
        
        if self.links:
            links_xml = ET.Element('links')
            for link in self.links:
                links_xml.append(link.to_xml())
            journal_number_xml.append(links_xml)
        
        headings_xml = ET.Element('headings')
        for heading in self.headings:
            headings_xml.append(ET.Element('heading', {'id': heading}))
        journal_number_xml.append(headings_xml)
        
        journal_number_xml.append(ET.Element('journal-year', {'id': self.journal_year_id}))
        
        number_xml = ET.Element('number')
        number_xml.text = self.number
        journal_number_xml.append(number_xml)
        
        return journal_number_xml



# #print tests
# test = JournalNumber('47', 'polityka_2002')
# test.__dict__

# test_xml = test.to_xml()
# from xml.dom import minidom
# xmlstr = minidom.parseString(ET.tostring(test_xml)).toprettyxml(indent="   ")
# print(xmlstr)

#%% schemat

# <journal-number removed="false"  id="journal-number-id-01" origin="IdentyfikatorŹródłaUAM">
# 				<journal-year id="journal-year-id-01" />
# 					
# 				<series-number>12345</series-number>
# 				<sub-series-number>67890</sub-series-number>
# 				
# 				<number>XIV</number>

#                 <sub-titles>
#                     <sub-title code="base" transliteration="true" newest="true">Przegląd Literacki</sub-title>
#                     <sub-title code="other" transliteration="true">PL</sub-title>
#                 </sub-titles>
# 					
# 					
# 				<as-a-book value="true"/>
# 				<book id="book-id-01"/>
# 				
# 				
# 				<edition>Edycja numeru czasopisma - pewnie jakiś numer</edition>
# 				
# 				<trigger-institutions>
# 					<institution id="institution-id-01"/>
# 					<institution id="https://viaf.org/viaf/000000000/"/> <!-- UP -->
# 				</trigger-institutions>
# 				
# 				
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
# 			

#                 <!-- list below can be empty - without heading-->
#                 <headings>
#                     <heading id="41c0870f6ba544db8e353b0b4d51876b"/>
#                     <heading id="639fd86bddd0f685ef252cbe0852ea11"/>
#                     <heading id="a35085756f205b365ada4c6907763e87"/>
#                     <heading id="ae7ca28642408ac6485e61fc7ad93a5c"/>
#                     <heading id="67127af726e088fdf1d28b52c9f3043f"/>
#                 </headings>
# 				


#                 <annotation>To jest jakaś adnotacja</annotation>

#                 <remark>To jest jakiś komentarz</remark>


#                 <tags>
#                     <tag>#WaznyRocznikCzasopisma</tag>
#                     <tag>#nicMiNiePrzychodzi</tag>
#                     <!-- ... -->
#                 </tags>

# 				<availability>
# 					<library id="https://viaf.org/viaf/000000001/" signature="123/45"/> <!-- BU -->
# 					<library id="institution-id-01" signature="124/56"/>					
# 				</availability>

#                 <links>
#                     <link access-date="12.05.2021" type="external-identifier">http://pbl.poznan.pl/</link>
#                     <link access-date="18.05.2021" type="broader-description-access">http://ibl.pbl.waw.pl/</link>
#                     <link access-date="18.05.2021" type="online-access">http://ibl.pbl.waw.pl/</link>
#                 </links>
                
# 				<publication-frequency code="daily" />
# 				
# 				<physical-deficiencies>Deficyty fizyczne</physical-deficiencies>
# 				
# 				
# 				<journal-number-status value="development-of-sources"/> <!-- inability-to-develop, outside-order-development, development-on-delegation, development-of-sources -->
# 				
# 				<resignation-reason-public>Ble ble ble ale publiczne</resignation-reason-public>
# 				<resignation-reason-private>Pismo jest do dudy nie ma co opracowywać</resignation-reason-private>
# 				
# 			
# 			</journal-number>
















