import json
from SPUB_additional_functions import get_wikidata_label, give_fake_id
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import xml.etree.cElementTree as ET
from datetime import datetime
import regex as re

from SPUB_additional_functions import get_wikidata_label, get_wikidata_coordinates, simplify_string, marc_parser_for_field, parse_mrk, parse_java, get_number


#%% main

class Institution:
    
    def __init__(self, id_, viaf, name=''):
        self.id = f"http://www.wikidata.org/entity/Q{id_}"if id_ else None
        self.viaf = f"https://viaf.org/viaf/{viaf}" if viaf else None
        self.creator = 'cezary_rosinski'
        self.status = 'published'
        self.date = str(datetime.today().date())
        self.publishing_date = self.date
        self.headings = ['f56c40ddce1076f01ab157bed1da7c85']
        self.names = [self.InstitutionName(value=name)]
        self.links = []
        for el in [self.id, self.viaf]:
            self.add_institution_link(el)
        self.newest_name = [e for e in self.names if e.newest == 'true'][0].value
        self.removed = 'false'
    
    class XmlRepresentation:
        
        def to_xml(self):
            match self.__class__.__name__:
                case 'InstitutionName':
                    name_xml = ET.Element('name', {'type': self.type, 'newest': self.newest})
                    name_xml.text = self.value
                    return name_xml
                case 'InstitutionLink':
                    link_xml = ET.Element('link', {'access-date': self.access_date, 'type': self.type})
                    link_xml.text = self.link
                    return link_xml
        
    class InstitutionName(XmlRepresentation):
        
        def __init__(self, value):
            self.value = value
            self.type = 'base'
            self.newest = 'true'
            
        def __repr__(self):
            return "InstitutionName('{}')".format(self.value)
    
    class InstitutionLink(XmlRepresentation):
        
        def __init__(self, institution_instance, link):
            self.access_date = institution_instance.date
            self.type = 'external-identifier'
            self.link = link
            
        def __repr__(self):
            return "InstitutionLink('{}', '{}', '{}')".format(self.access_date, self.type, self.link)    
    
    @classmethod
    def from_dict(cls, institution_dict):
        id_ = institution_dict.get('wiki')
        viaf = institution_dict.get('viaf')
        name = institution_dict.get('name')
        return cls(id_, viaf, name)
    
    def add_institution_link(self, institution_link):
        if institution_link:
            self.links.append(self.InstitutionLink(institution_instance=self, link=institution_link))
            
    def to_xml(self):
        institution_dict = {k:v for k,v in {'id': self.id, 'status': self.status, 'creator': self.creator, 'creation-date': self.date, 'publishing-date': self.publishing_date, 'viaf': self.viaf, 'removed': self.removed}.items() if v}
        institution_xml = ET.Element('institution', institution_dict)
        
        if self.links:
            links_xml = ET.Element('links')
            for link in self.links:
                links_xml.append(link.to_xml())
            institution_xml.append(links_xml)
            
        history_xml = ET.Element('history')
        period_node = ET.SubElement(history_xml, 'period')
        names_node = ET.SubElement(period_node, 'names')
        for name in self.names:
            names_node.append(name.to_xml())
        institution_xml.append(history_xml)
        
        headings_xml = ET.Element('headings')
        for heading in self.headings:
            headings_xml.append(ET.Element('heading', {'id': heading}))
        institution_xml.append(headings_xml)
        newest_name_xml = ET.Element('newest_name')
        newest_name_xml.text = self.newest_name
        institution_xml.append(newest_name_xml)
        return institution_xml
        
# institutions = [Institution.from_dict(e) for e in data]
# give_fake_id(institutions)
# [e.__dict__ for e in institutions]

# institutions_xml = ET.Element('pbl')
# files_node = ET.SubElement(institutions_xml, 'files')
# institutions_node = ET.SubElement(files_node, 'institutions')
# for institution in institutions:
#     institutions_node.append(institution.to_xml())

# tree = ET.ElementTree(institutions_xml)

# ET.indent(tree, space="\t", level=0)
# tree.write(f'import_institutions_{datetime.today().date()}.xml', encoding='UTF-8')

# #print tests
# test_xml = institutions[0].names[0].to_xml()
# test_xml = institutions[0].links[0].to_xml()
# test_xml = institutions[1].to_xml()

# from xml.dom import minidom
# xmlstr = minidom.parseString(ET.tostring(test_xml)).toprettyxml(indent="   ")
# print(xmlstr)

#kolejne kroki:
    #type

#%% schemat
# <institution id="TuJestZewnetrznyId" status="published|draft|prepared" createor="c_rosinski" creation-date="01.01.2021" publishing-date="01.01.2021" origin="IdentyfikatorŹródła">
#                 <types>
#                     <!-- main-codes: polish, polish-abroad, foreign -->
#                     <!-- codes:
#                         library, foundation, literary-group, cultural-institution, non-university-institute, institute,
#                         public-administration-unit, museum, theatre, university, publishing-house, film-company,
#                         creative-association, no-data
#                      -->
#                     <type main-code="polish" code="library"/>
#                     <type main-code="polish-abroad" code="foundation"/>
#                     <type main-code="foreign" code="literary-group"/>
#                     <type main-code="polish" code="literary-group"/>
#                     <!-- ... -->
#                 </types>

# <!--
#                 <newest-name>Intytut Najnowszej Nazwy PAN</newest-name>
# -->

#                 <history>
#                     <period>
#                         <date from="2021-05-25" from-bc="True|False" to="" to-bc="True|False" uncertain="True|False" in-words=""/>
#                         <names>
#                             <!-- types: base, other,  -->
#                             <name type="base">INN PAN</name>
#                             <name type="other" newest="true">Intytut Najnowszej Nazwy PAN</name>
#                         </names>

#                         <places>

#                             <place geonames="XXX"  period="" lang=""/>

#                             <place id="lwow" period="" lang=""/>
#                         </places>
#                     </period>

#                     <period><!-- ... --></period>
#                     <!-- ... -->
#                 </history>

#                 <parnet id="idInstutucjiNadrzędnej">
                   
# 					<date from="2021-05-25" from-bc="True|False" to="" to-bc="True|False" uncertain="True|False" in-words="" name="begin"/>
                    
#                     <date from="2021-05-25" from-bc="True|False" to="" to-bc="True|False" uncertain="True|False" in-words="" name="end"/>
#                 </parnet>


#                 <subordinates>
#                     <subordinate-institution id="instytucja_podległa_1"/>
#                     <subordinate-institution id="instytucja_podległa_2"/>
#                     <!-- ... -->
#                 </subordinates>

#                 <prevoius id="xxx"/>

#                 <next id="xxx">
#                     <date from="2021-05-25" from-bc="True|False" to="" to-bc="True|False" uncertain="True|False" in-words="" name="begin"/>
#                     <date from="2021-05-25" from-bc="True|False" to="" to-bc="True|False" uncertain="True|False" in-words="" name="end"/>
#                 </next>

#                 <!-- list below can be empty - without heading-->
#                 <headings>
#                     <heading id="lit-pol"/>
#                     <heading id="teor-lit"/>
#                     <!-- ... -->
#                 </headings>

#                 <annotation>To jest jakaś adnotacja</annotation>

#                 <remark>To jest jakiś komentarz</remark>


#                 <tags>
#                     <tag>#WaznyInstytut</tag>
#                     <tag>#nicMiNiePrzychodzi</tag>
#                     <!-- ... -->
#                 </tags>

#                 <links>
#                     <link access-date="12.05.2021" type="external-identifier|broader-description-access|online-access">http://pbl.poznan.pl/</link>
#                     <link access-date="18.05.2021" type="...">http://ibl.pbl.waw.pl/</link>
#                     <!-- ... -->
#                 </links>

#             </institution>