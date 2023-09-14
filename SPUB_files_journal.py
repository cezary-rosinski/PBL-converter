import json
from SPUB_additional_functions import get_wikidata_label, give_fake_id, simplify_string
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import xml.etree.cElementTree as ET
from datetime import datetime
import regex as re
from SPUB_files_journal_year import JournalYear
import sys

#%%

# na potrzeby retro anotacja z journal jest przekazywana dalej na year i number, trzeba to ograc inaczej, zeby taka sytuacja nie wystepowala, gdy nie jest potrzebna

class Journal:
    
    def __init__(self, id_='', viaf='', title='', issn='', years_with_numbers_set=None, character='', annotation='', **kwargs):
        # self.id = f"http://www.wikidata.org/entity/Q{id_}"if id_ else None
        self.viaf = f"https://viaf.org/viaf/{viaf}" if viaf else None
        self.creator = 'cezary_rosinski'
        self.status = 'published'
        self.date = str(datetime.today().date())
        self.publishing_date = self.date
        self.headings = []
        self.issn = issn
        self.status = 'source'
        self.removed = 'false'
        self.origin = ''
        self.id = simplify_string(title, with_spaces=True, nodiacritics=True).replace(' ', '-')
        if years_with_numbers_set:
            self.years = [JournalYear(year=y, journal_id=self.id, numbers_set=n, annotation=annotation) for y, n in years_with_numbers_set]
        
        self.titles = [self.JournalTitle(value=title)]
        self.links = []
        # for el in [self.id, self.viaf]:
        #     self.add_journal_link(el)
        
        self.newest_journal_number_id = max(self.years, key=lambda x: int(x.year)).numbers[-1].id
        # <newest-journal-number id="journal-number-id-01"/>
        
        self.annotation = annotation
    
    class XmlRepresentation:
        
        def to_xml(self):
            match self.__class__.__name__:
                case 'JournalTitle':
                    title_xml = ET.Element('title', {'code': self.code, 'lang': self.lang, 'newest': self.newest, 'transliteration': self.transliteration})
                    title_xml.text = self.value
                    return title_xml
                case 'JournalLink':
                    link_xml = ET.Element('link', {'access-date': self.access_date, 'type': self.type})
                    link_xml.text = self.link
                    return link_xml
    
    class JournalTitle(XmlRepresentation):
        
        def __init__(self, value):
            self.value = value
            self.code = 'base'
            self.lang = 'pl'
            self.newest = 'true'
            self.transliteration = 'false'
            
        def __repr__(self):
            return "JournalTitle('{}')".format(self.value)
    
    #newest-journal-number --> zostawiamy/co zrobić?
    
    #publishing-series --> najpierw kartoteka publishing-series
    
    #date --> później
    
    #places --> później
        
    class JournalLink(XmlRepresentation):
        
        def __init__(self, journal_instance, link):
            self.access_date = journal_instance.date
            self.type = 'external-identifier'
            self.link = link
            
        def __repr__(self):
            return "JournalLink('{}', '{}', '{}')".format(self.access_date, self.type, self.link)
    
    @classmethod
    def from_dict(cls, journal_dict):
        title = journal_dict.get('name')
        issn = journal_dict.get('issn')
        years_with_numbers_set = tuple(journal_dict.get('years').items())
        # years = journal_dict.get('years')
        # return cls(title=title, issn=issn, years=years)
        return cls(title=title, issn=issn, years_with_numbers_set=years_with_numbers_set)
    
    def add_journal_link(self, journal_link):
        if journal_link:
            self.links.append(self.JournalLink(journal_instance=self, link=journal_link))
    
    def years_to_xml(self):
        return [e.to_xml() for e in self.years]
    
    def numbers_to_xml(self):
        return [ele for sub in [[el.to_xml() for el in e.numbers] for e in self.years] for ele in sub]

    def to_xml(self):
        journal_dict = {'removed': self.removed, 'id': self.id, 'status': self.status, 'creator': self.creator, 'creation-date': self.date, 'publishing-date': self.publishing_date, 'origin': self.origin}
        journal_xml = ET.Element('journal', journal_dict)
        
        if self.links:
            links_xml = ET.Element('links')
            for link in self.links:
                links_xml.append(link.to_xml())
            journal_xml.append(links_xml)
        
        if self.titles:
            titles_xml = ET.Element('titles', {'without-title': 'false'})
            for title in self.titles:
                titles_xml.append(title.to_xml())
        else: titles_xml = ET.Element('titles', {'without-title': 'true'})
        journal_xml.append(titles_xml)
        
        years_xml = ET.Element('years')
        for year in self.years:
            years_xml.append(ET.Element('year', {'id': year.id}))
        journal_xml.append(years_xml)
        
        headings_xml = ET.Element('headings')
        for heading in self.headings:
            headings_xml.append(ET.Element('heading', {'id': heading}))
        journal_xml.append(headings_xml)
        
        issn_xml = ET.Element('issn')
        issn_xml.text = self.issn
        journal_xml.append(issn_xml)
        
        journal_xml.append(ET.Element('journal-status', {'value': self.status}))
        
        journal_xml.append(ET.Element('newest-journal-number', {'id': self.newest_journal_number_id}))
        
        if self.annotation:
            annotation_xml = ET.Element('annotation')
            annotation_xml.text = self.annotation
            journal_xml.append(annotation_xml)
            
        return journal_xml
    

# journals = [Journal.from_dict(e) for e in data]
# give_fake_id(journals)
# [e.__dict__ for e in journals]

# # test_xml = journals[-3].numbers_to_xml()

# # journals[-3].__dict__
# # .years[0].numbers[0].__dict__
# # __dict__

# journals_xml = ET.Element('pbl')
# files_node = ET.SubElement(journals_xml, 'files')
# journals_node = ET.SubElement(files_node, 'journals')
# journals_years_node = ET.SubElement(files_node, 'journal-years')
# journals_numbers_node = ET.SubElement(files_node, 'journal-numbers')
# for journal in journals:
#     journals_node.append(journal.to_xml())
#     for year_xml in journal.years_to_xml():
#         journals_years_node.append(year_xml)
#     for number_xml in journal.numbers_to_xml():
#         journals_numbers_node.append(number_xml)
    
# tree = ET.ElementTree(journals_xml)

# ET.indent(tree, space="\t", level=0)
# tree.write(f'import_journals_{datetime.today().date()}.xml', encoding='UTF-8')

# # #print tests
# # test_xml = journals[0].to_xml()

# # from xml.dom import minidom
# # xmlstr = minidom.parseString(ET.tostring(test_xml)).toprettyxml(indent="   ")
# print(xmlstr)

#%% schemat
# <journal removed="false"  id="journal-id-01" status="prepared" creator="a_margraf" creation-date="16.11.2022" publishing-date="30.11.2022" origin="IdentyfikatorŹródłaUAM" flags="123" >
# 			
#                 <titles>
#                     <title code="base" transliteration="true" newest="true">Przegląd Literacki</name>
#                     <title code="other" transliteration="true">PL</name>
#                 </titles>
                
#                 <years>
# 					<year id="journal-year-id-01"/>
# 					<year id="journal-year-id-01"/>
# 				</years>
# 				
# 				<newest-journal-number id="journal-number-id-01"/>
# 				
# 				<pub-series id="pub-series-id-01"/>
# 				
# 				<abbreviation>PrzLit</abbreviation>
# 				
#                 <date from="1945-10-01" from-bc="false" uncertain="false" in-words="" name="from"/>
#                 <date from="1989-09-30" from-bc="false" uncertain="false" in-words="" name="to"/>
# 				
# 				<places>
# 					<place id="https://www.wikidata.org/wiki/Q268" period="❦" lang="pl"/>  <!-- Poznań -->
# 					<place id="https://www.wikidata.org/wiki/Q52842" lang="pl"/>  <!-- Kalisz -->
# 					<place id="https://www.wikidata.org/wiki/Q1475264" period="2005-10-01❦" />  <!-- Obra -->
# 				</places>

# 				<issn>1234-abcd</issn>
# 				
# 				
#                 <parent id="journal-id-01" />
#                 <next id="journal-id-02" />

# 				<subject-department value="false"/>
# 			
#                 <!-- list below can be empty - without heading-->
#                 <headings>
#                     <heading id="41c0870f6ba544db8e353b0b4d51876b"/>
#                     <heading id="639fd86bddd0f685ef252cbe0852ea11"/>
#                     <heading id="a35085756f205b365ada4c6907763e87"/>
#                     <heading id="ae7ca28642408ac6485e61fc7ad93a5c"/>
#                     <heading id="67127af726e088fdf1d28b52c9f3043f"/>
#                 </headings>
                
#                 <characters>
# 					<character code="literary" />
# 					<character code="film" />
# 					<character code="theater" />
# 				</characters>
                

#                 <annotation>To jest jakaś adnotacja</annotation>

#                 <remark>To jest jakiś komentarz</remark>


#                 <tags>
#                     <tag>#WazneCzasopismo</tag>
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
# 			
# 			
# 				<available-in-national-library value="true"/>
# 				
# 				<journal-status value="source"/> <!-- source, potential_source, indirect_information, other -->





















