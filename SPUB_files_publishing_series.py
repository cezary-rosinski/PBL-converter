import json
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import regex as re
from SPUB_additional_functions import give_fake_id
from datetime import datetime
import xml.etree.cElementTree as ET

class PublishingSeries:
    
    def __init__(self, id_='', title='', annotation=''):
        self.id = f"http://www.wikidata.org/entity/Q{id_}"if id_ else None
        self.creator = 'cezary_rosinski'
        self.status = 'published'
        self.date = str(datetime.today().date())
        self.publishing_date = self.date
        self.origin = ''
        self.flags = ''
        self.titles = [self.PublishingSeriesTitle(value=title)]
        self.annotation = annotation
        
    class XmlRepresentation:
        
        def to_xml(self):
            match self.__class__.__name__:
                case 'PublishingSeriesTitle':
                    title_xml = ET.Element('title', {'code': self.code, 'newest': self.newest, 'transliteration': self.transliteration})
                    title_xml.text = self.value
                    return title_xml
    
    class PublishingSeriesTitle(XmlRepresentation):
        
        def __init__(self, value):
            self.value = value
            self.code = 'base'
            self.newest = 'true'
            self.transliteration = 'false'
            
        def __repr__(self):
            return "PublishingSeriesTitle('{}')".format(self.value)
        
    @classmethod
    def from_dict(cls, publishing_series_dict):
        title = publishing_series_dict.get('title')
        return cls(title=title)
    
    def to_xml(self):
        publishing_series_dict = {'id': self.id, 'status': self.status, 'creator': self.creator, 'creation-date': self.date, 'publishing-date': self.publishing_date, 'origin': self.origin, 'flags': self.flags}
        publishing_series_dict = {k:v for k,v in publishing_series_dict.items() if v}
        publishing_series_xml = ET.Element('publishing-series', publishing_series_dict)
        
        if self.titles:
            titles_xml = ET.Element('titles')
            for title in self.titles:
                titles_xml.append(title.to_xml())
        publishing_series_xml.append(titles_xml)
        
        if self.annotation:
            annotation_xml = ET.Element('annotation')
            annotation_xml.text = self.annotation
            publishing_series_xml.append(annotation_xml)
        
        return publishing_series_xml
    
# publishing_series_list = [PublishingSeries.from_dict(e) for e in series_data]
# give_fake_id(publishing_series_list)
# [e.__dict__ for e in publishing_series_list]

# #print tests
# test_xml = publishing_series[0].to_xml()

# from xml.dom import minidom
# xmlstr = minidom.parseString(ET.tostring(test_xml)).toprettyxml(indent="   ")
# print(xmlstr)



#%% schemat

# <publishing-series id="creative-work-id-01" status="published" creator="a_margraf" creation-date="2022-12-01" publishing-date="2022-12-03" origin="CW-src-id-01" flags="123">

#                 <titles>
#                     <title code="base" transliteration="false" newest="true">Utwór</title>
#                     <title code="other" transliteration="false">UTW</title>
#                     <title code="other" transliteration="false">U</title>
#                 </titles>


#                 <subtitles>
#                     <subtitle code="base" transliteration="false" newest="true">Podtytuł Utworu</subtitle>
#                 </subtitles>

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


#                 <editors>
#                     <person id="person-id-01" />
#                     <person id="person-id-01" />
#                 </editors>

#                 <annotation>Tutaj należy wpisać widoczny teks adnotacji dla Creative works</annotation>

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
#             </publishing-series>
