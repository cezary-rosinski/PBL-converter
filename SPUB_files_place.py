import xml.etree.cElementTree as ET
from datetime import datetime

#%% classes
    
class Place:
    
    def __init__(self, id_, lat, lon, geonames='', name=''):
        self.id = f"http://www.wikidata.org/entity/Q{id_}"
        self.lat = lat
        self.lon = lon
        self.geonames = geonames
        
        self.periods = [self.PlacePeriod(name=name)]
        
    class PlacePeriod:
        
        def __init__(self, date_from='', date_to='', name='', country='', lang='pl'):
            self.date_from = date_from
            self.date_to = date_to
            self.name = name
            self.country = country
            self.lang = lang    
            
        def __repr__(self):
            return "PlacePeriod('{}', '{}', '{}', '{}', '{}')".format(self.date_from, self.date_to, self.name, self.country, self.lang)
        
        def to_xml(self):
            period_xml = ET.Element('period', {'date-from': self.date_from, 'date-to': self.date_to})
            for attr_tag, value in zip(['name', 'country'], [self.name, self.country]):
                if value:
                    ET.SubElement(period_xml, attr_tag, {'lang': self.lang}).text = value
            return period_xml
            
    @classmethod
    def from_dict(cls, place_dict):
        id_ = place_dict.get('wiki')
        lat, lon = place_dict.get('coordinates').split(',') if place_dict.get('coordinates') else ['', '']
        name = place_dict.get('name')
        return cls(id_, lat, lon, name=name)
    
    def to_xml(self):
        place_xml = ET.Element('place', {k:v for k,v in self.__dict__.items() if k != 'periods' and v})
        for period in self.periods:
            place_xml.append(period.to_xml())
        return place_xml

# # schemat XML
# <place geonames="3088171" lon="16.92993" lat="52.40692" id="https://www.wikidata.org/wiki/Q268">
#                 <period date-from="" date-to="">
#                     <name lang="pl">Poznań</name>
#                     <country lang="pl">Polska</country>
#                 </period>
#             </place>






























