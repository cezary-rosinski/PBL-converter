import xml.etree.cElementTree as ET
from datetime import datetime

#%% 
from datetime import datetime


def parse_date(date_str, default):
    if not date_str:
        return default
    return datetime.fromisoformat(date_str)


def get_country_for_period(date_from, date_to, countries):
    """
    Zwraca nazwę kraju, którego okres obowiązywania
    przecina się z okresem nazwy.
    """

    period_from = parse_date(date_from, datetime.min)
    period_to = parse_date(date_to, datetime.max)

    for country in countries:
        country_from = parse_date(country.get("start_time"), datetime.min)
        country_to = parse_date(country.get("end_time"), datetime.max)

        # przedziały się przecinają
        if period_from <= country_to and country_from <= period_to:
            return country["country"]

    return ""


def get_current_country(countries):
    """Zwraca aktualnie obowiązujący kraj."""
    for country in countries:
        if country.get("end_time") is None:
            return country["country"]

    # awaryjnie pierwszy
    return countries[0]["country"] if countries else ""

#%% classes
    
class Place:
    
    def __init__(self, id_, lat, lon, geonames='', name='', annotation='', **kwargs):
        self.id = f"http://www.wikidata.org/entity/Q{id_}"
        self.lat = lat
        self.lon = lon
        self.geonames = geonames
        
        self.periods = [self.PlacePeriod(name=name)]
        
        self.annotation = annotation
        
    class PlacePeriod:
        
        def __init__(self, date_from='', date_to='', name='', country='', country_lang='', lang='pl'):
            self.date_from = date_from
            self.date_to = date_to
            self.name = name
            self.country = country
            self.country_lang = country_lang
            self.lang = lang    
            
        def __repr__(self):
            return "PlacePeriod('{}', '{}', '{}', '{}', '{}')".format(self.date_from, self.date_to, self.name, self.country, self.lang)
        
        def to_xml(self):
            period_xml = ET.Element('period', {'date-from': self.date_from, 'date-to': self.date_to})
            for attr_tag, value in zip(['name', 'country'], [(self.name, self.lang), (self.country, self.country_lang)]):
                if value[0] and value[1]:
                    ET.SubElement(period_xml, attr_tag, {'lang': value[1]}).text = value[0]
            return period_xml
            
    @classmethod
    def from_dict(cls, place_dict):
        wiki = place_dict.get("wiki", "")
        wiki_data = place_dict.get("fromWiki", {})

        id_ = wiki.lstrip("Q")

        coordinates = wiki_data.get("coordinates", {})
        lat = str(coordinates.get("latitude", ""))
        lon = str(coordinates.get("longitude", ""))

        geonames = wiki_data.get("geonames_id", "")

        place = cls(
            id_=id_,
            lat=lat,
            lon=lon,
            geonames=geonames,
            name=""
        )

        # usuwamy domyślny okres
        place.periods = []

        labels = wiki_data.get("labels", {})
        countries = wiki_data.get("countries", [])

        current_country = get_current_country(countries)

        # countries mapping
        countries_labels_mapping = {
            'Slovakia' : {
                'pl': 'Słowacja',
                'sk': 'Slovensko'}
        }

        # aktualne etykiety
        if labels.get("pl"):
            country_pl = countries_labels_mapping.get(current_country, {}).get('pl', None)
            place.periods.append(
                cls.PlacePeriod(
                    name=labels["pl"],
                    country=country_pl if country_pl else current_country,
                    country_lang='pl' if country_pl else 'en',
                    lang="pl"
                )
            )

        if labels.get("en"):
            place.periods.append(
                cls.PlacePeriod(
                    name=labels["en"],
                    country=current_country,
                    country_lang="en",
                    lang="en"
                )
            )

        original = labels.get("original")
        if original:
            original_lang = original.get("language", "")
            country_by_lang = countries_labels_mapping.get(current_country, {}).get(original_lang, None)
            place.periods.append(
                cls.PlacePeriod(
                    name=original.get("value", ""),
                    country=country_by_lang if country_by_lang else None,
                    country_lang=original_lang if country_by_lang else None,
                    lang=original_lang
                )
            )

        # nazwy historyczne
        # for official_name in wiki_data.get("official_names", []):
        #     date_from = official_name.get("start_time") or ""
        #     date_to = official_name.get("end_time") or ""

        #     place.periods.append(
        #         cls.PlacePeriod(
        #             date_from=date_from,
        #             date_to=date_to,
        #             name=official_name.get("name", ""),
        #             lang=official_name.get("language", "")
        #         )
        #     )

        # # awaryjnie - gdyby nie było żadnych nazw
        # if not place.periods:
        #     place.periods.append(
        #         cls.PlacePeriod(
        #             name=wiki,
        #             country=current_country
        #         )
        #     )

        return place
    
    def to_xml(self):
        place_xml = ET.Element('place', {k:v for k,v in self.__dict__.items() if k not in ('periods', 'annotation') and v})
        
        for period in self.periods:
            place_xml.append(period.to_xml())
            
        if self.annotation:
            annotation_xml = ET.Element('annotation')
            annotation_xml.text = self.annotation
            place_xml.append(annotation_xml)
            
        return place_xml

# # schemat XML
# <place geonames="3088171" lon="16.92993" lat="52.40692" id="https://www.wikidata.org/wiki/Q268">
#                 <period date-from="" date-to="">
#                     <name lang="pl">Poznań</name>
#                     <country lang="pl">Polska</country>
#                 </period>
#             </place>






























