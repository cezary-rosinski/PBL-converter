import xml.etree.cElementTree as ET
from datetime import datetime
from SPUB_files_journal_number import JournalNumber

#%% main
class JournalYear:
    
    def __init__(self, year, journal_id='', numbers_set=None, character='literary'):
        self.year = year
        self.removed = 'false'
        self.origin = ''
        self.journal_id = journal_id
        self.id = f'{self.journal_id}_{self.year}'
        self.characters = [self.JournalYearCharacter(character=character)]
        self.closed = 'false'
        if numbers_set:
            self.numbers = [JournalNumber(number=e, journal_year_id=self.id) for e in numbers_set]
        self.status = 'under-development'
        
    class XmlRepresentation:
        
        def to_xml(self):
            match self.__class__.__name__:
                case 'JournalYearCharacter':
                    return ET.Element('character', {'code': self.character})
    
    class JournalYearCharacter(XmlRepresentation):
        
        def __init__(self, character):
            self.character = character
    
        def __repr__(self):
            return "JournalYearCharacter('{}')".format(self.character)

    def __repr__(self):
        return "JournalYear('{}', '{}', '{}')".format(self.year, self.journal_id, self.numbers)
            
    def to_xml(self):
        journal_year_dict = {k:v for k,v in {'removed': self.removed, 'id': self.id, 'origin': self.origin}.items() if v}
        journal_year_xml = ET.Element('journal-year', journal_year_dict)
        
        journal_year_xml.append(ET.Element('journal', {'id': self.journal_id}))
        
        characters_xml = ET.Element('characters')
        for character in self.characters:
            characters_xml.append(character.to_xml())
        journal_year_xml.append(characters_xml)
        
        journal_year_xml.append(ET.Element('year', {'value': self.year}))
        
        journal_year_xml.append(ET.Element('journal-year-status', {'value': self.status}))
        
        journal_year_xml.append(ET.Element('closed', {'value': self.closed}))
        
        return journal_year_xml



# #print tests
# test = JournalYear('2002', 'polityka', numbers_set = ['47', '74'])
# test.__dict__

# test_xml = test.to_xml()
# from xml.dom import minidom
# xmlstr = minidom.parseString(ET.tostring(test_xml)).toprettyxml(indent="   ")
# print(xmlstr)

#%% schemat

# <journal-year removed="false"  id="journal-year-id-01" origin="IdentyfikatorŹródłaUAM">
# 				<journal id="journal-id-01">
# 					
# 				
# 				<year value="22"/>
# 				
# 				<dedication-year-1 value="23"/>
# 				<dedication-year-2 value="24"/>
# 			
# 			
#                 <annotation>To jest jakaś adnotacja</annotation>

#                 <remark>To jest jakiś komentarz</remark>


#                 <tags>
#                     <tag>#WaznyRocznikCzasopisma</tag>
#                     <tag>#nicMiNiePrzychodzi</tag>
#                     <!-- ... -->
#                 </tags>
                
                
#                 <characters>
# 					<character code="radio" />
# 					<character code="scientific-society" />
# 					<character code="scientific" />
# 				</characters>
                
# 				<journal-year-status value="under-development"/> <!-- under-development, selection, elimination -->
# 				
# 				<resignation-reason-public>Ble ble ble ale publiczne</resignation-reason-public>
# 				<resignation-reason-private>Pismo jest do dudy nie ma co opracowywać</resignation-reason-private>
# 				
# 				<closed value="false" />
# 			
# 			</journal-year>