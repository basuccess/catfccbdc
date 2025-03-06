# Constants for the project
# Example list of states and territories formatted as (FIPS Code, Abbreviation, Full Name)
STATES_AND_TERRITORIES = (
    ("01", "AL", "Alabama"),
    ("02", "AK", "Alaska"),
    ("04", "AZ", "Arizona"),
    ("05", "AR", "Arkansas"),
    ("06", "CA", "California"),
    ("08", "CO", "Colorado"),
    ("09", "CT", "Connecticut"),
    ("10", "DE", "Delaware"),
    ("11", "DC", "District_of_Columbia"), 
    ("12", "FL", "Florida"), 
    ("13", "GA", "Georgia"), 
    ("15", "HI", "Hawaii"), 
    ("16", "ID", "Idaho"),  
    ("17", "IL", "Illinois"),  
    ("18", "IN", "Indiana"),  
    ("19", "IA", "Iowa"),
    ("20", "KS", "Kansas"),
    ("21", "KY", "Kentucky"),
    ("22", "LA", "Louisiana"),
    ("23", "ME", "Maine"),
    ("24", "MD", "Maryland"),
    ("25", "MA", "Massachusetts"),
    ("26", "MI", "Michigan"),
    ("27", "MN", "Minnesota"),
    ("28", "MS", "Mississippi"),
    ("29", "MO", "Missouri"),  
    ("30", "MT", "Montana"),  
    ("31", "NE", "Nebraska"),  
    ("32", "NV", "Nevada"), 
    ("33", "NH", "New_Hampshire"), 
    ("34", "NJ", "New_Jersey"), 
    ("35", "NM", "New_Mexico"), 
    ("36", "NY", "New_York"), 
    ("37", "NC", "North_Carolina"), 
    ("38", "ND", "North_Dakota"),
    ("39", "OH", "Ohio"),
    ("40", "OK", "Oklahoma"),  
    ("41", "OR", "Oregon"),  
    ("42", "PA", "Pennsylvania"),  
    ("44", "RI", "Rhode_Island"),  
    ("45", "SC", "South_Carolina"),
    ("46", "SD", "South_Dakota"),
    ("47", "TN", "Tennessee"),
    ("48", "TX", "Texas"),
    ("49", "UT", "Utah"),
    ("50", "VT", "Vermont"),
    ("51", "VA", "Virginia"),
    ("53", "WA", "Washington"),
    ("54", "WV", "West_Virginia"),
    ("55", "WI", "Wisconsin"),
    ("56", "WY", "Wyoming"),
    # Territories
    ("60", "AS", "American_Samoa"),
    ("64", "FM", "Federated_States_of_Micronesia"),
    ("66", "GU", "Guam"),
    ("68", "MH", "Marshall_Islands"),
    ("69", "MP", "Northern_Mariana_Islands"),
    ("70", "PW", "Palau"),
    ("72", "PR", "Puerto_Rico"),
    ("74", "UM", "U.S._Minor_Outlying_Islands"),
    ("78", "VI", "U.S._Virgin_Islands")
)

# Technology abbreviation mapping with indicator for speed calculations
TECH_ABBR_MAPPING = {
    10: ("Copper", True),   # 6 chars
    40: ("Cable", True),    # 5 chars
    50: ("Fiber", True),    # 5 chars
    60: ("GeoSat", False),  # 6 chars
    61: ("NGeoSt", False),  # 6 chars
    70: ("UnlFWA", False),  # 6 chars
    71: ("LicFWA", True),   # 6 chars
    72: ("LBRFWA", True),   # 6 chars
    0: ("Other", False)     # 5 chars
}

BDC_US_PROVIDER_FILE_PATTERN = r'bdc_us_provider_list_[A-Z]\d{2}_(.*)\.(zip|csv)'

BDC_TECH_CODES_FILE_PATTERN = r'bdc-Fixed-and-Mobile-Technology-Codes\.(zip|csv)'

TABBLOCK20_FILE_PATTERN = r'tl_\d{2}_tabblock20\.(shp|zip)'

BDC_FILE_PATTERN = r'bdc_\d{2}_(Cable|Copper|FibertothePremises|GSOSatellite|LBRFixedWireless|LicensedFixedWireless|NGSOSatellite|Other|UnlicensedFixedWireless)_fixed_broadband\.(zip|csv)'

SERVED_DL_SPEED = 100  # Served download speed in Mbps
SERVED_UL_SPEED = 20  # Served upload speed in Mbps
LOW_LATENCY = 1  # Low latency indicator
UNDERSERVED_DL_SPEED = 25  # Underserved download speed in Mbps
UNDERSERVED_UL_SPEED = 3  # Underserved upload speed in Mbps
