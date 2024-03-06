import os
import re
import sqlite3
from datetime import datetime

def get_valid_data_files(folder = "./data"):
    '''
    Map file names to file paths in a user defined folder inside the working directory
    Args:
        folder (str) : folder in working directory, default = "./data"
    Returns:
        self (dict, {file name : (file path, license_type, sales_type, sales_month, sales_year)}) : tuple of file paths and file information keyed to file names 
    '''
    self = {}

    file_count = 0  # Initialize variables
    license_file_count = 0
    sales_file_count = 0

    for fname in os.listdir(folder):    # Iterate through files in folder
        file_count += 1 # Count total files
        license_type = None # Initialize for each loop
        sales_type = None
        sales_month = None
        sales_year = None
        
        if re.search(".csv", fname):    # Read .csv file
            fpath = os.path.join(folder, fname)
            fhand = open(fpath)
            count = 0
            
            while count < 5:    # Read first 5 lines of each .csv file
                for line in fhand:
                    line = line.strip()
                    if re.search("LICENSE TYPE", line): # Identify license type
                        license_type = re.findall('\s\S\s([A-Za-z ]*)[,]*', line)
                        license_type = license_type[0]
                        count = 5
                        continue
                    elif count == 4:    # Exit loop if license information not at top of file
                        print(f'INVALID .CSV FILE: {fname}')
                        continue
                    count += 1
            license_file_count += 1
            self[fname] = (fpath, license_type, sales_type, sales_month, sales_year) # Insert file information tuple into dictionary
            
        if re.search(".txt", fname):    # Read .txt file
            fpath = os.path.join(folder, fname)
            if re.search('BEER', fname):
                sales_type = 'BEER'
                month = re.findall('^([A-Z]+)\s', fname)
                sales_month = month[0]
                year = re.findall('^[A-Z]+\s([0-9]+)\s', fname)
                sales_year = year[0]
                sales_file_count += 1
                self[fname] = (fpath, license_type, sales_type, sales_month, sales_year) # Insert file information tuple into dictionary
                continue

            if re.search('WINE', fname):
                sales_type = 'WINE'
                month = re.findall('^([A-Z]+)\s', fname)
                sales_month = month[0]
                year = re.findall('^[A-Z]+\s([0-9]+)\s', fname)
                sales_year = year[0]
                sales_file_count += 1
                self[fname] = (fpath, license_type, sales_type, sales_month, sales_year) # Insert file information tuple into dictionary
                continue

            else:
                print(f'INVALID .TXT FILE: {fname}')
                continue

    print(f'\nREAD {file_count} FILES\nVALID LICENSE FILES: {license_file_count}\nVALID SALES FILES: {sales_file_count}\n')
    return self



class WLCB_DB:
    '''WLCB Database object'''

    def __init__(self, name):
        '''Create database and changelog if not exists, connect to database
        Args:
            name (str) : database name
        Returns:
            None
        '''

        # Initialize variables
        name = 'wlcb_db'
        db_extension = '.sqlite'
        db_filename = None
        valid_db_name = False
        
        # Set valid database filename
        while valid_db_name == False:
            if name == ' ':
                valid_db_name = True
                continue
            if re.search('[^a-zA-Z0-9_]', name):
                print(f'INVALID DATABASE FILENAME: {name}')
                user_db_name = None
                user_db_name = input("ENTER A DATABASE FILENAME, ' ' FOR DEFAULT:")
                name = user_db_name
                continue
            if re.search('[A-Za-z0-9_]', name):
                db_name = name
                valid_db_name = True
        
        db_filename = db_name + db_extension
        self._db_name = db_filename
        
        # Get current time for event log
        eventtime = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
        self._created = eventtime

        # Create database, establish connection, define cursor
        if not os.path.exists(db_filename):
            self._conn = sqlite3.connect(db_filename)
            self._cur = self._conn.cursor()
            event = (f'{db_filename} created {eventtime}')
        elif os.path.exists(db_filename):
            self._conn = sqlite3.connect(db_filename)
            self._cur = self._conn.cursor()
            event = (f'{db_filename} connected {eventtime}')
        else:
            print(f'ERROR: UNABLE TO CREATE OR CONNECT TO DATABASE: {db_filename}')
        
        # Confirm database instantiation
        print(f'{event}\n')

        # Create event log
        self._event_log_filename = self._db_name.replace('.sqlite', '_log.txt')
        if not os.path.exists(self._event_log_filename):
            event = (f'{self._db_name} created {eventtime}')
            self.create_event_log(event)
        return None

    def create_event_log(self, event):
        '''Create event log'''
        name = self._event_log_filename
        self = open(self._event_log_filename, 'x')
        self.write(f"{name.replace('_log.txt', '')} Event Log\n\n{event}\n\n")
        self.close()
        return None

    def append_event_log(self, event):
        '''Append event log'''
        self = open(self._event_log_filename, 'a')
        self.write(f'{event}\n')
        self.close()
        return None

    def get_connection(self):
        '''Return connection'''
        return self._conn
    
    def get_cursor(self):
        '''Return cursor'''
        return self._cur
    
    def create_datatables(self):
        '''Create empty datatables'''
        conn = self._conn
        cur = self._cur

        # Create empty tables
        cur.execute('''CREATE TABLE IF NOT EXISTS event_log (id INTEGER PRIMARY KEY, time, event_type)''')
        cur.execute('''CREATE TABLE IF NOT EXISTS license_types (id INTEGER PRIMARY KEY, license_type TEXT UNIQUE, created, modified)''')
        cur.execute('''CREATE TABLE IF NOT EXISTS seller_types (id INTEGER PRIMARY KEY, seller_type TEXT UNIQUE, created, modified)''')
        cur.execute('''CREATE TABLE IF NOT EXISTS names (id INTEGER PRIMARY KEY, name TEXT UNIQUE, created, modified)''')
        cur.execute('''CREATE TABLE IF NOT EXISTS addresses (id INTEGER PRIMARY KEY, address TEXT UNIQUE, created, modified)''')
        cur.execute('''CREATE TABLE IF NOT EXISTS cities (id INTEGER PRIMARY KEY, city TEXT UNIQUE, created, modified)''')
        cur.execute('''CREATE TABLE IF NOT EXISTS states (id INTEGER PRIMARY KEY, state TEXT UNIQUE, created, modified)''')
        cur.execute('''CREATE TABLE IF NOT EXISTS zipcodes (id INTEGER PRIMARY KEY, zip TEXT UNIQUE, created, modified)''')
        cur.execute('''CREATE TABLE IF NOT EXISTS month (id INTEGER PRIMARY KEY, month TEXT UNIQUE, created, modified)''')
        cur.execute('''CREATE TABLE IF NOT EXISTS year (id INTEGER PRIMARY KEY, year INTEGER UNIQUE, created, modified)''')
        cur.execute('''CREATE TABLE IF NOT EXISTS licenses (id INTEGER PRIMARY KEY, license, 
                    name_id, address_id, city_id, state_id, zip_id, license_type_id, created, modified, UNIQUE (license, license_type_id))''')

        cur.execute('''CREATE TABLE IF NOT EXISTS beer_sales (id INTEGER PRIMARY KEY, name_id, seller_type_id, month_id INTEGER, 
                    year_id INTEGER, over_60k_barrels INTEGER, under_60k_barrels INTEGER, total_beer_barrels INTEGER, created, modified,
                    UNIQUE(name_id, month_id, year_id, over_60k_barrels, under_60k_barrels, total_beer_barrels))''')

        cur.execute('''CREATE TABLE IF NOT EXISTS wine_sales (id INTEGER PRIMARY KEY, name_id, seller_type_id, month_id INTEGER, 
                    year_id INTEGER, under_14abv INTEGER, over_14abv INTEGER, total_wine_liters INTEGER, created, modified,
                    UNIQUE(name_id, month_id, year_id, under_14abv, over_14abv, total_wine_liters))''')

        cur.execute('''CREATE TABLE IF NOT EXISTS cider_sales (id INTEGER PRIMARY KEY, name_id, seller_type_id, month_id INTEGER, 
                    year_id INTEGER, total_cider_liters INTEGER, created, modified,
                    UNIQUE(name_id, month_id, year_id, total_cider_liters))''')

        cur.execute('''CREATE TABLE IF NOT EXISTS names_licenses_sellers (id INTEGER PRIMARY KEY, name_id, license_id, bev_type,
                    wa_brew, wa_wine_import, wine_coa, wa_beer_import, beer_rep_coa, wine_rep_coa, wine_ship_cons,
                    wine_beer_dist, wa_wine, beer_coa, wa_wine_dist, wine_ship_retail, wine_coa_ship_cons, beer_coa_ship_retail,
                    s_in_state_brew, s_out_state_brew, s_beer_coa, s_in_state_wine, s_out_state_wine, s_wine_coa, created, modified,
                    UNIQUE(name_id, license_id, bev_type))''')

        # Commit datatables
        conn.commit()

        # Update event_log table with database creation
        self.update_event_log_datatable(self._created, 'created')

        # Get details for event log
        eventtime = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
        event = (f'Empty datatables created {eventtime}')
        self.append_event_log(event)
        print(f'{event}\n')
        return None
    
    def update_event_log_datatable(self, time, event_type):
        '''Append event log datatable'''
        conn = self._conn
        cur = self._cur

        cur.execute('''INSERT OR IGNORE INTO event_log (time, event_type) VALUES (?, ?)''', (time, event_type))

        # Commit changes
        conn.commit()
        return None