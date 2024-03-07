import os
import re
import sqlite3
import time
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

def check_row(dict, line, key):
    '''
    Check for specific text from a list while parsing sales data
    Args:
        dict (dict, {keep OR ignore : [lst of regex]}) : list of text patterns and regex to keep or ignore
        line (str): row to search
    Returns:
        self (bool) : True or False, default = False
    '''

    self = False
    
    lst = dict[key]
        
    for i in lst:
        result = re.search(i, line)
        if result != None:
            self = True
            break
        else:
            self = False
    
    return self

def parse_license_data(line):
    '''Parse license data
    Args:
        line (str) : current line of current file
    Returns:
        self (tuple) : (license, name, address, city, state, zc) -> tuple of license data
    '''

    self = ()

    # Parse normal license data
    if re.search('^.[0-9]', line):
        entries = line.split(',')
        license = entries[0]
        name = entries[1]
        address = entries[2]
        city = entries[3]
        state = entries[4]
        zc = entries[5]

        # Parsing routine for abnormal license data
        if len(entries) != 6:
            if re.search('[""]', line):
                license = re.findall('^([0-9]*),?', line)
                license = license[0]
                name = re.findall('^[0-9]*,([A-Z0-9 .&]*),?', line)
                name = name[0]
                    
                # Clean lines with extra commas, no whitespace, etc
                if re.findall(',"(.*)",.+,.+,.+', line):
                    address = re.findall(',"(.*)",', line)
                    address = address[0]
                    address = re.sub(r',(\S)', r', \1', address, count = 0)
                    city = re.findall('^[0-9]*,.*,".*",([A-Z ]*),.+,.+', line)
                    city = city[0]
                else:
                    address = re.findall('^[0-9]*,.*,(.*),".*",.+,.+', line)
                    address = address[0]
                    city = re.findall('^[0-9]*,.*,.*,"([A-Z, ]*)",.+,.+', line)
                    city = city[0]
                    city = re.sub(',', '', city)
                state = re.findall('^[0-9]*,.*,.*,([A-Z]*),[0-9]*$', line)
                state = state[0]
                zc = re.findall('^[0-9]*,.*,.*,.*,([0-9]+)$', line)
                zc = zc[0]

    self = (license, name, address, city, state, zc)

    return self

def parse_sales_data(fhand, beer, wine, line_count, sales_count, seller_type, seller, regex, error_count):
    '''Parse sales data
    Args:
        line (str) : current line of current file
    Returns:
        self (list) : [(name, seller_type, values, sales_count, line_count, error_count)] -> list of tuples of sales data
        '''

    self = []

    # Parse beer sales data
    if beer == True:
        values = [] # Container for sales values
        for line in fhand:
            line_count += 1
            line = line.strip()
            if len(line) < 1: continue
            if check_row(regex, line, 'keep') == True:
                if re.search('^In', line):
                    seller = line
                    slice = 3
                if re.search('^Out', line):
                    seller = line
                    slice = 4
                if re.search('^Authorized', line):
                    seller = 'Beer ' + line
                    slice = 4
                if seller != None:
                    seller = seller.split(' ')
                    seller = seller[:slice]
                    seller_type = ' '.join(seller)
                    continue
                else: 
                    print('INVALID SELLER_TYPE:', line)
                    error_count += 1
                    continue
            if check_row(regex, line, 'ignore') == True: continue
            if re.search('[A-Z]', line):
                name = line
                continue
            if re.search('[.][0-9][0-9]$', line) and name != None:
                value = line
                value = float(value.replace(',', ''))
                values.append(value)
            if name != None and len(values) == 3:
                sales_count += 1
                sale = (name, seller_type, values, sales_count, line_count, error_count)
                self.append(sale)
                name = None # Reset loop variables for next iteration
                values = []
            if re.search('[.][0-9][0-9]$', line) and name == None: continue
            elif name == None:
                print('PARSE ERROR:', line)
                error_count += 1

        print(f'SALES COUNT: {sales_count}\n')

    # Parse wine sales data
    if wine == True:
        values = [] # Container for sales values
        for line in fhand:
            line_count += 1
            line = line.strip()
            if len(line) < 1: continue
            if check_row(regex, line, 'keep') == True:
                if re.search('^In', line):
                    seller = line
                    slice = 3
                if re.search('^Out', line):
                    seller = line
                    slice = 4
                if re.search('^Authorized', line):
                    seller = 'Wine ' + line
                    slice = 4
                if seller != None:
                    seller = seller.split(' ')
                    seller = seller[:slice]
                    seller_type = ' '.join(seller)
                else: 
                    print('SELLER_TYPE ERROR:', line)
                    error_count += 1
            if check_row(regex, line, 'ignore') == True: continue
            if re.search('[A-Z]', line):
                name = line
                continue
            if re.search('[.][0-9][0-9]$', line) and name != None:
                value = line
                value = float(value.replace(',', ''))
                values.append(value)
            if name != None and len(values) == 4:
                sales_count += 1
                sale = (name, seller_type, values, sales_count, line_count, error_count)
                self.append(sale)
                name = None # Reset loop variables for next iteration
                values = []
            if re.search('[.][0-9][0-9]$', line) and name == None: continue
            if re.search('[a-z][.]', line) and name == None: continue
            elif name == None:
                print('PARSE ERROR:', line)
                error_count += 1

        print(f'SALES COUNT: {sales_count}\n')

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
            event = (f'Created {db_filename}, {eventtime}')
        elif os.path.exists(db_filename):
            self._conn = sqlite3.connect(db_filename)
            self._cur = self._conn.cursor()
            event = (f'Connected {db_filename}, {eventtime}')
        else:
            print(f'ERROR: UNABLE TO CREATE OR CONNECT TO DATABASE: {db_filename}')
        
        # Confirm database instantiation
        print(f'{event}\n')

        # Create event log
        self._event_log_filename = self._db_name.replace('.sqlite', '_log.txt')
        if not os.path.exists(self._event_log_filename):
            time.sleep(1)
            event = (f'Created {self._db_name}, {eventtime}')
            self.create_event_log(event)
        
        return None

    def create_event_log(self, event):
        '''Create event log
        Args:
            event (str) : event description
        Returns:
            None
        '''
        name = self._event_log_filename
        self = open(self._event_log_filename, 'x')
        self.write(f"{name.replace('_log.txt', '')} Event Log\n\n{event}\n")
        self.close()
        
        return None

    def append_event_log(self, event):
        '''Append event log
        Args:
            event (str) : event description
        Returns:
            None
        '''
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
        event = (f'Created empty datatables, {eventtime}')
        self.append_event_log(event)
        print(f'{event}\n')

        return None
    
    def update_event_log_datatable(self, time, event_type):
        '''Append event log datatable
        Args:
            time (str) : time of event
            event_type (str) : event description
        Returns:
            None
        '''
        conn = self._conn
        cur = self._cur

        cur.execute('''INSERT OR IGNORE INTO event_log (time, event_type) VALUES (?, ?)''', (time, event_type))

        # Commit changes
        conn.commit()
        
        return None
    
    def insert_data(self, files):
        '''Read datafile, parse data, insert data into database, log event
        Args:
            files (dict, {file name : (file path, license_type, sales_type, sales_month, sales_year)}) :
                tuple of file paths and file information keyed to file names
            Returns:
                None
        '''
        conn = self._conn
        cur = self._cur
        time.sleep(1)
        eventtime = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
        event_type = 'inserted'
        self.update_event_log_datatable(eventtime, event_type)

        print(f'INSERTING LICENSE DATA')

        # Initialize variables
        file_count = 0
        line_count = 0
        error_count = 0
        license_count = 0
        sales_count = 0
        reg_exp = {}
        license_files = []
        sales_files = []
        
        # Get time_id
        cur.execute('SELECT id FROM event_log WHERE time = ?', (eventtime,))
        time_id = cur.fetchone()[0]

        # Populate month table
        for m in ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']:
            cur.execute('''INSERT OR IGNORE INTO month (month, created) VALUES (?, ?)''', (m, time_id))

        # Sort licenses and sales files before parsing
        for file in files:
            if not files[file][1]:
                sales_files.append(files[file])
            else:
                license_files.append(files[file])

        # Text to ignore
        reg_exp = {'ignore' : ['LICENSE TYPE','License #,Licensee,Address,City,State,Zip']}

        # Read license files, parse data, insert into database
        for file in license_files:
            print(f'READING: {file[0]}')
            license_type = None # Reset value each loop
            fhand = open(file[0])
            license_type = file[1]
            file_count += 1

            # Insert and commit licence type
            cur.execute('''INSERT OR IGNORE INTO license_types (license_type, created) VALUES (?, ?)''', (license_type, time_id))
            cur.execute('SELECT id FROM license_types WHERE license_type = ?', (license_type,))
            license_type_id = cur.fetchone()[0]
            conn.commit()

            for line in fhand:
                line_count += 1
                line = line.strip()
                license = None  # Reset values each loop
                name = None
                address = None
                city = None
                state = None
                zc = None
                if check_row(reg_exp, line, 'ignore') == True:
                    continue
                try:
                    #Parse license text
                    p_line = parse_license_data(line)
                    license = p_line[0]
                    name = p_line[1]
                    address = p_line[2]
                    city = p_line[3]
                    state = p_line[4]
                    zc = p_line[5]

                    # Insert parsed text into database
                    # Insert name
                    cur.execute('''INSERT OR IGNORE INTO names (name, created) VALUES (?, ?)''', (name, time_id))
                    cur.execute('SELECT id FROM names WHERE name = ?', (name,))
                    name_id = cur.fetchone()[0]
                    # Insert address
                    cur.execute('''INSERT OR IGNORE INTO addresses (address, created) VALUES (?, ?)''', (address, time_id))
                    cur.execute('SELECT id FROM addresses WHERE address = ?', (address,))
                    address_id = cur.fetchone()[0]
                    # Insert city
                    cur.execute('''INSERT OR IGNORE INTO cities (city, created) VALUES (?, ?)''', (city, time_id))
                    cur.execute('SELECT id FROM cities WHERE city = ?', (city,))
                    city_id = cur.fetchone()[0]
                    # Insert state
                    cur.execute('''INSERT OR IGNORE INTO states (state, created) VALUES (?, ?)''', (state, time_id))
                    cur.execute('SELECT id FROM states WHERE state = ?', (state,))
                    state_id = cur.fetchone()[0]
                    # Insert zipcode
                    cur.execute('''INSERT OR IGNORE INTO zipcodes (zip, created) VALUES (?, ?)''', (zc, time_id))
                    cur.execute('SELECT id FROM zipcodes WHERE zip = ?', (zc,))
                    zip_id = cur.fetchone()[0]
                    # Insert license and foreign keys
                    cur.execute('''INSERT OR IGNORE INTO licenses 
                                (license, license_type_id, name_id, address_id, city_id, state_id, zip_id, created) 
                                VALUES ( ?, ?, ?, ?, ?, ?, ?, ?)''', 
                                (license, license_type_id, name_id, address_id, city_id, state_id, zip_id, time_id))
                    
                    # Increment license count
                    license_count += 1

                    # Commit to database every 10 lines
                    if license_count % 10 == 0:
                        conn.commit()
                
                except Exception as e:
                    error_count += 1
                    print(f'\nLICENSE PARSE or INSERT ERROR:\n{line}\n{e}\n')
                    continue

            # Commit to database before loading next file
            conn.commit()

            # Print running total of results
            print(f'LICENSE COUNT: {license_count}\n')

        print(f'INSERTING SALES DATA')

        line = None # Clear variable

        # Text to keep or ignore while parsing beer and wine sales data
        reg_exp = {'keep' : ['State Brewery Trade Name', 'State Winery Trade Name', 'Authorized Rep COA Trade Name'],
                   'ignore' : ['WASHINGTON STATE LIQUOR', '^OLYMPIA, WA 98504', '^REPORT OF NET BEER SALES', '^NET WINE SALES TO', 
                               '^IN THE STATE OF WASHINGTON', '^STATED IN', '^Over 60,000 Barrels', '^60,000 Barrels & Under', 
                               '^cider liters', '^14% & Under', '^Over 14%', '^Total ', '^DATE:', '^NOTE:', '^[A-Z][a-z]']}
        
        # Read sales files, parse data, insert into database
        for file in sales_files:
            print(f'READING: {file[0]}')
            m = None
            y = None
            seller_type = None
            seller = None
            beer = False
            wine = False
            fhand = open(file[0])
            file_count += 1
            sales_type = file[2]
            if sales_type == 'BEER': beer = True
            if sales_type == 'WINE': wine = True
            m = file[3]
            if len(m) > 3: 
                m = m[:3]
            y = file[4]
            
            # Insert and commit month and year
            #cur.execute('''INSERT OR IGNORE INTO month (month, created) VALUES (?, ?)''', (m, time_id))
            cur.execute('''INSERT OR IGNORE INTO year (year, created) VALUES (?, ?)''', (y, time_id))
            conn.commit()

            # Parse and insert beer sales data
            if beer == True:
                name = None
                seller_type = None
                seller = None
                p_sales = []
                try:
                    # Parse sales data
                    p_sales = parse_sales_data(fhand, beer, wine, line_count, sales_count, seller_type, seller, reg_exp, error_count)
                    for sale in p_sales:
                        name = sale[0]
                        seller_type = sale[1]
                        values = sale[2]
                        sales_count = sale[3]
                        line_count = sale[4]
                        error_count = sale[5]
                    
                        # Insert sales data in database
                        if name != None and len(values) == 3:
                            # Insert name
                            cur.execute('''INSERT OR IGNORE INTO names (name, created) VALUES (?, ?)''', (name, time_id))
                            # Insert seller_type
                            cur.execute('''INSERT OR IGNORE INTO seller_types (seller_type, created) VALUES (?, ?)''', (seller_type, time_id))
                            # Get name_id
                            cur.execute('SELECT id FROM names WHERE name = ?', (name,))
                            name_id = cur.fetchone()[0]
                            # Get seller_type_id
                            cur.execute('SELECT id FROM seller_types WHERE seller_type = ?', (seller_type,))
                            seller_type_id = cur.fetchone()[0]
                            # Get month_id
                            cur.execute('SELECT id FROM month WHERE month = ?', (m,))
                            month_id = cur.fetchone()[0]
                            # Get year_id
                            cur.execute('SELECT id FROM year WHERE year = ?', (y,))
                            year_id = cur.fetchone()[0]
                            # Insert beer sales data and foreign keys 
                            cur.execute('''INSERT OR IGNORE INTO beer_sales (name_id, seller_type_id, month_id, year_id, 
                                        over_60k_barrels, under_60k_barrels, total_beer_barrels, created) VALUES (?, ?, ?, ?, ?, ?, ?, ?)''', 
                                        (name_id, seller_type_id, month_id, year_id, values[0], values[1], values[2], time_id))
                
                            # Commit to database every 10 entries
                            if sales_count % 10 == 0:
                                conn.commit()

                except Exception as e:
                    error_count += 1
                    print(f'\nSALES PARSE or INSERT ERROR:\n{sale}\n{e}\n')
                    continue

            # Parse and insert wine & cider sales data
            if wine == True:
                name = None
                seller_type = None
                seller = None
                p_sales = []
                try:
                    # Parse sales data
                    p_sales = parse_sales_data(fhand, beer, wine, line_count, sales_count, seller_type, seller, reg_exp, error_count)
                    for sale in p_sales:
                        name = sale[0]
                        seller_type = sale[1]
                        values = sale[2]
                        sales_count = sale[3]
                        line_count = sale[4]
                        error_count = sale[5]

                        # Insert sales data in database
                        if name != None and len(values) == 4:
                            cider_total = values[0]
                            wine_total = values[1] + values[2]
                            # Insert name
                            cur.execute('''INSERT OR IGNORE INTO names (name, created) VALUES (?, ?)''', (name, time_id))
                            # Insert seller_type
                            cur.execute('''INSERT OR IGNORE INTO seller_types (seller_type, created) VALUES (?, ?)''', (seller_type, time_id))
                            # Get name_id
                            cur.execute('SELECT id FROM names WHERE name = ?', (name,))
                            name_id = cur.fetchone()[0]
                            # Get seller_type_id
                            cur.execute('SELECT id FROM seller_types WHERE seller_type = ?', (seller_type,))
                            seller_type_id = cur.fetchone()[0]
                            # Get month_id
                            cur.execute('SELECT id FROM month WHERE month = ?', (m,))
                            month_id = cur.fetchone()[0]
                            # Get year_id
                            cur.execute('SELECT id FROM year WHERE year = ?', (y,))
                            year_id = cur.fetchone()[0]
                            
                            # Insert wine sales data and foreign keys 
                            if values[1] != 0.00 or values[2] != 0.00:
                                cur.execute('''INSERT OR IGNORE INTO wine_sales (name_id, seller_type_id, month_id, year_id, 
                                            under_14abv, over_14abv, total_wine_liters, created) VALUES (?, ?, ?, ?, ?, ?, ?, ?)''', 
                                            (name_id, seller_type_id, month_id, year_id, values[1], values[2], wine_total, time_id))
                            
                            # Insert cider sales data and foreign keys 
                            if values[0] != 0.00:
                                cur.execute('''INSERT OR IGNORE INTO cider_sales (name_id, seller_type_id, month_id, year_id, 
                                            total_cider_liters, created) VALUES (?, ?, ?, ?, ?, ?)''', 
                                            (name_id, seller_type_id, month_id, year_id, cider_total, time_id))
                                
                            # Commit to database every 10 entries
                            if sales_count % 10 == 0:
                                conn.commit()

                except Exception as e:
                    error_count += 1
                    print(f'\nSALES PARSE or INSERT ERROR:\n{sale}\n{e}\n')
                    continue

            # Commit to database before loading next file
            conn.commit()

        # Commit and close connection to database
        conn.commit()
        conn.close()

        # Append event log
        #self.append_event_log(action = 'Insert Data,', details = (f'{line_count} Lines,'))
        self.append_event_log(f'Inserted {line_count} lines, {eventtime}')

        # Print Summary
        print('\n*** INSERT DATA COMPLETE ***\n')
        print(f'TOTAL FILE COUNT: {file_count}')
        print(f'TOTAL LINE COUNT: {line_count}')
        print(f'TOTAL LICENSE COUNT: {license_count}')
        print(f'TOTAL SALES COUNT: {sales_count}')
        print(f'TOTAL ERROR COUNT: {error_count}')

        return None