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

        # Create database, establish connection, define cursor
        self._conn = sqlite3.connect(db_filename)
        self._cur = self._conn.cursor()

        # Confirm database instantiation
        event = (f'{db_filename} created {eventtime}')
        print(f'{event}\n')

        # Create event log
        self.create_event_log(event)

        return None

    def create_event_log(self, event):
        '''Create event log'''
        event_log_filename = self._db_name.replace('.sqlite', '_log.txt')
        self = open(event_log_filename, 'x')
        self.write(f"{event_log_filename.replace('_log.txt', '')} Event Log\n\n{event}\n\n")
        self.close()

    def get_connection(self):
        '''Return connection'''
        return self._conn
    
    def get_cursor(self):
        '''Return cursor'''
        return self._cur