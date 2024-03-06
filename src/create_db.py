import utils

# Define location of license and sales files, map names to paths
files = utils.get_valid_data_files("./data")

# Create SQL database and changelog for WLCB license and sales data
db = utils.WLCB_DB('wlcb_db')

# Create datatables
db.create_datatables()

# Populate datatables with license and sales data
db.insert_data(files)