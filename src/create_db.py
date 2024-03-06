import utils
import time

# Define location of license and sales files, map names to paths
data_files = utils.get_valid_data_files("./data")

# Create SQL database for WLCB license and sales data
db = utils.WLCB_DB('wlcb_db')