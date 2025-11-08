import firebase_admin
import time
import gspread
from datetime import datetime
from firebase_admin import credentials, db

# --- 1. SETUP ---

# YOUR Database URL
DATABASE_URL = 'https://niha-ipd-default-rtdb.firebaseio.com/'

# YOUR Service Account Key path
cred_path = r"C:\Users\sairu\OneDrive\Desktop\firebaseintegration\niha-ipd-firebase-adminsdk-fbsvc-a72b6a1b3c.json"

# YOUR Google Sheet Name
SHEET_NAME = "Niha_ipd_" 

# --- 2. INITIALIZE SERVICES ---

print("Connecting to Firebase...")
# Initialize Firebase
cred = credentials.Certificate(cred_path)
firebase_admin.initialize_app(cred, {
    'databaseURL': DATABASE_URL
})

print("Connecting to Google Sheets...")
# Initialize Google Sheets
try:
    gc = gspread.service_account(filename=cred_path)
    sh = gc.open(SHEET_NAME).sheet1
    print(f"Successfully connected to Google Sheet: '{SHEET_NAME}'")
    
    # --- CHANGED ---
    # Add new, wider headers if the sheet is empty
    if not sh.get_all_values():
        # These are just generic headers. The columns will fill as needed.
        sh.append_row(["Timestamp", "Node 1", "Node 2", "Node 3", "Node 4", "Node 5", "Value"])
        print("Added new headers to empty sheet.")
        
except gspread.exceptions.SpreadsheetNotFound:
    print(f"ERROR: Spreadsheet not found. Did you name it '{SHEET_NAME}'?")
    print("Please create the sheet and try again.")
    exit()
except Exception as e:
    print(f"ERROR connecting to Google Sheets: {e}")
    print("Have you shared the sheet with the client_email?")
    exit()


# --- 3. THE LISTENER FUNCTION ---

# This function will run every time data changes
def listener(event):
    
    # We will IGNORE the big initial data dump
    if event.path == "/":
        print("--- Initial data snapshot received. Now listening for new changes... ---")
        return

    # Get a clean timestamp
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Get the path and the new data
    path_string = event.path   # e.g., "/60002231145/data/Accuracy"
    data_value = str(event.data) # e.g., "100"

    print(f"New Data: Path={path_string}, Value={data_value}")

    # --- CHANGED ---
    # This is the new logic to split the path
    
    # 1. Split the path string by '/'
    #    e.g., "/60002231145/data/Accuracy" -> ['', '60002231145', 'data', 'Accuracy']
    path_parts = path_string.split('/')
    
    # 2. Remove the first empty part (from the first '/')
    #    We use [1:] to get everything from the second item onwards
    #    e.g., -> ['60002231145', 'data', 'Accuracy']
    path_parts_cleaned = path_parts[1:]

    # 3. Build the new row
    #    Start with the timestamp
    row = [timestamp]
    
    #    Add all the path parts (this will add them as new columns)
    row.extend(path_parts_cleaned)
    
    #    Add the final value at the end
    row.append(data_value)
    
    #    Your row is now: ['2025-11-09...', '60002231145', 'data', 'Accuracy', '100']
    
    # --- END OF CHANGES ---

    # Write the row to Google Sheets
    try:
        sh.append_row(row)
        print(f"Successfully wrote to Google Sheet: {row}")
    except Exception as e:
        print(f"ERROR writing to sheet: {e}")
    
    print("--------------------------\n")


# --- 4. START THE SCRIPT ---

# We will listen at the '/User' node, as seen in your data.
print("\nListener started at /User node.")
print("Waiting for new data. Go add/change data in Firebase under /User")

db.reference('/User').listen(listener)

# Keep the script running forever
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("Stopping script.")