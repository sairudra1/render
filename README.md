# 📊 Firebase to Google Sheets Data Logger

A simple **Python-based automation script** that transfers data from **Google Firebase** directly into a **Google Sheet**.  
With every update in Firebase, the script **appends new data as a new row** in the Google Sheet, making it ideal for logging, monitoring, and analysis.


## 📖 About the Project

This project demonstrates how data stored in **Google Firebase** can be programmatically fetched and logged into **Google Sheets** using Python.

Instead of manual exports or dashboards, this script provides:
- Automatic data transfer
- Continuous data logging
- Easy visualization and analysis using Google Sheets


## ⚙️ How It Works

1. The script connects to **Firebase** using the Admin SDK  
2. It fetches the latest data from the Firebase database  
3. The data is formatted into rows  
4. Each update is **appended** to a Google Sheet  
5. The sheet grows dynamically with every new Firebase update  


## ✨ Features

- 🔄 Automatic data fetching from Firebase
- ➕ Appends data (no overwrite)
- 📊 Seamless integration with Google Sheets
- 🧩 Simple and lightweight Python script
- 🧠 Easy to understand and modify


## 🛠️ Tech Stack

- **Language:** Python  
- **Database:** Google Firebase  
- **Spreadsheet:** Google Sheets  
- **Libraries:**  
  - `firebase-admin`  
  - `gspread`  
  - `oauth2client`

---

## 📂 Repository Structure

