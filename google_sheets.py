import gspread
import pandas as pd
import datetime


gc = gspread.service_account("secret.json")

current1 = pd.DataFrame.from_dict(gc.open("Runo BD Process (Apr'23-Aug'23)").get_worksheet(0).get_all_records())
current2 = pd.DataFrame.from_dict(gc.open("Runo BD Process (Sep'23-Dec'23)").get_worksheet(0).get_all_records())
current3 = pd.DataFrame.from_dict(gc.open("Runo BD Process (Jan'24-Apr'24)").get_worksheet(0).get_all_records())

#interactions_data1 = google_sheets.current1
current1["Interaction Date"] = pd.to_datetime(current1["Interaction Date"], dayfirst=True)
current1.rename(columns= {"Mobile Number": "Phone", "Assigned To": "Assigned to", "Industry": "industry"}, inplace=True)

#current2 = google_sheets.current2
current2["Interaction Date"] = pd.to_datetime(current2["Interaction Date"], dayfirst=True)
current2.rename(columns= {"Mobile Number": "Phone", "Assigned To": "Assigned to", "Industry": "industry"}, inplace=True)

#current3 = google_sheets.current3
current3["Interaction Date"] = pd.to_datetime(current3["Interaction Date"], dayfirst=True)
current3.rename(columns= {"Mobile Number": "Phone", "Assigned To": "Assigned to", "Industry": "industry"}, inplace=True)

current = pd.concat([current1,current2, current3], axis=0, ignore_index=True)
print(f"Google Sheet with {len(current.index)} records imported successfully")
now = datetime.datetime.now()
print(f"Current time is {now}")

df_staff1 = pd.DataFrame.from_dict(gc.open("Staff Hierarchy").get_worksheet(0).get_all_records())

df_sourcemap = pd.DataFrame.from_dict(gc.open("Source Mapping").get_worksheet(0).get_all_records())

df_lg_staff1 = pd.DataFrame.from_dict(gc.open("Lead Gen Team Reporting").get_worksheet(0).get_all_records())
