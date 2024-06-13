import pandas as pd
import datetime
import yagmail
import google_sheets

pd.set_option('mode.chained_assignment', None)
now = datetime.datetime.now()
now = now.strftime("%d-%b-%y %H:%M:%S")

df = google_sheets.current[["Phone", "Email", "Company Name", "City", "State", "Assigned to", "Assigned By",
                      "Interaction Date", "Source", "industry", "status", "Account Size", "Client Objections - 1","Not interested reason"]]

df["status"] = df["status"].replace(["1 FIRST TIME ENTRY", "2 Cold Followup", "3 Hot Followup", 
                                                               "3.5 Future Followup", "4 Breakup Message", "5 Online Demo Appointment", 
                                                               "6 Appointment Fixed", "7 Followup post demo", "8 Free period implemented", 
                                                               "9 Not interested", "9.1 Sales Closed", "9.2 Case Lost", "9.3 Partner connect", 
                                                               "9.4 Direct Call", "Allocation", "Duplicate Entry", "Not contacted"], 
                                                              ["1 FTE", "2 CFU", "3 HFU", "3.5 FFU", "4 BM", "5 ODA", "6 AF", "7 FPD", 
                                                               "8 FPI", "9 NI", "9.1 SC", "9.2 CL", "9.3 PC", "9.4 DC", "Alloc", "DE", "NC"])

new_df_first = df[["Phone", "Email", "Assigned to", "Assigned By", "Interaction Date", "Source", "industry", "status", "Account Size"]]
new_df_first["First Interaction Date"] = new_df_first.sort_values(["Phone",'Interaction Date']).groupby('Phone').head(1)["Interaction Date"]
new_df_first["First Status"] = new_df_first.sort_values(["Phone",'Interaction Date']).groupby('Phone').head(1)["status"]
new_df_first["First Source"] = new_df_first.sort_values(["Phone",'Interaction Date']).groupby('Phone').head(1)["Source"]
new_df_first["First Assigned to"] = new_df_first.sort_values(["Phone",'Interaction Date']).groupby('Phone').head(1)["Assigned to"]
new_df_first["First Assigned by"] = new_df_first.sort_values(["Phone",'Interaction Date']).groupby('Phone').head(1)["Assigned By"]
new_df_first = new_df_first[new_df_first["First Interaction Date"].notna()]

new_df_last = df[["Phone", "Email", "Company Name", "Assigned to", "Interaction Date", "Source", "industry", "status", "Account Size", "Client Objections - 1","Not interested reason"]]
new_df_last["Last Interaction Date"] = new_df_last.sort_values(["Phone", "Interaction Date"]).groupby("Phone").tail(1)["Interaction Date"]
new_df_last["Last Status"] = new_df_last.sort_values(["Phone", "Interaction Date"]).groupby("Phone").tail(1)["status"]
new_df_last["Last NI Status"] = new_df_last.sort_values(["Phone", "Interaction Date"]).groupby("Phone").tail(1)["Not interested reason"]
mask = (new_df_last['Last Status'] == "9 NI") & (new_df_last['Last NI Status'] == 'No Response - Multiple Attempts')
new_df_last.loc[mask, 'Last Status'] = '9 NI-No Response - Multiple Attempts'
new_df_last["Last Assigned to"] = new_df_last.sort_values(["Phone", "Interaction Date"]).groupby("Phone").tail(1)["Assigned to"]
new_df_last["Last Source"] = new_df_last.sort_values(["Phone", "Interaction Date"]).groupby("Phone").tail(1)["Source"]
new_df_last["Last Account Size"] = new_df_last.sort_values(["Phone", "Interaction Date"]).groupby("Phone").tail(1)["Account Size"]
new_df_last = new_df_last[new_df_last["Last Interaction Date"].notna()]

df_total = pd.merge(new_df_first, new_df_last[["Phone", "Last Interaction Date", "Company Name", "Last Status", 
                                               "Last Assigned to","Last Source","Last Account Size", "Client Objections - 1"]], on="Phone", how="left")
df_total["Total Interaction Days"] = df_total["Last Interaction Date"] - df_total["First Interaction Date"]

#Staff_Hierarchy Details
df_staff1 = google_sheets.df_staff1

#Functions for Slabs
def fpi_slab(d):
    current_time = pd.to_datetime(now.date())
    if current_time - d in pd.timedelta_range(start='0 days', end='10 days', freq="D"):
        d = "10 Days"
    elif current_time - d in pd.timedelta_range(start='11 days', end='20 days', freq="D"):
        d = "11-20 Days"
    elif current_time - d in pd.timedelta_range(start='21 days', end='30 days', freq="D"):
        d = "21-30 Days"
    elif current_time - d in pd.timedelta_range(start='31 days', end='60 days', freq="D"):
        d = "31-60 Days"
    else:
        d = "60 Days"
    return d
def demo_fpd_slab(d):
    current_time = pd.to_datetime(now.date())
    if current_time - d in pd.timedelta_range(start='0 days', end='3 days', freq="D"):
        d = "3 Days"
    elif current_time - d in pd.timedelta_range(start='4 days', end='5 days', freq="D"):
        d = "4-5 Days"
    elif current_time - d in pd.timedelta_range(start='6 days', end='10 days', freq="D"):
        d = "6-10 Days"
    elif current_time - d in pd.timedelta_range(start='11 days', end='30 days', freq="D"):
        d = "11-30 Days"
    else:
        d = "30 Days"
    return d

#Health Report Script
df_health = df_total.merge(df_staff1, left_on="Last Assigned to", right_on="Assigned to", how="left", indicator=True)
df_health.drop(columns=["Interaction Date", "status", "Assigned to_x", "Assigned to_y"], inplace=True)
df_health["First Interaction Date"] = df_health["First Interaction Date"].apply(pd.to_datetime)
df_health["First Date"] = df_health["First Interaction Date"].dt.date.apply(pd.to_datetime)
df_health["Last Interaction Date"] = df_health["Last Interaction Date"].apply(pd.to_datetime)
df_health["Last Date"] = df_health["Last Interaction Date"].dt.date.apply(pd.to_datetime)
df_health["Inactive Since"] = datetime.datetime.now() - df_health["Last Interaction Date"]
#df_health["ReGenerated Lead Since"] = (datetime.datetime.now()-2) - df_health["Last Interaction Date"]
df_health["Count_Num"] = 1
print(f"Health Report with {len(df_health.index)} records created successfully")

#Unique Data Script
df_unique = df[["Phone", "Interaction Date", "status"]]
df_unique["Date"] = df_unique["Interaction Date"].dt.date
df_unique["Date"] = df_unique["Date"].apply(pd.to_datetime)
df_unique.dropna(subset=["status"], inplace=True)
df_unique.sort_values(["Phone", "Interaction Date", "status"], inplace=True)
df_temp_unique = df_unique.loc[df_unique.groupby(["Phone", "status"])["Interaction Date"].idxmin()]
df_unique_final = df_temp_unique.merge(df_health[["Phone", "Company Name", "First Assigned to", "Current Owner", "Last Status", "Last Date", "Last Source", "Last Account Size",
                                                  "Sales Manager", "Zone", "Inactive Since"]], left_on ="Phone", right_on="Phone", how="left")
print(f"Unique Report with {len(df_unique_final.index)} records created successfully")

#Ageing Report
df_demo_ageing = df_health[df_health["Last Status"].isin(["5 ODA", "6 AF"])]
df_demo_data1 = df_unique_final[df_unique_final["status"].isin(["5 ODA", "6 AF"])]
df_demo_data2 = df_demo_data1[df_demo_data1["Last Status"].isin(["5 ODA", "6 AF"])]
df_demo_final = df_demo_ageing.merge(df_demo_data2[["Phone", "Date"]], left_on="Phone", right_on = "Phone", how="left")
df_demo_final["demo_slab"] = df_demo_final["Date"].apply(demo_fpd_slab)
df_demo_table = df_demo_final.pivot_table(index=["Zone", "Current Owner"], columns=["demo_slab"], values=["Count_Num"], aggfunc=sum)

df_fpd_ageing = df_health[df_health["Last Status"].isin(["7 FPD"])]
df_fpd_data1 = df_unique_final[df_unique_final["status"].isin(["7 FPD"])]
df_fpd_data2 = df_fpd_data1[df_fpd_data1["Last Status"].isin(["7 FPD"])]
df_fpd_final = df_fpd_ageing.merge(df_fpd_data2[["Phone", "Date"]], left_on="Phone", right_on = "Phone", how="left")
df_fpd_final["fpd_slab"] = df_fpd_final["Date"].apply(demo_fpd_slab)
df_fpd_table = df_fpd_final.pivot_table(index=["Zone", "Current Owner"], columns=["fpd_slab"], values=["Count_Num"], aggfunc=sum)

df_fpi_ageing = df_health[df_health["Last Status"].isin(["8 FPI"])]
df_fpi_data1 = df_unique_final[df_unique_final["status"].isin(["8 FPI"])]
df_fpi_data2 = df_fpi_data1[df_fpi_data1["Last Status"].isin(["8 FPI"])]
df_fpi_final = df_fpi_ageing.merge(df_fpi_data2[["Phone", "Date"]], left_on="Phone", right_on = "Phone", how="left")
df_fpi_final["fpi_slab"] = df_fpi_final["Date"].apply(fpi_slab)
df_fpi_table = df_fpi_final.pivot_table(index=["Zone", "Current Owner"], columns=["fpi_slab"], values=["Count_Num"], aggfunc=sum)

print(f"Ageing Report with {len(df_demo_ageing.index)}, {len(df_fpd_ageing.index)}, {len(df_fpi_ageing.index)} records created successfully")

start_date =  pd.to_datetime(now.date() - datetime.timedelta(days = 2))
email_firststatus = ["1 FTE", "2 CFU"]
dfd_15 = df_health[df_health["First Date"] == start_date]
df_email = dfd_15[dfd_15["First Status"].isin(email_firststatus)]
email_final = ["1 FTE", "2 CFU", "NC", "9.4 DC", "4 BM"]
df_email1 = df_email[df_email["Last Status"].isin(email_final)]
df_email1 = df_email1[df_email1['Email'].str.strip().astype(bool)]
print(f"D-2 Emails Report with {len(df_email1.index)} records created successfully")

#Positive movement of clients
positive_status = ["3 HFU", "5 ODA", "6 AF", "7 FPD", "8 FPI", "9 NI", "9.1 SC", "DE"]
status_to_ignore = df_health[df_health["Last Status"].isin(positive_status)]
date_to_ignore = pd.to_datetime(now.date() - datetime.timedelta(days = 1))
date_filter = status_to_ignore[status_to_ignore["Last Date"] == date_to_ignore]

#Sales_Sourcewise performance
df_source = df_unique_final[df_unique_final["status"].isin(["7 FPD", "8 FPI", "9.1 SC"])]
df_sourcemap = google_sheets.df_sourcemap
df_finalsource = pd.merge(df_source, df_sourcemap, left_on = "Last Source", right_on="Row Labels", how="left", indicator="True")
df_finalsource["Date"] = df_finalsource["Date"].apply(pd.to_datetime)
df_finalsource["Count_num"] = 1
df_finalsource_Jun24 = df_finalsource[df_finalsource["Date"].dt.strftime("%Y-%m") == "2024-06"]
print(f"Sales Source Report with {len(df_finalsource.index)} records created successfully")

#Lead Generation Report
df_leadgen = df_unique_final[df_unique_final["status"].isin(["5 ODA", "6 AF"])]
df_leadgen["Count_Num"] = 1
df_leadgen["Date"] = df_leadgen["Date"].apply(pd.to_datetime)
df_leadgen_Jun24 = df_leadgen[df_leadgen["Date"].dt.strftime("%Y-%m") == "2024-06"]
df_leadgen_May24 = df_leadgen[df_leadgen["Date"].dt.strftime("%Y-%m") == "2024-05"]
df_leadgen_Apr24 = df_leadgen[df_leadgen["Date"].dt.strftime("%Y-%m") == "2024-04"]
df_leadgen_Mar24 = df_leadgen[df_leadgen["Date"].dt.strftime("%Y-%m") == "2024-03"]
print(f"Lead generation Report with {len(df_leadgen.index)} records created successfully")

#Overall_FPI_Report
df_overall_fpi = df_unique_final[df_unique_final["status"] == "8 FPI"]
df_overall_fpi["Date"] = df_overall_fpi["Date"].apply(pd.to_datetime)
print(f"Overall FPI Report with {len(df_overall_fpi.index)} records created successfully")

#Runo Objections
df_health["Last Date"] = df_health["Last Date"].apply(pd.to_datetime)
runo_objections = df_health[df_health["Last Date"] > "2023-12-01"]
runo_final_objections = runo_objections[["Phone", "Company Name", "Last Status", "Current Owner", "Last Source", "Last Account Size", "Client Objections - 1", "Last Date","Zone"]]
print(f"Objections Report with {len(runo_final_objections.index)} records created successfully")

with pd.ExcelWriter("runo_healthreport.xlsx") as writer:
    df_health.to_excel(writer, sheet_name="health_data",index=False)
    df_unique_final.to_excel(writer, sheet_name="unique_data",index=False)
    
with pd.ExcelWriter("runo_otherreports.xlsx") as writer:    
    runo_final_objections.to_excel(writer, sheet_name="Objections", index=False)
    df_overall_fpi.to_excel(writer, sheet_name="Overall FPI", index=False)
    df_demo_final.to_excel(writer,sheet_name="ODA&AF",index=False)
    df_fpd_final.to_excel(writer,sheet_name="FPD",index=False)
    df_fpi_final.to_excel(writer,sheet_name="FPI",index=False)


with pd.ExcelWriter("D2&PSWE.xlsx") as writer:    
    df_email1.to_excel(writer, sheet_name="df_email1.xlsx", index=False)
    date_filter.to_excel(writer, sheet_name="positive_data", index=False)
    
with pd.ExcelWriter("runo_Leadgen_sourcewise.xlsx") as writer:
    df_leadgen_Jun24.to_excel(writer, sheet_name="Jun 2024", index=False)
    df_leadgen_May24.to_excel(writer, sheet_name="May 2024", index=False)
    df_leadgen_Apr24.to_excel(writer, sheet_name="Apr 2024", index=False)
    df_leadgen_Mar24.to_excel(writer, sheet_name="Mar 2024", index=False)

with pd.ExcelWriter("sales_sourcewise.xlsx") as writer:
    df_finalsource_Jun24.to_excel(writer, sheet_name="sales_source_Jan24", index=False)

print("All scripts are executed properly")

yagmail.register("reports.mis@runo.in", "Runo@2022@#")
yag = yagmail.SMTP("reports.mis@runo.in", "Runo@2022@#")
yag.send(["devendarrao.l@runo.in", "reports.mis@runo.in"], 
         f"A & P Report - {now}", "Sharing you the report of Health, Unique and Source wise", attachments=["runo_healthreport.xlsx", "runo_otherreports.xlsx"])
yag.send("saikarthik.k@runo.in", f"D-2 Emails & Marketing Data - {now:%d-%B-%Y}", "FYI", attachments=["D2&PSWE.xlsx"])
yag.send(["reports.mis@runo.in", "devendarrao.l@runo.in"], f"Sourcewise Pivot Tables - November 22 & December 22 - {now:%d-%B-%Y}", "FYI", attachments=["runo_Leadgen_sourcewise.xlsx"])

print("Report is Sent Successfully")

#now:%d-%B-%Y