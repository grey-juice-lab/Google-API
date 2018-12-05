#!/usr/local/bin/python3.7

import gspread
import time

from oauth2client.service_account import ServiceAccountCredentials

ORIGIN_SHEET_KEY = '1Y2q8EdZK-QkiRJo_xL6L66N1zDfsiEZ2lmd68A90eyU'
WORKSHEETS_NOT_WANTED = [1989266713,1831617327,672807133]
DESTINATION_SHEET_KEY = '1ZnUrsv9PR2GEfU5WZfQkJmxkt3ZCnZ-IJnSbqEUobcE'
STARTING_HEADER_COLUMN_TITLE= "TERM YR"
COLUMNS_TO_CHECK = ["Redelivery", "Feature"]
DESTINATION_HEADER = ("TERM YR", "ID", "Title Original", "Title Spanish", "VOD Start", "VOD End", "Provider-Right")

#CREDENTIALS
scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('Spreadsheet_test_api.json', scope)
client = gspread.authorize(creds)


def start_row(content): #Función que devuelve cuando empieza la data 
	row = 0
	index_row = 0
	for rows in content:
		if STARTING_HEADER_COLUMN_TITLE in content[row]:
			index_row = row + 1
		else:
			row = row + 1	
	return index_row
	
def extract_header(data):
	header = start_row(data) - 1


def get_rows(sheet):
	worksheet_list = sheet.worksheets()
	rows =[] 
	for sheet in worksheet_list:
		if sheet.id in WORKSHEETS_NOT_WANTED:
			print(sheet,"not wanted")
		else:
			print("collecting data from ", sheet)
			values =  sheet.get_all_values() #API has also get_all_records
			header = extract_header(values) 
			startrow = start_row(values)
			for values in values[startrow:]:
				rows.append(
					{"term_year" : values[0],
					"id" : values [1],
					"original_title" : values[2],
					"spanish_title" : values[3],
					"confirmed_tentative" : values[4],
					"redelivery" : values[5],
					"VOD_start" : values[6],
					"VOD_end" : values[7],
					"feature" : values[9],
					"sheet_name" : sheet.title
					})
	return rows

def row_is_valid(row):
	is_not_redelivery = row["redelivery"].strip().upper() != "Y"
	is_not_cancelled = row["feature"].strip() != "Cancelled" 
	return is_not_redelivery and is_not_cancelled	

def filter_valid_rows(rows):
	filtered_rows = []
	for row in rows:
		if row_is_valid(row):
			 filtered_rows.append(row)
	return filtered_rows

def send_rows(rows, sheet):
	ignored_columns = ("feature", "redelivery", "confirmed_tentative")
	
	worksheet = sheet.worksheets()[0] # first tab in destination sheet
		
	worksheet.clear()
	
	worksheet.insert_row(DESTINATION_HEADER, 1)

	print("Sending", len(rows), "rows")

	for index, row in enumerate(rows, start=2):
		out_row = []
		
		for col in ignored_columns:
			row.pop(col)

		for value in row.values():
			out_row.append(value)

		worksheet.insert_row(out_row, index)
		time.sleep(1)

	print("DONE")
		


original_sheet = client.open_by_key(ORIGIN_SHEET_KEY)
rows = filter_valid_rows(get_rows(original_sheet))

destination_sheet = client.open_by_key(DESTINATION_SHEET_KEY)
send_rows(rows, destination_sheet)

print(rows) 
	
#to do
# get only the wanted columns
# IF COLUMN REDELIVEYR IS "Y" IGNORE ROW
# IF COLUMN FEATURE IS "CANCELLED" IGNORE ROW
# join the wanted columns with 1 header
# add 1 columns with #worksheet name 
#PUT this data in the destiny worksheet
#set this program in our server (windows in amazon) and run every 2 hours.
#email si hay problemas & e-mail informe todo ha ido bien.	
	
#import ipdb; ipdb.set_trace()
