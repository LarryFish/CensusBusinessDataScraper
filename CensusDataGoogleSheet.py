import googlemaps
import json
import requests
from pprint import pprint
import httplib2
import os
import time
import csv
from apiclient import discovery
from apiclient.discovery import build
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage
try:
    import argparse
    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
    flags = None

#Add Functionality for Any Year
#Run for several years and use Tableau to visualize the data
#Rewrite version to be used with CSV output; Google Spreadsheets have limitations/inefficiencies

#List of Zipcodes to Find Business Data for
ZipCodes = [77845]
#Year to pull business data from
DataYear = 2015
#Census Data Variable Labels/Descriptions
EMP = "Total Number of Employees"
EMP_F = "Flag for Number of Employees"
EMP_N = "Noise Flag for Total Mid-March Employees"
EMP_N_F = "Flag for Noise Field for Total Mid-March Employees"
EMPSZES = "Employment Size of Establishment"
EMPSZES_TTL = "Title of Employment Size"
ESTAB = "Total Number of Establishments"
ESTAB_F = "Flag for Total Number of Establishments"
FOOTID_GEO = "FootID of Geography"
FOOTID_NAICS = "FootID of NAICS Code"
GEO_ID = "ID of Geography"
GEO_TTL = "Title of Geography"
GEOTYPE = "Geography"
NAICS2012 = "NAICS Industry Code"
NAICS2012_TTL = "Title of NAICS Industry Code"
PAYANN = "Total Annual Payroll"
PAYANN_F = "Flag for Total Annual Payroll"
PAYANN_N = "Total Annual Payroll with Noise"
PAYANN_N_F = "Flag for Noise Field for Total Annual Payroll"
PAYQTR1 = "Total First Quarter Payroll"
PAYQTR1_F = "Flag for Total First Quarter Payroll"
PAYQTR1_N = "Noise Flag for Total First Quarter Payroll Data"
PAYQTR1_N_F = "Flag for Noise Field for Total First Quarter Payroll"
ST = "FIPS State Code"
YEAR = "Year"
ZIPCODE = "FIPS Zip" 


def get_credentials():
    #Needed for creation of credentials with scope
    SCOPES = 'https://www.googleapis.com/auth/spreadsheets'
    CLIENT_SECRET_FILE = 'client_secret.json'
    APPLICATION_NAME = 'Google Sheets API Python Quickstart'
    #User Authorization Credentials
    """Gets valid user credentials from storage.
    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.
    Returns: Credentials, the obtained credential.
    """
    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.credentials')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir,'sheets.googleapis.com-python-quickstart.json')
    store = Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
        flow.user_agent = APPLICATION_NAME
        if flags:
            credentials = tools.run_flow(flow, store, flags)
        else: # Needed only for compatibility with Python 2.6
            credentials = tools.run(flow, store)
        print('Storing credentials to ' + credential_path)
    return credentials

def write_header(sheet,Headers):
    #Write the column names in the first row
    #Basics for Sheets API Usage
    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())
    discoveryUrl = ('https://sheets.googleapis.com/$discovery/rest?''version=v4')
    service = discovery.build('sheets', 'v4', http=http, discoveryServiceUrl=discoveryUrl)
    #ID variable of spreadsheet to update
    spreadsheetId = "11l2NUmvsRWdg8BPyk1zCwydNgqKJTIkeYUzXtsiZw34"
    rangeName = '%s!A1:G1' % (sheet)
    #How input data should be interpreted
    value_input_option = 'USER_ENTERED'
    #Entry Data
    values = [Headers]
    body = {'values': values}
    #Request service and execute response
    request = service.spreadsheets().values().update(spreadsheetId=spreadsheetId, range=rangeName, valueInputOption=value_input_option, body=body).execute()


def get_MetaData():

    sheet = "MetaData"
    count = 2
    Headers = [EMP,ESTAB,PAYANN,ZIPCODE]
    
    write_header(sheet,Headers)
    
    for zipcode in ZipCodes:

        urlMetaData = "https://api.census.gov/data/"+"%s" % (DataYear)+"/zbp?get=EMP,ESTAB,PAYANN&for=zipcode:"+"%s" % (zipcode)+"&key=78c725ef5d0decad9de9b18cd285c1bf4b45a332"
        responseMetaData = requests.get(urlMetaData)
        stringMetaData = responseMetaData.text
        jsonMetaData = json.loads(stringMetaData)
        pprint(jsonMetaData)
        #Basics for Sheets API Usage
        credentials = get_credentials()
        http = credentials.authorize(httplib2.Http())
        discoveryUrl = ('https://sheets.googleapis.com/$discovery/rest?''version=v4')
        service = discovery.build('sheets', 'v4', http=http, discoveryServiceUrl=discoveryUrl)
        #ID variable of spreadsheet to update
        spreadsheetId = "11l2NUmvsRWdg8BPyk1zCwydNgqKJTIkeYUzXtsiZw34"
        rangeName = '%s!A%d:G%d' % (sheet, count, count)
        count += 1
        #How input data should be interpreted
        value_input_option = 'USER_ENTERED'
        #Entry Data
        values = [jsonMetaData[1]]
        body = {'values': values}
        #Request service and execute response
        request = service.spreadsheets().values().update(spreadsheetId=spreadsheetId, range=rangeName, valueInputOption=value_input_option, body=body).execute()

def get_SectorAndSize():  
    #Variables for write_header method
    sheet = "SectorAndSize"
    Headers = [EMPSZES_TTL,EMPSZES,ESTAB,NAICS2012_TTL,NAICS2012,ZIPCODE]
    #Count used to change sheet row; leaving row 1 open for headers
    count = 2
    #Call on write_header method
    write_header(sheet,Headers)
    #Loop to write header values
    for zipcode in ZipCodes:
        urlBySectorAndSize = "https://api.census.gov/data/"+"%s" % (DataYear)+"/zbp?get=EMPSZES_TTL,EMPSZES,ESTAB,NAICS2012_TTL,NAICS2012&for=zipcode:"+"%s" % (zipcode)+"&key=78c725ef5d0decad9de9b18cd285c1bf4b45a332"
        responseSectorAndSize = requests.get(urlBySectorAndSize)
        stringSectorAndSize = responseSectorAndSize.text
        jsonSectorAndSize = json.loads(stringSectorAndSize)
        pprint(stringSectorAndSize) 
        for array in jsonSectorAndSize:
            if array != jsonSectorAndSize[0]:
                #Basics for Sheets API Usage
                credentials = get_credentials()
                http = credentials.authorize(httplib2.Http())
                discoveryUrl = ('https://sheets.googleapis.com/$discovery/rest?''version=v4')
                service = discovery.build('sheets', 'v4', http=http, discoveryServiceUrl=discoveryUrl)
                #ID variable of spreadsheet to update
                spreadsheetId = "11l2NUmvsRWdg8BPyk1zCwydNgqKJTIkeYUzXtsiZw34"
                rangeName = '%s!A%d:G%d' % (sheet, count, count)
                #How input data should be interpreted
                value_input_option = 'USER_ENTERED'
                #Entry Data
                values = [array]
                body = {'values': values}
                if array[2] != "0":
                    #Request service and execute response
                    request = service.spreadsheets().values().update(spreadsheetId=spreadsheetId, range=rangeName, valueInputOption=value_input_option, body=body).execute()
                    count += 1
                    time.sleep(.5)

get_MetaData()
get_SectorAndSize()
