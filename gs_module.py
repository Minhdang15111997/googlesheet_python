import pickle
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.auth.exceptions import *
import os
import logging
import time


class GS_MODULE(object):
    def __init__(self):
        super(GS_MODULE, self).__init__()
        try:
            SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
            creds = None
            creds_path = os.path.dirname(os.path.realpath(__file__)).replace('python_file\config', 'etc').__add__('\\credentials-noreplyasia.json')
            token_path = os.path.dirname(os.path.realpath(__file__)).replace('python_file\config', 'etc').__add__('\\token-noreplyasia-gs.pickle')

            if os.path.exists(token_path):
                with open(token_path, 'rb') as token:
                    creds = pickle.load(token)
            # If there are no (valid) credentials available, let the user log in.
            if not creds or not creds.valid:
                if creds and creds.expired and creds.refresh_token:
                    creds.refresh(Request())
                else:
                    flow = InstalledAppFlow.from_client_secrets_file(creds_path, SCOPES)
                    creds = flow.run_local_server(port=0)
                # Save the credentials for the next run
                with open(token_path, 'wb') as token:
                    pickle.dump(creds, token)

            self.service = build('sheets', 'v4', credentials=creds, cache_discovery=False)
        except GoogleAuthError as e:
            print("Google Auth Error", e)
            self.service = None

    def get(self, spreadsheet_id, range=None):
        try:
            if self.service is not None:
                if range is not None:
                    sheet = self.service.spreadsheets()
                    result = sheet.values().get(spreadsheetId=spreadsheet_id, range=range).execute()
                    return result
                else:
                    sheet = self.service.spreadsheets()
                    result = sheet.get(spreadsheetId=spreadsheet_id).execute()
                    return result

            return False
        except Exception as e:
            print("Google Sheet GET {0} - {1}, error: {2}".format(spreadsheet_id, range, e))
            return False

    def batch_get(self, spreadsheet_id, range=None,render_option=None):
        try:
            if self.service is not None:
                sheet = self.service.spreadsheets()
                if render_option:
                    result = sheet.values().batchGet(spreadsheetId=spreadsheet_id, ranges=range,**render_option).execute()
                    return result
                elif range:
                    result = sheet.values().batchGet(spreadsheetId=spreadsheet_id, range=range).execute()
                    return result
                else:
                    result = sheet.batchGet(spreadsheetId=spreadsheet_id).execute()
                    return result

            return False
        except Exception as e:
            print("Google Sheet BATCH GET {0} - {1}, error: {2}".format(spreadsheet_id, range, e))
            return False

    def post(self, data_array, spreadsheet_id, range, overwrite=0):
        retry = 0
        is_success = 0
        while retry <= 5 and is_success == 0:
            try:
                if self.service is not None:
                    sheet = self.service.spreadsheets()
                    body = {
                        'values': data_array
                    }
                    if overwrite == 1:

                        # clear
                        clear_body = {}
                        sheet.values().clear(spreadsheetId=spreadsheet_id, range=range, body=clear_body).execute()

                        # overwrite
                        result = sheet.values().update(spreadsheetId=spreadsheet_id, range=range,
                                                       valueInputOption="USER_ENTERED", body=body).execute()

                    else:
                        # append
                        result = sheet.values().append(spreadsheetId=spreadsheet_id, range=range,
                                                       valueInputOption="USER_ENTERED", body=body).execute()

                    is_success = 1
                    return result
                is_success = 0
                retry += 1
                print('Google Sheet Retry Post (Service Return None) . . .')
                time.sleep(1)
            except Exception as e:
                print("Exception Google Sheet Post {0} - {1}: {2}".format(spreadsheet_id, range, e))
                is_success = 0
                retry += 1
                print('Google Sheet Retry Post . . .')
                time.sleep(1)

        if is_success == 0:
            return False

    def add_sheet(self, spreadsheet_id, title, rows, columns):
        try:
            if self.service is not None:
                sheet = self.service.spreadsheets()
                request = {
                    "addSheet": {
                        "properties":
                            {'title': title,
                             "gridProperties":
                                 {"rowCount": rows,
                                  "columnCount": columns
                                  }
                             }
                    }
                }
                body = {'requests': [request]}
                sheet.batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
                print("Add Sheet Success {0}".format(title))

        except Exception as e:
            print("Google Sheet Add Sheet {0} - {1}, error: {2}".format(spreadsheet_id, range, e))

    def delete_sheet(self, spreadsheet_id, sheet_id):
        try:
            if self.service is not None:
                sheet = self.service.spreadsheets()
                request = {
                    "deleteSheet": {
                        "sheetId": sheet_id
                    }
                }
                body = {'requests': [request]}
                sheet.batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
        except Exception as e:
            print("Google Sheet Delete Sheet {0} - {1}, error: {2}".format(spreadsheet_id, range, e))

    def clear(self, spreadsheet_id, range_name):
        try:
            if self.service is not None:
                sheet = self.service.spreadsheets()
                clear_body = {}

                sheet.values().clear(spreadsheetId=spreadsheet_id, range=range_name, body=clear_body).execute()

        except Exception as e:
            print("Google Sheet Clear Sheet {0} - {1}, error: {2}".format(spreadsheet_id, range, e))