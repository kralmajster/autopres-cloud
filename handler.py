from __future__ import print_function
# import requests
import datetime
import os
import time
import logging
import asyncio
import platform
import json
import aiohttp
from config import *
from objects_definitions import *
from pprint import pprint
# from quart import Quart, request, Response
from requests.auth import HTTPBasicAuth
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from google.cloud import bigquery
from google.oauth2 import service_account


def hello(event, context):
    print(event)
    input_form = event['body']
    if input_form is not None:
        keyword = str(input_form['text']) + '.exportcfg'
        command = str(input_form['command']).strip('/')
        response_url = str(input_form['response_url'])
        user_name = str(input_form['user_name'])
        channel_name = str(input_form['channel_name'])

    params = {
        "keyword": keyword,
        "command": command,
        "response_url": response_url,
        "user_name": user_name,
        "channel_name": channel_name
    }

    if command == "createpreshelp":
        return {
            "statusCode": 200,
            "body": "Auto Presentation manual link: https://docs.google.com/document/d/1nbNpVm6SEU_ogtY-3HtoIYxgX5DudDdzJK6Q19wpaaE/edit?usp=sharing"
        }

    loop = asyncio.get_event_loop()
    fut = loop.create_future()
    loop.create_task(
        create_pres(fut, params)
    )

    return {
        "statusCode": 200,
        "body": "Processing /" + command + " command for " + keyword +
        " config. It can take up to 1 minute."
    }


async def create_pres(fut, input_form):

    try:
        fut.set_result("ok")
        if input_form is not None:
            keyword = str(input_form['keyword'])
            command = str(input_form['command'])
            response_url = str(input_form['response_url'])

        proj_params = ProjectParams()

        creds = ServiceAccountCredentials.from_json_keyfile_name(
            'ExponeaAPIExport-c8a61ccb0acf.json', scopes=GlobalConfig.SCOPES)

        pres_service = build('slides', 'v1', credentials=creds)
        sheet_service = build('sheets', 'v4', credentials=creds)
        drive_service = build('drive', 'v3', credentials=creds)

        credentials = service_account.Credentials.from_service_account_file(
            'ExponeaAPIExport-c8a61ccb0acf.json')
        proj_params.BQ_CLIENT = bigquery.Client(
            credentials=credentials, project=GlobalConfig.BQ_PROJECT_ID)
        proj_params.BQ_QUERIES_CALLS = {}
        proj_params.ASYNC = True

        found_files = {}
        # --- Call the Drive v3 API
        page_token = None
        while True:
            response = drive_service.files().list(q="mimeType='application/vnd.google-apps.spreadsheet' and name contains '.exportcfg'",
                                                  spaces='drive',
                                                  fields='nextPageToken, files(id, name)',
                                                  pageToken=page_token).execute()
            for file in response.get('files', []):
                print('Found file: %s (%s)' %
                      (file.get('name'), file.get('id')))
                found_files[file.get('name')] = (file.get('id'))

            page_token = response.get('nextPageToken', None)
            if page_token is None:
                break

        if keyword in found_files:
            config_id = found_files[keyword]
        else:
            slack_request("Config " + keyword + " not found!", response_url)
            return

        # ---------- Get config values ----------
        try:
            ranges = ['A2', 'E2', 'A5', 'H5', 'A8',
                      'A11', 'A14', 'A17', 'D11', 'D14']
            config_output = sheet_service.spreadsheets().values().batchGet(
                spreadsheetId=config_id, ranges=ranges).execute()

            proj_params.PROJECT_TOKEN = config_output['valueRanges'][0]['values'][0][0].strip(
                ' ')

            base_url = config_output['valueRanges'][1]['values'][0][0].strip(
                ' ')
            proj_params.URL = base_url + "/data/v2/projects/" + \
                proj_params.PROJECT_TOKEN + "/analyses/"

            auth_name = config_output['valueRanges'][2]['values'][0][0].strip(
                ' ')
            auth_passwd = config_output['valueRanges'][3]['values'][0][0].strip(
                ' ')

            proj_params.AUTH_CODE = aiohttp.BasicAuth(auth_name, auth_passwd)
            proj_params.AUTH_CODE_SYNC = HTTPBasicAuth(auth_name, auth_passwd)

            proj_params.PRESENTATION_ID = convert_link(
                config_output['valueRanges'][4]['values'][0][0].strip(' '), "d", 1)

            proj_params.RETENTIONS = list(
                map(int, config_output['valueRanges'][5]['values'][0][0].strip(' ').split(',')))

            proj_params.NUM_WEEKS = int(
                config_output['valueRanges'][6]['values'][0][0].strip(' '))

            proj_params.INDEXES_START_POS = config_output['valueRanges'][7]['values'][0][0].strip(
                ' ')

            try:
                proj_params.BQ_NAME = config_output['valueRanges'][8]['values'][0][0].strip(
                    ' ')
            except KeyError:
                proj_params.BQ_NAME = ""

            proj_params.QUERIES = {k: v.replace(
                "(****)", proj_params.BQ_NAME) for k, v in GlobalConfig.QUERIES.items()}

            try:
                proj_params.GL_DATE = config_output['valueRanges'][9]['values'][0][0].strip(
                    ' ')
            except KeyError:
                proj_params.GL_DATE = ""

        except Exception as e:
            raise WrongConfigException(
                e, config_output, input_form, response_url)

        try:
            sheets = []
            i = 1
            while True:
                ranges = [base10ToBase26Letter(
                    i) + "20", base10ToBase26Letter(i+3) + "19"]
                config_output = sheet_service.spreadsheets().values().batchGet(
                    spreadsheetId=config_id, ranges=ranges).execute()

                out_link = config_output['valueRanges'][0]
                if 'values' not in out_link:
                    break

                out_name = config_output['valueRanges'][1]
                if 'values' not in out_name:
                    name = ""
                else:
                    name = out_name['values'][0][0]

                sheets.append(Sheet(out_link['values'][0][0], name, sheet_service,
                                    config_id, input_form, response_url, proj_params))
                sheets[-1].add_analyses(i, 23)
                i += 9
        except Exception as e:
            raise WrongConfigSheetsException(
                e, config_output, input_form, response_url)

        slack_request("10% |=_________|", response_url)

        # --- Call the Slides API
        try:
            presentation = pres_service.presentations().get(
                presentationId=proj_params.PRESENTATION_ID).execute()

            slides = presentation.get('slides')

            slides_text = str(slides)
            requested_ixs = parse_ixs_from_presentation(slides_text)
            # pprint(requested_ixs)
        except Exception as e:
            raise PresentationIDException(
                e, proj_params.PRESENTATION_ID, input_form, response_url)

        if len(requested_ixs) <= 0 and (command == "createpres" or command == "updatepres"):
            slack_request(
                "Presentation from config file doesn't contain any indexes!", response_url)
            return

        if command == "createpres":
            try:
                body = {
                    'name': presentation.get('title').replace('[Template]', '').strip(' ') + " (" + datetime.datetime.fromtimestamp(datetime.datetime.now().timestamp()).strftime('%d.%m.%Y') + ")"
                }
                drive_response = drive_service.files().copy(
                    fileId=proj_params.PRESENTATION_ID, body=body).execute()
                presentation_copy_id = drive_response.get('id')
                proj_params.PRESENTATION_ID = presentation_copy_id

                presentation = pres_service.presentations().get(
                    presentationId=proj_params.PRESENTATION_ID).execute()
            except Exception as e:
                raise PresentationCopyException(
                    e, proj_params.PRESENTATION_ID, input_form, response_url)

            try:
                body = {
                    "role": "writer",
                    "type": "user",
                    "emailAddress": "martin.miklis@cellense.com"
                }
                drive_service.permissions().create(fileId=proj_params.PRESENTATION_ID,
                                                   sendNotificationEmail=False, body=body).execute()
            except Exception as e:
                pass

        loop = asyncio.get_event_loop()
        print('** Adding data to spreadsheet')
        #proj_params.ASYNC = False
        try:
            t = time.time()
            tasks = [loop.create_task(sheet.fetch_analyses_data())
                     for sheet in sheets]
            res = [str(await t) for t in tasks]
            print("*** fetch after await ***", f"{time.time() - t} seconds  ")
        except MissingDataException as e:
            raise e
        except:
            proj_params.ASYNC = False
            t = time.time()
            tasks = [loop.create_task(sheet.fetch_analyses_data())
                     for sheet in sheets]
            res = [str(await t) for t in tasks]
            print("*** sync fetch after await ***",
                  f"{time.time() - t} seconds  ")

        reqs = []

        # --- Add date
        add_req_pres("date_now", datetime.datetime.fromtimestamp(
            datetime.datetime.now().timestamp()).strftime('%d.%m.%Y'), reqs, requested_ixs, True)

        for sheet in sheets:
            sheet.process_analyses_data(requested_ixs, reqs)

        slack_request("50% |====D_____|", response_url)

        print('** Procesing data to spreadsheet finished')

        try:
            # --- Update data in sheets
            for sheet in sheets:
                sheet.update_sheets_data()

            # --- Update indexes
            for sheet in sheets:
                sheet.update_indexes()
        except Exception as e:
            raise SheetUpdateException(e, [], input_form, response_url)

        print('** Adding data to spreadsheet - Done')

        # --- Update presentation
        try:
            if command != "updatesheet":
                print('** Adding data to presentation')

                pres_service.presentations().batchUpdate(body={'requests': reqs},
                                                         presentationId=proj_params.PRESENTATION_ID, fields='').execute()
                print('** Adding data to presentation - Done')
        except Exception as e:
            raise PresentationUpdateException(
                e, proj_params.PRESENTATION_ID, input_form, response_url)

        slack_request("100%|=========D|", response_url)

        print('Done')
    except BaseException as e:
        return
    except Exception as e:
        exc = BaseException(e, [], input_form, response_url)
        exc.send_messages()
        return

    res_text = (f"Presentation URL: {GlobalConfig.PRES_BASE_URL + proj_params.PRESENTATION_ID} \nSpreadsheet URLs: \n" +
                "\n".join([sheet.get_name() + " " + sheet.get_link() for sheet in sheets]))
    slack_request(res_text, response_url)
    return


# ============================ create_pres() end ================================================


def parse_ixs_from_presentation(pres_json_str):
    str_on = False
    w_on = False
    word = ''
    words = []
    for i in range(len(pres_json_str)-1):
        c1, c2 = pres_json_str[i], pres_json_str[i+1]
        if c1 == "'":
            str_on = not str_on
        if str_on:
            if c1 == '{' and c2 == '{':
                w_on = True
            elif w_on and c1 == '}' and c2 == '}':
                words.append(word)
                w_on = False
                word = ''
            elif w_on and c2 != '}':
                word += c2
    return words


def get_dates_from_retention(data, start, end):
    """
    from - date from "2018,11,8,23,59,0" "yyyy,m,d,h,min,s"
    to - date to "2018,11,8,23,59,0" "yyyy,m,d,h,min,s"
    """
    y, m, d, h, mn, s = map(int, start.split(','))
    start = int(datetime.datetime(y, m, d, h, mn, s).timestamp())

    y, m, d, h, mn, s = map(int, end.split(','))
    end = int(datetime.datetime(y, m, d, h, mn, s).timestamp())

    return list(filter(lambda datapoint: datapoint['date'] >= start and datapoint['date'] <= end, data))


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8081, debug=True)
