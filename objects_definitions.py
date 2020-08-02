import json
import datetime
import calendar
import itertools
import os
import time
import traceback
import asyncio
import aiohttp
import time
import platform
from config import *
from pprint import pprint
from urllib import request, parse


if platform.system() != "Windows":
    os.environ['TZ'] = GlobalConfig.TIMEZONE
    time.tzset()

# ============================ Between weeks functions start ================================================


def percentage_dif(from_val, to_val):
    if from_val == 0:
        return -1
    return (to_val/from_val - 1) * 100


def absolute_dif(from_val, to_val):
    return to_val - from_val


FUNC_BINDING = {
    '%dif': percentage_dif,
    'absdif': absolute_dif
}

# ============================ Between weeks functions end ================================================


# ======================== Sheet class ============================


class Sheet(object):
    def __init__(self, link, name, sheet_service, config_id, input_form, response_url, proj_params):
        self.id = convert_link(link, "d", 1)
        self.name = name
        self.reqs = []
        self.analyses = []
        self.sheet_service = sheet_service
        self.indexes = []
        self.data_to_sheet = {}
        self.config_id = config_id
        self.input_form = input_form
        self.response_url = response_url
        self.proj_params = proj_params

    def add_analysis(self):
        pass

    def add_analyses(self, from_col_pos, from_row_pos=23):
        j = from_row_pos
        num_rows = 20
        while True:
            out = self.sheet_service.spreadsheets().values().get(spreadsheetId=self.config_id,
                                                                 range=base10ToBase26Letter(from_col_pos) + str(j) + ':' +
                                                                 base10ToBase26Letter(from_col_pos+4) + str(j+20-1)).execute()
            j += 20
            if 'values' not in out:
                break
            for analysis_data in out["values"]:
                self.analyses.append(Analysis(analysis_data, self.sheet_service, self.get_prefix(
                ), self.input_form, self.response_url, self.proj_params))
            if num_rows > len(out["values"]):
                break

    async def fetch_analyses_data(self):
        # Set any session parameters here before calling `fetch`
        loop = asyncio.get_event_loop()
        print("*** fetch start ***")
        t = time.time()
        tasks = [loop.create_task(analysis.fetch())
                 for analysis in self.analyses]
        res = [str(await t)[:50] for t in tasks]
        print("*** fetch end ***", f"{time.time() - t} seconds  ", res)
        return True

    def process_analyses_data(self, requested_ixs, reqs):
        for analysis in self.analyses:
            analysis.update(self.data_to_sheet, self.indexes,
                            requested_ixs, reqs)

    def update_indexes(self):
        chr = ''.join(filter(lambda c: c.isalpha(),
                             self.proj_params.INDEXES_START_POS))
        num = int(''.join(filter(lambda c: c.isnumeric(),
                                 self.proj_params.INDEXES_START_POS)))
        data = []
        for ix_data in self.indexes:
            range = chr+str(num)
            ix_data["range"] = range
            data.append(ix_data)
            num += len(ix_data['values']) + 4

        batch_update_values_request_body = {
            'value_input_option': 'RAW',
            'data': data
        }
        self.sheet_service.spreadsheets().values().batchUpdate(spreadsheetId=self.id,
                                                               body=batch_update_values_request_body).execute()

    def update_sheets_data(self):
        data = []
        for k, val in self.data_to_sheet.items():
            val["range"] = k
            data.append(val)
        batch_update_values_request_body = {
            'value_input_option': 'RAW',
            'data': data
        }
        self.sheet_service.spreadsheets().values().batchUpdate(spreadsheetId=self.id,
                                                               body=batch_update_values_request_body).execute()

    def get_id(self):
        return self.id

    def get_link(self):
        return GlobalConfig.SHEET_BASE_URL + self.id

    def get_name(self):
        return self.name

    def get_prefix(self):
        return self.name.lower()[:3] + "_" if self.name != "" else ""

    def add_request(self):
        pass

    def batch_update(self):
        pass

    def __repr__(self):
        return "Sheet() " + self.__str__()

    def __str__(self):
        return "\n".join([self.name, self.id, str(self.analyses)])


# ======================== Analysis class ============================


class Analysis(object):
    def __init__(self, def_array, sheet_service, sheet_name_prefix, input_form, response_url, proj_params):
        if "http" in def_array[0]:
            self.id = convert_link(def_array[0], "analytics", 2).strip(' ')
        else:
            self.id = def_array[0].strip(' ')
        self.link = def_array[0].strip(' ')
        self.type = def_array[1].strip(' ')
        self.pos = def_array[2].strip(' ')

        time = def_array[4].strip(' ').split(':')
        if len(time) <= 1:
            time.append('00')
            time.append('59')
        elif len(time) <= 2:
            time.append('59')
        time = list(map(int, time))

        self.time = time
        self.date = self.__get_closest_weekday_date(def_array[3], time)

        self.exponea_type = ""
        self.name = ""
        self.request = ""
        self.weeks = proj_params.NUM_WEEKS
        self.sheet_service = sheet_service
        self.sheet_name_prefix = sheet_name_prefix
        self.input_form = input_form
        self.response_url = response_url
        self.data = []
        self.proj_params = proj_params
        self.bq_headers = []

    def get_id(self):
        return self.id

    def get_link(self):
        return GlobalConfig.SHEET_BASE_URL + self.id

    def get_name(self):
        return self.name

    def get_type(self):
        return self.type

    def __repr__(self):
        return "Analysis() " + self.__str__()

    def __str__(self):
        return "\n".join([self.name + "  " + self.id, self.link, self.type + "  " + self.pos, self.date + "  " + repr(self.time)])

    # ======== ANALYSIS class get data from server functions ==========

    async def get_basic_metrics(self, date=""):
        """
        analysis_id - id of analysis
        type - report/funnel/retention/segmentation
        date - end date, start date is preset in analysis, format is "2018,11,8,23,59,0" "yyyy,m,d,h,min,s"
        output - "rows": [
            {
                "data": [
                    33.12,
                    11.43,
                    11.43,
                    0,
                    21.69,
                    21.69,
                    0
                ],
                "type": "single",
                "value": 1530136800
            }]
            list of data
        """
        if date != "":
            date = ','.join(list(map(str, map(int, date.split(',')))))
        else:
            date = self.date

        body = {
            "analysis_id": self.id,
            "timezone": "CET",
            "format": "table_json"
        }
        if date:
            y, m, d, h, mn, s = map(int, date.split(','))
            timestamp = int(datetime.datetime(y, m, d, h, mn, s).timestamp())
            body["execution_time"] = timestamp

        if self.proj_params.ASYNC:
            print("async")
            auth = self.proj_params.AUTH_CODE
            # self.proj_params.URL = "https://webhook.site/5f4a40a2-0b1a-4c55-a3aa-08efd0eb57c0"
            # print(self.proj_params.URL + self.exponea_type, body, auth, GlobalConfig.HEADERS)
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(verify_ssl=False)) as session:
                async with session.post(self.proj_params.URL + self.exponea_type, json=body, auth=auth, headers=GlobalConfig.HEADERS, timeout=500) as resp:
                    return await resp.json()
        # else:
            # print("sync")
            # auth = self.proj_params.AUTH_CODE_SYNC
            # response = urllib.request.urlopen(urllib.request.Request(url=self.proj_params.URL + self.exponea_type,
            #                          headers=GlobalConfig.HEADERS,  auth=auth, method='POST', data=parse.urlencode(body).encode()))
            # return json.loads(response.text)

    async def get_basic_metrics_timestamp(self, date):
        tm = datetime.datetime.fromtimestamp(
            date).strftime('%Y,%m,%d,%H,%M,%S')
        d = ','.join(list(map(str, map(int, tm.split(',')))))
        return await self.get_basic_metrics(d)

    # ======== ANALYSIS class update functions ==========

    async def fetch(self):
        self.exponea_type = ""
        self.name = ""
        self.request = ""
        self.data = []
        self.bq_headers = []

        if self.type == 'retention':
            self.exponea_type = "retention"
            return await self.fetch_basic()
        elif self.type == 'report_date':
            self.exponea_type = "report"
            return await self.fetch_basic()
        elif self.type == 'report_week':
            self.exponea_type = "report"
            return await self.fetch_weeks()
        elif self.type == 'funnel_week':
            self.exponea_type = "funnel"
            return await self.fetch_weeks()
        elif "bq_" in self.type:
            return await self.fetch_bq()
        return None

    async def fetch_basic(self):
        self.data = await self.get_basic_metrics()
        # pprint(self.data)
        self.name = self.data['name']
        return self.data

    async def fetch_weeks(self):
        y, m, d, h, mn, s = map(int, self.date.split(','))
        timestamp = int(datetime.datetime(y, m, d, h, mn, s).timestamp())
        timeDelta = 60*60*24*7  # week
        for i in range(self.weeks-1, -1, -1):
            self.data.append(await self.get_basic_metrics_timestamp(timestamp - i*timeDelta))
        self.name = self.data[0]['name']
        return self.data

    async def fetch_bq(self):
        self.all_data = []
        if self.id in self.proj_params.BQ_QUERIES_CALLS:
            self.all_data = self.proj_params.BQ_QUERIES_CALLS[self.id]
        else:
            query_job = self.proj_params.BQ_CLIENT.query(
                self.proj_params.QUERIES[self.id])
            results = query_job.result()
            self.all_data = list(results)
            self.proj_params.BQ_QUERIES_CALLS[self.id] = self.all_data

        self.name = self.id + " " + self.type
        return self.all_data

    def update(self, data_to_sheet, indexes, requested_ixs, reqs):
        if self.type == 'retention':
            self.update_retention(data_to_sheet)
        elif self.type == 'report_date':
            self.update_report_by_date(data_to_sheet)
        elif self.type == 'report_week':
            self.update_report_weeks(
                indexes, reqs, requested_ixs, data_to_sheet)
        elif self.type == 'funnel_week':
            self.update_funnel_weeks(
                indexes, reqs, requested_ixs, data_to_sheet)
        elif "bq_date_" in self.type:
            self.update_bq_date(data_to_sheet)
        elif "bq_week_" in self.type:
            self.update_bq_weeks(indexes, reqs, requested_ixs, data_to_sheet)

    def update_bq_date(self, data_to_sheet):
        y, m, d, _, _, _ = map(int, self.date.split(','))
        start_date = datetime.date(y, m, d)
        start_ix = find_ix_bq(self.all_data, start_date, 0)

        suffix = self.type.split('_')[2]
        if "all" in suffix:
            end_ix = -1
        else:
            days = int(suffix) - 1
            end_date = start_date - datetime.timedelta(days=days)
            end_ix = len(self.all_data) - \
                find_ix_bq(self.all_data[::-1], end_date, 0)
            end_ix = -1 if end_ix < 0 else end_ix

        temp_data = self.all_data[start_ix: end_ix]

        temp_dict = {}
        for row in temp_data:
            category = ()
            values = []
            for k, v in row.items():
                if type(v) == datetime.date:
                    date = v.strftime('%Y-%m-%d')
                elif type(v) == str:
                    category += (v,)
                elif type(v) == float or type(v) == int:
                    values.append(v)
            if category not in temp_dict:
                temp_dict[category] = [[date] + values]
            else:
                temp_dict[category].append([date] + values)

        headers = [k for k, v in temp_data[0].items() if type(v) != str]
        processed_data = []
        for k, v in temp_dict.items():
            nm = [self.id + " " + self.type, ' '.join(k)]
            new_part = [nm + [""]*(len(headers)-len(nm)), headers] + v.copy()
            if processed_data == []:
                processed_data = new_part
                continue
            processed_data = [r1 + [""] + r2 for r1, r2 in itertools.zip_longest(
                processed_data, new_part, fillvalue=[""]*len(new_part[0]))]

        body_data = {'values': processed_data}
        data_to_sheet[self.pos] = body_data

    def update_bq_weeks(self, indexes, reqs, requested_ixs, data_to_sheet):
        y, m, d, _, _, _ = map(int, self.date.split(','))
        start_date_base = datetime.date(y, m, d)

        temp_dict = {}
        suffix = self.type.split('_')[2]
        params = suffix.split('.')
        for i in range(self.weeks-1, -1, -1):
            start_date = start_date_base - datetime.timedelta(days=i*7)
            start_ix = find_ix_bq(self.all_data, start_date, 0)
            if "all" == params[0]:
                if self.proj_params.GL_DATE == "":
                    end_ix = -1
                else:
                    da, ma, ya = map(int, self.proj_params.GL_DATE.split('-'))
                    GL_date = datetime.date(ya, ma, da)
                    end_ix = len(self.all_data) - \
                        find_ix_bq(self.all_data[::-1], GL_date, 0)
                    end_ix = -1 if end_ix < 0 else end_ix
            else:
                days = int(params[0]) - 1
                end_date = start_date - datetime.timedelta(days=days)
                end_ix = len(self.all_data) - \
                    find_ix_bq(self.all_data[::-1], end_date, 0)
                end_ix = -1 if end_ix < 0 else end_ix
            week_data = self.all_data[start_ix: end_ix]
            week_dict = {}
            for row in week_data:
                category = ()
                values = []
                isNone = False
                for k, v in row.items():
                    if type(v) == str:
                        category += (v,)
                    elif v is None:
                        isNone = True
                    elif type(v) == float or type(v) == int:
                        values.append(v)
                if not isNone:
                    if category not in week_dict:
                        week_dict[category] = (values, 1)
                    else:
                        w_cat_vals, w_cat_num = week_dict[category]
                        w_cat_vals_new = [sum(x) for x in itertools.zip_longest(
                            w_cat_vals, values, fillvalue=0)]
                        week_dict[category] = (w_cat_vals_new, w_cat_num + 1)

            for cat, w_data in week_dict.items():
                w_vals, w_num = w_data
                if "avg" == params[1]:
                    w_vals = [x/y for x, y in zip(w_vals, [w_num]*len(w_vals))]
                if cat not in temp_dict:
                    temp_dict[cat] = [w_vals]
                else:
                    temp_dict[cat].append(w_vals)

        headers = [k for k, v in self.all_data[0].items() if type(v) == float]
        processed_data = []
        for cat, values in temp_dict.items():
            ix_headers = [
                ' '.join(self.id.split(
                    '_') + self.type[3:].replace('.', '_').split('_') + list(cat) + k.split('_'))
                for k, v in self.all_data[0].items() if type(v) == float
            ]
            data_for_indexes = []
            for week_values in values:
                data_for_indexes.append({
                    "header": ix_headers,
                    "name": self.name + ' ' + ' '.join(cat),
                    "rows": [week_values]
                })

            indexes_data = self.__generate_indexes(
                data_for_indexes, reqs, requested_ixs)
            body_indexes_data = {'values': indexes_data}
            indexes.append(body_indexes_data)

            nm = [self.name, ' '.join(cat)]
            new_part = [nm + [""]*(len(headers)-len(nm)),
                        headers] + values.copy()
            if processed_data == []:
                processed_data = new_part
                continue
            processed_data = [r1 + [""] + r2 for r1, r2 in itertools.zip_longest(
                processed_data, new_part, fillvalue=[""]*len(new_part[0]))]

        body_data = {'values': processed_data}
        data_to_sheet[self.pos] = body_data

    def update_retention(self, data_to_sheet):
        def index_filter(data, indexes):
            output = []
            for ix in indexes:
                data_filtered = list(filter(lambda val: val != '', data))
                if len(data_filtered) > ix:
                    output.append(data_filtered[ix])
            return output

        data = self.data
        try:
            processed_data = [[datetime.datetime.fromtimestamp(dp[0]).strftime('%Y-%m-%d'), dp[1]] +
                              index_filter(dp[2:], self.proj_params.RETENTIONS) for dp in data['rows']]

            body_data = {'values': [['ALL Date', 'Initial step'] + list(
                map(lambda ret: 'D' + str(ret), self.proj_params.RETENTIONS))] + processed_data}
            data_to_sheet[self.pos] = body_data
        except Exception as e:
            raise MissingDataException(
                e, data, self.input_form, self.response_url)

    def update_report_by_date(self, data_to_sheet):
        data = self.data
        try:
            processed_data = [[datetime.datetime.fromtimestamp(
                int(dp[0])).strftime('%Y-%m-%d')] + dp[1:] for dp in data['rows']]
            processed_data.insert(
                0, list(map(process_header_name, data['header'])))

            body_data = {'values': processed_data}
            data_to_sheet[self.pos] = body_data
        except Exception as e:
            raise MissingDataException(
                e, data, self.input_form, self.response_url)

    def update_report_weeks(self, indexes, reqs, requested_ixs, data_to_sheet):
        data = []
        try:
            processed_data = []
            processed_data_row_names = {}
            is_more_rows = False

            for i in range(self.weeks-1, -1, -1):
                data.append(self.data[self.weeks-1-i])
                if len(data[-1]['rows']) > 1 or data[-1]['header'][0][0] != '#':
                    is_more_rows = True
                    for j in range(len(data[-1]['rows'])):
                        data[-1]['rows'][j] = none_to_zero(data[-1]['rows'][j])
                        key = data[-1]['rows'][j][0]
                        if key in processed_data_row_names:
                            processed_data_row_names[key].append(
                                data[-1]['rows'][j][1:])
                        else:
                            processed_data_row_names[key] = [
                                data[-1]['rows'][j][1:]]
                else:
                    try:
                        data[-1]['rows'][0] = none_to_zero(data[-1]['rows'][0])
                        processed_data.append(data[-1]['rows'][0])
                    except IndexError:
                        # add missing rows if data are missing
                        processed_data.append(
                            [0 for i in range(len(data[-1]['header']))])
                        data[-1]['rows'].append(processed_data[-1])

            # add missing rows if data are missing
            if is_more_rows:
                for i in range(self.weeks-1, -1, -1):
                    if data[i]['rows'] == []:
                        data[i]['rows'] = data[self.weeks-1]['rows']
                        data[i]['rows'] = list(map(lambda rw: list(
                            map(lambda x: 0 if type(x) != str else x, rw)), data[i]['rows']))
                        for j in range(len(data[i]['rows'])):
                            key = data[i]['rows'][j][0]
                            processed_data_row_names[key] = []
                            for val in data:
                                if val['rows'] != []:
                                    processed_data_row_names[key].append(
                                        val['rows'][j][1:])
                                else:
                                    processed_data_row_names[key].append([])

            if len(data[-1]['rows']) <= 1 and data[-1]['header'][0][0] == '#':
                indexes_data = self.__generate_indexes(
                    data, reqs, requested_ixs)
                body_indexes_data = {'values': indexes_data}
                indexes.append(body_indexes_data)

                processed_data.insert(
                    0, list(map(process_header_name, data[0]['header'])))
            else:
                for k, val in processed_data_row_names.items():

                    data_for_indexes = []

                    for row in val:
                        processed_data.append(row)
                        data_for_indexes.append({'header': list(map(lambda tit: tit + ' ' + k, data[0]['header'][1:])),
                                                 'rows': [row], 'name': data[0]['name'] + ' ' + k})

                    indexes_data = self.__generate_indexes(
                        data_for_indexes, reqs, requested_ixs)
                    body_indexes_data = {'values': indexes_data}
                    indexes.append(body_indexes_data)

                processed_data.insert(
                    0, list(map(process_header_name, data[0]['header'][1:])))

            body_data = {'values': processed_data}
            data_to_sheet[self.pos] = body_data
        except Exception as e:
            raise MissingDataException(
                e, data, self.input_form, self.response_url)

    def update_funnel_weeks(self, indexes, reqs, requested_ixs, data_to_sheet):
        data = []
        try:
            processed_data = []

            for i in range(self.weeks-1, -1, -1):
                data.append(self.data[self.weeks-1-i])

                for j in range(len(data[-1]['rows'])):
                    data[-1]['rows'][j] = data[-1]['rows'][j][1:]
                data[-1]['header'] = data[-1]['header'][1:]

                processed_data.append(data[-1]['rows'][0])

            processed_data.insert(
                0, list(map(process_header_name, data[0]['header'])))

            indexes_data = self.__generate_indexes(data, reqs, requested_ixs)
            body_indexes_data = {'values': indexes_data}
            indexes.append(body_indexes_data)

            body_data = {'values': processed_data}
            data_to_sheet[self.pos] = body_data
        except Exception as e:
            raise MissingDataException(
                e, data, self.input_form, self.response_url)

    # ======== ANALYSIS class other data processing help functions ==========

    def __get_closest_weekday_date(self, weekday, time):
        weekdays = list(map(lambda d: d[:3].lower(), calendar.day_name))
        weekday = weekday.lower()

        if weekday not in weekdays:
            return None

        date_now = datetime.datetime.now()
        day_now = weekdays[date_now.weekday()]
        while True:
            if day_now == weekday:
                date_out = date_now.replace(
                    hour=time[0], minute=time[1], second=time[2])
                return date_out.strftime('%Y,%m,%d,%H,%M,%S')
            date_now = date_now - datetime.timedelta(days=1)
            day_now = weekdays[date_now.weekday()]

    def __generate_indexes(self, data, reqs, requested_ixs):

        def process_name(name, num=3):
            return '_'.join(list(map(lambda pt: pt.lower()[:num], list(
                filter(lambda part: not ('#' in part and len(part) > 1),
                       name.split(' ')
                       ))
            )))

        indexes_weekly = []
        indexes_weekly_keys = []
        ld = len(data)
        for i in range(ld):
            indexes_weekly.append({})
            indexes_weekly_keys.append(['w' + str(ld-i-1)])
            for j in range(len(data[i]['header'])):
                key = self.sheet_name_prefix + \
                    process_name(data[i]['header'][j])
                if key in indexes_weekly[i]:
                    key = self.sheet_name_prefix + \
                        process_name(data[i]['header'][j], 4)
                indexes_weekly[i][key] = data[i]['rows'][0][j]
                indexes_weekly_keys[i].append(
                    '{{' + key + '_w' + str(ld-i-1) + '}}')
                while add_req_pres(key + '_w' + str(ld-i-1), data[i]['rows'][0][j], reqs, requested_ixs):
                    pass

        indexes_w2w = {}
        for w1, w2 in itertools.combinations(list(range(self.weeks)), 2):
            for key, val in indexes_weekly[w1].items():
                for f in GlobalConfig.FUNCS_BETWEEN_WEEKS:
                    ix_from, ix_to = sorted([w1, w2])

                    new_val = FUNC_BINDING[f](
                        indexes_weekly[ix_from][key], indexes_weekly[ix_to][key])
                    new_key = key + '_w' + \
                        str(self.weeks-1-ix_from) + 'w' + \
                        str(self.weeks-1-ix_to) + '_' + f
                    indexes_w2w['{{' + new_key + '}}'] = new_val
                    while add_req_pres(new_key, new_val, reqs, requested_ixs):
                        pass
        indexes_data = [
            [data[0]['name']],
            [''] + data[0]['header'],
        ]
        indexes_data.extend(indexes_weekly_keys)
        indexes_data.append(["Week2Week"] + list(indexes_w2w.keys()))
        return indexes_data


# =============== Exceptions Classes ====================


class BaseException(Exception):
    def __init__(self, e, data, input_form, response_url):
        self.e = e
        self.data = data
        self.input_form = input_form
        self.response_url = response_url
        self.text = 'Error occurred! Please try again or contact admin.'

        error_str = str(self.e) + '\n' + str(self.data) + \
            '\n' + traceback.format_exc()
        if 'socket.timeout: The read operation timed out' in error_str:
            self.text = 'Google API error - request timed out. Try your action again in more than 1 minute.'
        elif 'HttpError 500' in error_str and 'www.googleapis.com/drive' in error_str:
            self.text = 'Google Drive API error - copying presentation template failed. Try your action again in more than 1 minute.'
        elif 'Must specify at least one request' in error_str and 'slides.googleapis.com' in error_str:
            self.text = 'Your presentation template has no indices, check your presentation template and load older version with indices.'
        elif "KeyError: 'name'" in error_str:
            self.text = ("Exponea requests failed. Mark checked everything in\n" +
                         "Exponea project settings >> Access management >> API >> API Groups >> APIExport (private) >>\n" +
                         "scroll down to Group Permissions >> Everything in GET column should be checked in CUSTOMER PROPERTIES, EVENTS, DEFINITIONS, GDPR, CATALOGS.\n" +
                         "Or Exponea probably changed public or private keys to your project. Create a new key pair in your project settings and copy it to AUTH_NAME and AUTH_PASSWD in exportcfg.")
        elif 'IndexError: list index out of range' in error_str:
            self.text = 'Not enough data in your reports/data sources. Check recently added reports/data sources. You should have at least 3 weeks of data.'

        self.admin_text = ""

    def send_messages(self):
        self.send_message_to_user()
        self.send_message_to_admin()

    def send_message_to_user(self):
        slack_request(self.text, self.response_url)

    def send_message_to_admin(self):
        print('Error occurred!\n' + str(self.e) + '\n' + traceback.format_exc())
        self.admin_text = (f"Error occurred!\n project: {str(self.input_form['keyword'])}\n user_name: {str(self.input_form['user_name'])}\n" +
                           f"channel_name: {str(self.input_form['channel_name'])}\n" +
                           f"message to user: {self.text}\n" +
                           str(self.e) + '\n' + str(self.data) + '\n' + traceback.format_exc())
        slack_request(self.admin_text, GlobalConfig.SLACK_ADMIN_URL)

    def __str__(self):
        return str(self.e)


class MissingDataException(BaseException):
    def __init__(self, e, data, input_form, response_url):
        super().__init__(e, data, input_form, response_url)
        self.send_messages()

    def send_message_to_user(self):
        try:
            self.text = f'Missing data or wrong analysis format in "{self.data["name"]}"'
        except TypeError:
            self.text = f'Missing data or wrong analysis format in "{self.data[0]["name"]}"'
        slack_request(self.text, self.response_url)


class WrongConfigException(BaseException):
    def __init__(self, e, data, input_form, response_url):
        super().__init__(e, data, input_form, response_url)
        self.text = 'Wrong config format! Try to check your settings above sheets settings.'
        self.send_messages()


class WrongConfigSheetsException(BaseException):
    def __init__(self, e, data, input_form, response_url):
        super().__init__(e, data, input_form, response_url)
        self.text = 'Wrong config format! Try to check your sheets and analyses settings. Do you have 10 free columns to the right from last sheet settings?'
        self.send_messages()


class PresentationIDException(BaseException):
    def __init__(self, e, data, input_form, response_url):
        super().__init__(e, data, input_form, response_url)
        self.text = 'Error while loading presentation template! Try to check your PRESENTATION_URL in config, or presentation could be empty.'
        self.send_messages()


class PresentationCopyException(BaseException):
    def __init__(self, e, data, input_form, response_url):
        super().__init__(e, data, input_form, response_url)
        self.text = 'Error while copying presentation template! Try to check your PRESENTATION_URL in config, or contact admin.'
        self.send_messages()


class PresentationPermissionsException(BaseException):
    def __init__(self, e, data, input_form, response_url):
        super().__init__(e, data, input_form, response_url)
        self.text = 'PresentationPermissionsException occurred! Please try again or contact admin.'
        self.send_messages()


class ExponeaDataGetException(BaseException):
    def __init__(self, e, data, input_form, response_url):
        super().__init__(e, data, input_form, response_url)
        self.send_messages()


class SheetUpdateException(BaseException):
    def __init__(self, e, data, input_form, response_url):
        super().__init__(e, data, input_form, response_url)
        self.text = 'Error while updating export sheets! Check your URLs to sheets, permissions, or try again after 2 minutes. Do you have enough columns created for data export?'
        self.send_messages()


class PresentationUpdateException(BaseException):
    def __init__(self, e, data, input_form, response_url):
        super().__init__(e, data, input_form, response_url)
        self.text = 'Error while updating new presentation! Please try again or contact admin.'
        self.send_messages()


# =============== OTHER Classes and functions ===================


class ProjectParams(object):
    def __init__(self):
        self.PROJECT_TOKEN = ""
        self.PRESENTATION_ID = ''
        self.RETENTIONS = []
        self.NUM_WEEKS = 0
        self.INDEXES_START_POS = ''
        self.URL = ""
        self.AUTH_CODE = ''
        self.AUTH_CODE_SYNC = ''
        self.BQ_CLIENT = ""
        self.BQ_QUERIES_CALLS = {}
        self.BQ_NAME = ""
        self.QUERIES = ""
        self.ASYNC = True


def base10ToBase26Letter(num):
    ''' Converts any positive integer to Base26(letters only) with no 0th
    case. Useful for applications such as spreadsheet columns to determine which
    Letterset goes with a positive integer.
    '''
    if num <= 0:
        return ""
    elif num <= 26:
        return chr(64+num)
    else:
        return base10ToBase26Letter(int((num-1)/26))+chr(65+(num-1) % 26)


def convert_link(link, base_str, distance):
    # parse id from link
    path = link.split('/')
    d_ix = path.index(base_str)
    id = path[d_ix + distance]
    return id


def process_header_name(name):
    return ' '.join(list(filter(lambda part: not ('#' in part and len(part) > 1),
                                name.split(' ')
                                )))


def add_req_pres(key, value, reqs, requested_ixs, is_string=False):

    def find_ix_in_requested(key, requested_ixs):
        for i in range(len(requested_ixs)):
            sp = requested_ixs[i].split('.')
            if key == sp[0]:
                params = ''
                if len(sp) > 1:
                    params = sp[1]
                return True, i, params
        return False, -1, ""

    found, i, params = find_ix_in_requested(key, requested_ixs)

    if not found:
        return found

    del(requested_ixs[i])

    if params != '':
        if 'k' in params:
            value = int(round(value, -3) // 1000)
        if 'i' in params:
            value = int(round(value, 0))
        if 'f' in params and params[params.find('f') + 1] in '1234567890':
            value = round(value, int(params[params.find('f') + 1]))
        if 'd' in params and params[params.find('d') + 1] in '1234567890':
            value = int(
                round(value/int(params[params.find('d') + 1]), -2) // 100)/10
        params = '.' + params

    if is_string:
        repl_text = value
    else:
        repl_text = '{:,}'.format(value).replace(',', ' ')

    reqs.append(
        {'replaceAllText':
         {'replaceText': repl_text,
          "pageObjectIds": [],
          'containsText': {
              "text": '{{' + str(key) + params + '}}',
              "matchCase": False
          }}
         })
    return found


def none_to_zero(array):
    return list(map(lambda x: 0 if x == None else x, array))


def find_ix_bq(in_list, val, row_pos):
    for i in range(len(in_list)):
        if val == in_list[i][row_pos]:
            return i


def slack_request(text, response_url):
    data = GlobalConfig.SLACK_REQUEST.copy()
    data['text'] = text
    urllib.request.urlopen(urllib.request.Request(url=response_url,
                                                  headers=GlobalConfig.SLACK_HEADERS,  auth=auth, method='POST', data=parse.urlencode(data).encode()))
