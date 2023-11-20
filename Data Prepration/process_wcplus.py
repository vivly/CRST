import requests
import time
import re
import xml.etree.ElementTree as ET
import pandas as pd

wcplus_data_path = '../Data/wcplus'
wcplus_processed_data_path = '../Data/wcplus processed'
risk_key_word = '风险'
incident_key_word = '病例'
wcplus_account_name = ['南京发布', '健康南京']
api_key = 'ac25c3928ffe580bcca204e3e0740468'
request_str_header = 'https://restapi.amap.com/v3/geocode/geo?address='
request_str_output = '&output=XML'
request_str_city = '&city=南京'
request_str_key = '&key='


def process_risk_data(xlsx_path):
    raw_df = pd.read_excel(xlsx_path)
    processed_df = pd.DataFrame(columns=['date', 'regions', 'address', 'location', 'risk level'])
    raw_len = len(raw_df)
    for i in range(0, raw_len):
        date = raw_df.iloc[i, 0]
        date = date.date()
        # 获取记录的时间
        raw_region = raw_df.iloc[i, 1]
        # 获取记录的大致区划（未必准确）
        raw_add_str = raw_df.iloc[i, 2]
        # 获取记录的字符串（多个地址在同一字符串中）
        risk_level = raw_df.iloc[i, 3]
        # 获取记录的风险登记变更
        add_str_list = process_combined_str(raw_add_str)
        list_num = len(add_str_list)
        for j in range(0, list_num):
            address = add_str_list[j]
            if address == 'flag 1':
                location = 'flag 1'
            elif address == 'flag 0':
                location = 'flag 0'
            else:
                request = '%s%s%s%s%s%s' % (request_str_header, address, request_str_output, request_str_city,
                                            request_str_key, api_key)
                res = requests.get(request)
                data = ET.XML(res.text.encode('utf-8').decode('utf-8'))
                tree = ET.ElementTree(data)
                root = tree.getroot()
                try:
                    location = root[4][0][12].text
                except IndexError:
                    location = None
            new_record = {'date': date, 'regions': raw_region, 'address': address, 'location': location,
                          'risk level': risk_level}
            processed_df = processed_df.append(new_record, ignore_index=True)
        print('fetching location... round: ' + str(i))
        time.sleep(3)
    processed_df.to_csv('../Data/wcplus processed/risk_level.csv')


def process_combined_str(combined_str):
    output_str = []
    if combined_str == '全街道':
        output_str.append('flag 1')
    elif combined_str == '全区':
        output_str.append('flag 0')
    else:
        combined_str = combined_str.replace('，', '；')
        combined_str = combined_str.replace('、', '；')
        combined_str = combined_str.replace('。', '')
        tmp_list = combined_str.split('；')
        tmp_list_cp = tmp_list.copy()
        for i in range(0, len(tmp_list)):
            if list(tmp_list[i])[0].isdigit():
                tmp_list_cp.remove(tmp_list[i])
                continue
            if len(tmp_list[i]) <= 3:
                tmp_list_cp.remove(tmp_list[i])
        output_str = tmp_list_cp
    return output_str


def incident_filter(save_path):
    read_path = '%s/%s' % (wcplus_data_path, '健康南京.csv')
    df = pd.read_csv(read_path, low_memory=False)
    tmp_df = df[df['标题'].str.contains('病例')]
    tmp_df.to_excel(save_path)


def incident_location_fetch(read_path, save_path):
    df = pd.read_excel(read_path)
    df_len = len(df)
    processed_df = pd.DataFrame(columns=['date', 'regions', 'address', 'location'])
    for i in range(0, df_len):
        date = df.iloc[i, 0]
        date = date.date()
        # 记录的时间
        add_str = df.iloc[i, 2]
        request = '%s%s%s%s%s%s' % (request_str_header, add_str, request_str_output, request_str_city,
                                    request_str_key, api_key)
        res = requests.get(request)
        data = ET.XML(res.text.encode('utf-8').decode('utf-8'))
        tree = ET.ElementTree(data)
        root = tree.getroot()
        try:
            location = root[4][0][12].text
            region = root[4][0][5].text
        except IndexError:
            location = None
            region = None
        new_record = {'date': date, 'regions': region, 'address': add_str, 'location': location}
        processed_df = processed_df.append(new_record, ignore_index=True)
        print('fetching location... row: ' + str(i))
        time.sleep(1)
    processed_df.to_csv(save_path)


def subString(path):
    df = pd.read_excel(path)
    current_date = df.iloc[-1].tolist()[0].date()
    print('current date is ' + str(current_date))
    date = input('Date: ')
    with open('../Data/wcplus processed/incident.txt', 'r', encoding='utf-8') as f:
        string = f.read()

    string = string.strip()
    tmp_list = re.findall(r'居住于(.*?)，', string)
    for tmp_i, tmp_s in enumerate(tmp_list):
        tmp_dict = {'date': date, 'address': tmp_s}
        df = df.append(tmp_dict, ignore_index=True)
    tmp_list1 = re.findall(r'暂住于(.*?)，', string)
    for tmp_i, tmp_s in enumerate(tmp_list1):
        tmp_dict = {'date': date, 'address': tmp_s}
        df = df.append(tmp_dict, ignore_index=True)
    tmp_list2 = re.findall(r'现住(.*?)，', string)
    for tmp_i, tmp_s in enumerate(tmp_list2):
        tmp_dict = {'date': date, 'address': tmp_s}
        df = df.append(tmp_dict, ignore_index=True)
    if len(tmp_list) + len(tmp_list1) + len(tmp_list2) != 0:
        print('Hit!')
    df.to_excel(path, index=False)


def process_incident_case_data():
    pass


if __name__ == '__main__':
    fabu_risk_csv_path = '%s/%s' % (wcplus_processed_data_path, 'risk_filtered.xlsx')
    # process_risk_data(fabu_risk_csv_path)
    jiankang_incident_csv_path = '%s/%s' % (wcplus_processed_data_path, 'jiankang_incident_filtered.xlsx')
    incident_read_path = '%s/%s' % (wcplus_processed_data_path, 'incident1.xlsx')
    incident_save_path = '%s/%s' % (wcplus_processed_data_path, 'incident.csv')
    incident_location_fetch(incident_read_path, incident_save_path)
