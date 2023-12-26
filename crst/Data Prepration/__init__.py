import numpy as np
import pandas as pd
import POI
import utils
import wcplus
import json
import os
from utils import wgs_gcj, rebuild_np
import multiprocessing as mp
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon

POI_mapping_dict = {0: '风景名胜', 1: '公共交通', 2: '公司企业', 3: '金融服务', 4: '科教文化服务', 5: '汽车维修',
                    6: '商场市场', 7: '商务住宅', 8: '体育休闲服务', 9: '医疗', 10: '政府机构及社会团体', 11: '住宿服务',
                    12: '公共设施', 13: '餐饮服务', 14: '便民商店'}
POI_data_path = '../Data/POISearch/'
wcplus_data_path = '../Data/wcplus processed'
geo_data_path = '../Data/Geo'
x_width = 0.00001141
y_width = 0.00000899
# 1米换算经纬度度数
cell_length = 5000
cell_width = 5000
# 每个隔间的长宽（单位米）


def get_grid_map(city):
    print('---> Start griding city map of Nanjing...')
    file_path = '%s/%s_wgs84.geojson' % (geo_data_path, city)
    with open(file_path) as f:
        gj = json.load(f)
    wgs84_bbox = gj['features'][0]['geometry']['bbox']
    wgs84_x_left = wgs84_bbox[0]
    wgs84_y_bottom = wgs84_bbox[1]
    wgs84_x_right = wgs84_bbox[2]
    wgs84_y_top = wgs84_bbox[3]
    wgs84_left_bottom = [wgs84_x_left, wgs84_y_bottom]
    wgs84_left_top = [wgs84_x_left, wgs84_y_top]
    wgs84_right_bottom = [wgs84_x_right, wgs84_y_bottom]
    wgs84_right_top = [wgs84_x_right, wgs84_y_top]
    gcj02_left_bottom, gcj02_left_top, gcj02_right_bottom, gcj02_right_top = \
        wgs_box_2_gcj_box(wgs84_left_bottom, wgs84_left_top, wgs84_right_bottom, wgs84_right_top)
    count_x = (gcj02_right_bottom[0] - gcj02_left_bottom[0]) / (x_width * cell_length)
    count_y = (gcj02_left_top[1] - gcj02_left_bottom[1]) / (y_width * cell_width)
    count_x = int(round(count_x, 0))
    count_y = int(round(count_y, 0))
    print('%s:%s*%s, %s%s' % ('Current grid setting', str(cell_width), str(cell_length), 'total cell count is: ',
                              str(count_y * count_x)))
    index = 1
    grid_index_to_location_dict = {}
    for i in range(0, count_x):
        # 固定x轴坐标，切割
        start_x = round(gcj02_left_bottom[0] + i * x_width * cell_length, 10)
        end_x = round(start_x + x_width * cell_length, 10)
        for j in range(0, count_y):
            # 变动y轴坐标，切割
            start_y = round(gcj02_left_bottom[1] + j * y_width * cell_width, 10)
            end_y = round(start_y + y_width * cell_width, 10)
            grid_location_list = [start_x, end_x, start_y, end_y]
            grid_index_to_location_dict.update({index: grid_location_list})
            index += 1
    print('---> Grid finished!')
    return grid_index_to_location_dict


def wgs_box_2_gcj_box(left_bottom, left_top, right_bottom, right_top):
    gcj02_x_left, gcj02_y_bottom = wgs_gcj(left_bottom[0], left_bottom[1])
    gcj02_x_right, gcj02_y_top = wgs_gcj(right_top[0], right_top[1])
    gcj02_left_bottom = [gcj02_x_left, gcj02_y_bottom]
    gcj02_left_top = [gcj02_x_left, gcj02_y_top]
    gcj02_right_bottom = [gcj02_x_right, gcj02_y_bottom]
    gcj02_right_top = [gcj02_x_right, gcj02_y_top]
    return gcj02_left_bottom, gcj02_left_top, gcj02_right_bottom, gcj02_right_top


def clean_grid(cell_dict):
    grid1000_file_path = '%s/%s' % (geo_data_path, 'grid-1000.csv')
    grid500_file_path = '%s/%s' % (geo_data_path, 'grid-500.csv')
    grid_file_path_list = [grid500_file_path, grid1000_file_path]
    if os.path.exists(grid1000_file_path):
        print('---> The grid map is already cleaned')
        return grid_file_path_list
    print('---> Start Cleaning the grid map...')
    Thread_num = 8
    df = pd.DataFrame.from_dict(cell_dict, orient='index').rename(columns={0: 'start_x', 1: 'end_x', 2: 'start_y',
                                                                           3: 'end_y'})
    poi_df, CRST, COMET = read_all_poi_files(POI_data_path)
    poi_location_df = poi_df.reset_index()[['name', 'location']]
    print('Multi-processing enabled, thread number: ' + str(Thread_num))
    cut_df = utils.cut_df(df, Thread_num)

    # 多核心处理清洗，核心数为32
    mp.Pool(Thread_num)
    manager = mp.Manager()
    q = manager.Queue()
    # 使用 Manager 避免用queue存放数据时死锁
    jobs = []
    for i in range(0, len(cut_df)):
        p = mp.Process(target=contain_w_multi_thread, args=[cut_df[i], poi_location_df, i, Thread_num, q])
        jobs.append(p)
        p.start()
    for p in jobs:
        p.join()
    # p = mp.Process(target=contain_w_multi_thread, args=[cut_df[1], poi_location_df, 1, Thread_num, q])
    # jobs.append(p)
    # p.start()
    # p.join()
    result = [q.get() for j in jobs]
    processed_df = pd.concat(result)
    processed_np = processed_df.values
    if cell_length == 500 or cell_width == 500:
        processed_df.to_csv(grid500_file_path)
    elif cell_length == 1000 or cell_length == 1000:
        processed_df.to_csv(grid1000_file_path)
    return grid_file_path_list, COMET, CRST


def read_residence_poi_file(path):
    residence_poi_file_path = '%s/%s' % (path, 'Excel-商务住宅.xlsx')
    df = pd.read_excel(residence_poi_file_path)
    return df


def read_all_poi_files(path):
    _, file_path = utils.get_path_list(path)
    dfs = []
    tmps = []
    tmps1 = []
    for idx, paths in enumerate(file_path):
        print('Reading all POI files..., count: ' + str(idx))
        df = pd.read_excel(paths)
        crst_list = df['gcj02_lng'].tolist()
        high_crst = max(crst_list)
        no_crst = min(crst_list)
        low_crst = min(crst_list) + len(crst_list)
        med_crst = max(crst_list) - len(crst_list)
        avg_crst = sum(crst_list) / len(crst_list)
        tmps.append([high_crst, no_crst, low_crst, med_crst])
        tmps1.append(avg_crst)
        dfs.append(df)
    CRST = np.array(tmps)
    COMET = np.array(tmps1[0:4])
    alldata = pd.concat(dfs)
    return alldata, CRST, COMET


def contain_w_multi_thread(cell_df, poi_df, thread, thread_num, q):
    new_df = pd.DataFrame(columns=('start_x', 'end_x', 'start_y', 'end_y'))
    df_len = len(cell_df)
    for i in range(0, df_len):
        # 读取所有的网格
        if thread == 1:
            # 进度指示器
            if i % 50 == 0:
                print('Current cell count is ' + str(i * thread_num) + '/' + str(df_len * thread_num))
        lst = [cell_df.iloc[i, 0], cell_df.iloc[i, 1], cell_df.iloc[i, 2], cell_df.iloc[i, 3]]
        # 获取网格坐标点并形成多边形
        poi_hit_count = 0
        for j in range(0, len(poi_df)):
            # 对于每一个网格，通过遍历POI的Dataframe来查询其中包含多少POI
            tmp = poi_df.iloc[j, 1]
            tmp_list = tmp.split(',')
            tmp_list = [float(x) for x in tmp_list]
            point = Point(tmp_list[0], tmp_list[1])
            # 形成POI的位置点
            if (point.x >= lst[0]) and (point.y >= lst[2]) and (point.x <= lst[1]) and (point.y <= lst[3]):
                poi_hit_count += 1
                # 判断多边形中是否含有位置点，如有POI Hit数+1
        # if poi_hit_count >= 5:
        #     series = pd.Series({'start_x': lst[0], 'end_x': lst[1], 'start_y': lst[2], 'end_y': lst[3]})
        #     new_df = new_df.append(series, ignore_index=True)
    print('Thread ' + str(thread) + ' complete!')
    q.put(new_df)


def print_squad_number(COMET, CRST):
    print('='*20)
    print('Printing Results')
    print('The shape for CRST model is: ')
    print(CRST.shape)
    print('The shape for COMET model is: ')
    print(COMET.shape)


if __name__ == '__main__':
    POI.check_poi_files(POI_mapping_dict, POI_data_path)
    wcplus.check_wcplus_data(wcplus_data_path)
    grid_dict = get_grid_map(city='Nanjing')
    _, COMET, CRST = clean_grid(grid_dict)
    print_squad_number(COMET, CRST)

