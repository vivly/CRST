import pandas as pd
import utils


def check_poi_files(mapping_dict, poi_file_path):
    category_check_list = []
    name_list, path_list = utils.get_path_list(poi_file_path)
    new_mapping_dict = dict(zip(mapping_dict.values(), mapping_dict.keys()))
    for name in name_list:
        category_chinese = (name.split('-')[1]).split('.')[0]
        category_number = new_mapping_dict[category_chinese]
        category_check_list.append(category_number)
    if len(category_check_list) != len(mapping_dict):
        raise IOError('The POI files seem to be missing, please check /Data/POIsearch')
    else:
        print('POI files check completed!')


def fetch_poi_dataframe():
    pass
