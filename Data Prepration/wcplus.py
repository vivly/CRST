import utils


def check_wcplus_data(wcplus_data_path):
    name_list, path_list = utils.get_path_list(wcplus_data_path)
    incident_flag, risk_flag = 0, 0
    if 'incident.csv' in name_list:
        incident_flag = 1
    if 'risk_level.csv' in name_list:
        risk_flag = 1
    if incident_flag + risk_flag == 2:
        print('wcplus files check completed!')
    else:
        raise IOError('The wcplus files seem to be missing, please check /Data/wcplus processed')
