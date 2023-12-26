import math

import pandas as pd
import numpy as np

if __name__ == '__main__':
    incident_ts_fp = '../Data/incident_ts.csv'
    risk_ts1_fp = '../Data/risk_ts_1.csv'
    risk_ts2_fp = '../Data/risk_ts_2.csv'
    risk_ts3_fp = '../Data/risk_ts_3.csv'

    incident_df = pd.read_csv(incident_ts_fp, low_memory=False, index_col='Unnamed: 0')
    risk_df1 = pd.read_csv(risk_ts1_fp, low_memory=False, index_col='Unnamed: 0')
    risk_df2 = pd.read_csv(risk_ts2_fp, low_memory=False, index_col='Unnamed: 0')
    risk_df3 = pd.read_csv(risk_ts3_fp, low_memory=False, index_col='Unnamed: 0')

    incident_df = risk_df3 + (np.random.rand(*incident_df.shape) - 0.85)
    incident_df.where(incident_df >= 0, 0, inplace=True)
    incident_df = incident_df.apply(np.ceil)
    incident_df = incident_df.astype(int)
    incident_df.to_csv('./risk3_predict.csv')
