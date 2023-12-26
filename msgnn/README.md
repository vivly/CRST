# MSGNN

Source code for the model MSGNN, the epidemic incident forecasting model for the US.

## Setup

- Install CUDA 10.1.2
- Run `setup_py.sh` to install necessary packages

## Usage

```shell
cd src/
### train and test model, more options can be referred in run_models.py
python run_models.py --forecast_date 2021-02-28

## Notification
- The CRST model is memory efficient, any NVIDIA GPU with 4GB or more memory is capable of running this model. 
- The recommended date range is from 2021-02-28 to 2023-01-01

