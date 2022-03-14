import pandas as pd
from sodapy import Socrata
import json

from config import DOMAIN, APP_TOKEN, USERNAME, PASSWORD, TIMEOUT

client = Socrata(
    domain=DOMAIN,
    app_token=APP_TOKEN,
    username=USERNAME,
    password=PASSWORD,
    timeout=TIMEOUT
)


def get_json_file():
    results = client.get_all("5mzw-sjtu")
    with open('dt.json', 'w') as f:
        for i in results:
            f.write(json.dumps(i) + '\n')


def get_dataset_from_json():
    chunks = pd.read_json('dt.json', lines=True, chunksize=100000)
    header = True
    for chunk in chunks:
        aggregated_data_sales = chunk.groupby(['listyear', 'town']).agg({'saleamount': sum})
        aggregated_data_ratio = chunk.groupby(['listyear', 'town']).agg({'salesratio': 'mean'})
        mode = 'w' if header else 'a'
        aggregated_data_sales.to_csv('csv_results/out_sales.csv', mode=mode, header=header)
        aggregated_data_ratio.to_csv('csv_results/out_ratio.csv', mode=mode, header=header)
        header = False


def aggregate_of_aggregated():
    data_sales = pd.read_csv('csv_results/out_sales.csv')
    data_ratio = pd.read_csv('csv_results/out_ratio.csv')
    result_sale_amount = data_sales.groupby(['listyear', 'town']).agg({'saleamount': sum})
    result_ratio = data_ratio.groupby(['listyear', 'town']).agg({'salesratio': 'mean'})
    result_sale_amount.sort_values(by=['listyear', 'saleamount'], inplace=True, ascending=False)
    result_ratio.sort_values(by=['listyear', 'salesratio'], inplace=True, ascending=False)
    result_sale_amount.to_csv('csv_results/result_sale_amount.csv', header=True)
    result_ratio.to_csv('csv_results/result_ratio.csv', header=True)


def get_largest():
    data_sales = pd.read_csv('csv_results/result_sale_amount.csv')
    data_ratio = pd.read_csv('csv_results/result_ratio.csv')
    result_sale_amount = data_sales.groupby('listyear').head(10).reset_index(drop=True)
    result_ratio = data_ratio.groupby('listyear').head(10).reset_index(drop=True)
    result_sale_amount.to_csv('csv_results/largest_sale_amount.csv', header=True, index=False)
    result_ratio.to_csv('csv_results/largest_ratio.csv', header=True, index=False)


if __name__ == '__main__':
    get_json_file()
    get_dataset_from_json()
    aggregate_of_aggregated()
    get_largest()
