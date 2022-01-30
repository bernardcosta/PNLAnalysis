import requests
import json
import pickle
from elasticsearch import Elasticsearch
from datetime import datetime
import settings
import argparse
import pandas as pd
from dotenv import load_dotenv
import os
import logging
log = logging.getLogger(__name__)


def request_coins(assets_file, breakdown=False):
    log.info('Calculating assets...')
    with open(assets_file) as f:
        assets = json.load(f)

    total_usd=0
    breakdown_details = []
    for i, asset in enumerate(assets):

        coin = f'{asset["Coin"]}{asset["To"]}'

        url = f'{settings.API}?symbol={coin}'
        response = requests.get(url, headers=settings.HEADERS).json()
        # if final asset from file (which is EURO)
        if i == len(assets) - 1:
            log.info("Done")
            if breakdown:
                # In case there are multiple records for the same coin
                details = pd.DataFrame(breakdown_details).groupby("coin").sum().reset_index()

                for coin, val in asset["split_fraction"].items():
                    if val > 0:
                        print(coin)
                        details.loc[details.coin == coin, "contribution"] = val
                log.info(details)
                return total_usd / float(response["price"]), float(asset["Amount"]), details
            else:
                return total_usd / float(response["price"]), float(asset["Amount"])
        else:
            value = float(response["price"]) * asset["Amount"]
            total_usd += value

            if breakdown:
                breakdown_details.append({"coin":asset["Coin"], "value": value, "coinAmount": asset["Amount"], "contribution":0.0})


if __name__ == "__main__":
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format='[%(asctime)s-%(levelname)s] %(name)s: %(message)s')
    logging.getLogger("elasticsearch").setLevel(logging.WARNING)
    parser = argparse.ArgumentParser(description='Assets')
    parser.add_argument("--info_only", "-o", action='store_true', default=False, help="Only show assets details without any database I/O")
    parser.add_argument("--file", "-f", default="inputs/assets.json", help="json file to calculate current assets price")
    args = parser.parse_args()

    assets_net_worth, contribution, coins = request_coins(args.file, breakdown=True)
    log.info(f'Total: {assets_net_worth}\nContribution: {contribution}')

    if not args.info_only:
        es = Elasticsearch([{'host':os.getenv('HOST'), os.getenv('PORT'):9200}])
        response = es.search(index=os.getenv('MAIN_INDEX'), body=settings.QUERY_LAST_DATA)
        if not response['hits']['hits']:
            log.warning("Empty index...inputing first document.")
            last_balance = 0
            cumulative_amount = 0
        else:
            query_data = response['hits']['hits'][0]['_source']
            cumulative_amount = float(query_data['cumulative_amount'])
            last_balance = float(query_data['balance'])

        profit_change = assets_net_worth - last_balance
        log.info(f'profit change: {profit_change}')
        input = {
          "account": "Binance+Exodus",
          "amount":0.0,
          "fee": 0,
          "coin": "EUR"
        }
        if contribution > 0:
            input["amount"] = contribution
            input["account"] = "Binance"
        input["cumulative_amount"] = cumulative_amount + contribution
        date = str(datetime.now())+"+01:00"
        input["date"] = date.replace(" ", "T")
        input["interval_change"] = profit_change
        input["balance"] = assets_net_worth
        log.info(f'Cumulative amount: {input["cumulative_amount"]}')
        log.info("===========================")

        res = es.index(index=os.getenv('MAIN_INDEX'), body=input)

        final_date = input["date"]

        input2 = coins.to_dict(orient="records")
        log.info('Adding individual coin data to server')
        for record in input2:
            record['date'] = final_date
            # print(record)
            exchangeres = es.index(index=os.getenv('SECONDARY_INDEX'), body=record)
