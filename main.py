import requests
import json
import argparse
import pandas as pd
from dotenv import load_dotenv
import psycopg2
import os
import utils
import logging
from datetime import datetime
from psycopg2 import sql
log = logging.getLogger(__name__)


def request_coins(assets_file, breakdown=False):
    log.info('Calculating assets...')
    with open(assets_file) as f:
        assets = json.load(f)

    total_usd = 0
    breakdown_details = []
    for i, asset in enumerate(assets):

        coin = f'{asset["Coin"]}{asset["To"]}'

        url = f'{os.getenv("API")}?symbol={coin}'
        response = requests.get(
            url, headers=json.loads(os.getenv("HEADERS"))).json()
        # if final asset from file (which is EURO)
        if i == len(assets) - 1:
            log.info("Done")
            if breakdown:
                # In case there are multiple records for the same coin
                details = pd.DataFrame(breakdown_details).groupby(
                    "coin").sum().reset_index()

                for coin, val in asset["split_eur"].items():
                    if val > 0:
                        log.info(coin)
                        details.loc[details.coin == coin, "contribution"] = val
                log.info(details)
                return total_usd / float(response["price"]), float(asset["Amount"]), details
            else:
                return total_usd / float(response["price"]), float(asset["Amount"])
        else:
            value = float(response["price"]) * asset["Amount"]
            total_usd += value

            if breakdown:
                breakdown_details.append(
                    {"coin": asset["Coin"], "value": value, "coinAmount": asset["Amount"], "contribution": 0.0})


def setup_logging():
    logging.basicConfig(
        level=logging.INFO, format='[%(asctime)s-%(levelname)s] %(name)s: %(message)s')
    logging.getLogger("elasticsearch").setLevel(logging.WARNING)


def setup_cli_args():
    parser = argparse.ArgumentParser(description='Assets')
    parser.add_argument("--info_only", "-o", action='store_true', default=False,
                        help="Only show assets details without any database I/O")
    parser.add_argument("--file", "-f", default=os.path.abspath("inputs/assets.json"),
                        help="json file to calculate current assets price")
    arguments = parser.parse_args()
    return arguments


if __name__ == "__main__":
    load_dotenv()
    setup_logging()
    args = setup_cli_args()
    log.info("Input file {}".format(args.file))

    assets_net_worth, contribution, coins = request_coins(
        args.file, breakdown=True)
    log.info(f'Total: {assets_net_worth}')
    log.info(f'Contribution: {contribution}')

    db = utils.Postgres(dict_cursor=True)
    try:

        query = "SELECT * FROM {} ORDER BY date_created DESC LIMIT 1;"
        db.default_cursor.execute(sql.SQL(query).format(sql.Identifier(os.environ['DB_TABLE'])))
        last_ingest = db.default_cursor.fetchone()
        if not last_ingest:
            log.warning("Empty table... inputting first document.")
            last_balance = 0
            cumulative_amount = 0
        else:
            last_ingest = dict(last_ingest)
            cumulative_amount = float(last_ingest['cumulative_amount'])
            last_balance = float(last_ingest['total_balance'])

            profit_change = assets_net_worth - last_balance
            log.info(f'profit change: {profit_change}')

            amount = 0.0
            fee = 0.0
            if contribution > 0:
                amount = contribution
                fee = 0.02

            cumulative_amount = cumulative_amount + contribution
            date = str(datetime.now()) + "+01:00"
            date = date.replace(" ", "T")
            change = profit_change
            total_balance = assets_net_worth
            log.info(f'Cumulative amount: {cumulative_amount}')
            log.info("===========================")

            insert = """
            INSERT INTO {} (amount, fee, cumulative_amount, date_created, change, total_balance ) 
            VALUES (%s, %s, %s, %s, %s, %s)
            """

            db.default_cursor.execute(sql.SQL(insert).format(sql.Identifier(os.environ['DB_TABLE'])),
                                      (amount, fee, cumulative_amount, datetime.now(), change, total_balance,))
            db.connection.commit()

    except Exception as e:
        import sys
        ex_type, ex_object, ex_tb = sys.exc_info()
        log.error("{} (line: {}) ".format(repr(e), ex_tb.tb_lineno))
    finally:
        log.info("Closing DB connection")
        db.close_connection()
