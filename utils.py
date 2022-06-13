from datetime import datetime
import os
import logging
import json
import psycopg2
log = logging.getLogger(__name__)
import psycopg2.extras


class Postgres:

    def __init__(self, dict_cursor=False):

        self.connection = None
        self.dict_cursor = dict_cursor
        self.connect_to_db()
        self.default_cursor = self.create_cursor()

    def connect_to_db(self):
        self.connection = psycopg2.connect(host=os.environ['DB_HOST'], user=os.environ['DB_USER'],
                                           dbname=os.environ['DB_NAME'], port=os.environ['DB_PORT'])
        log.info('Connected to database {}'.format(os.environ['DB_NAME']))

    def create_cursor(self):
        if self.dict_cursor:
            return self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        else:
            return self.connection.cursor()

    def close_connection(self):
        if self.default_cursor:
            self.default_cursor.close()
        if self.connection:
            self.connection.close()
