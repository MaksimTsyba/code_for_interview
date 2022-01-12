from db.db import connect_db_data
from set_logging import Logging
from bucket import Bucket
from dotenv import load_dotenv
import pandas
import psycopg2
import psycopg2.extras  # type: ignore
import sys
import os
load_dotenv()


class LoadData(Bucket, Logging):
    """ Load markups data to DB """

    def __init__(self, markup_type_for_path, account_id, markup_path='preprocessed_markup.csv',
                 rules_path='preprocessed_rules.csv'):
        Bucket.__init__(self, os.getenv('AWS_BUCKET_NAME'))
        Logging.__init__(self, '', f"load_markups_{markup_type_for_path}.log",
                              f"{__name__}_{markup_type_for_path}")
        self.db_connect_data = connect_db_data(cursor_factory=psycopg2.extras.DictCursor)
        self.path_to_rules = rules_path
        self.path_to_markups = markup_path
        self.list_segments = None
        self.account_id = account_id
        self.type = markup_type_for_path
        self.models = list()
        self.archive_folder_name = 'archive_models'
        self.path_bucket_folder = f"models/{account_id}/{markup_type_for_path}"
        self.folder_list, self.timestamp = self.load_directories(self.path_bucket_folder)

    def load_data(self):
        """ Load data to DB"""
        print(f"Start load markups and segments for type {self.type}")
        markups = self.get_file(f"{self.path_bucket_folder}/{self.timestamp}/{self.path_to_markups}")
        rules = self.get_file(f"{self.path_bucket_folder}/{self.timestamp}/{self.path_to_rules}")
        if markups and rules:
            self.load_segments(rules)
            self.load_markups(markups)
            self.activation()
            self.db_connect_data.cursor.close()
            return True
        return False

    def insert_to_db(self, table: str, fields: tuple, data: tuple, returning_fields: list):
        """ Insert data to DB """
        query = f""" INSERT INTO {table} ({','.join(fields)}) VALUES %s RETURNING {','.join(returning_fields)} """
        result = psycopg2.extras.execute_values(
            self.db_connect_data.cursor, query, data, fetch=True
        )
        return result[0]

    def get_from_db(self, returning_fields: list, table: str, conditions: dict):
        """ Get from DB """
        query = f""" SELECT {','.join(returning_fields)} FROM {table} """
        if conditions:
            counter = 0
            for name, value in conditions.items():
                query += "WHERE " if counter == 0 else "AND "
                query += f""" {name} = '{value}'"""
                counter += 1
        self.db_connect_data.cursor.execute(query)
        return self.db_connect_data.cursor.fetchone()

    def insert_or_get(self, returning_fields, table, conditions):
        """ Get item from db if exists or insert """
        try:
            get_item = self.get_from_db(returning_fields, table, conditions)
            if not get_item:
                return self.insert_to_db(table, tuple(conditions.keys()), (tuple(conditions.values()),),
                                         returning_fields)
            else:
                return get_item
        except Exception as e:
            print(e)
            self.logger.error(f"Error to get or insert: table {table}")

    def add_or_update_segment(self, data):
        """ Add data for table segments or update it if exists """
        try:
            segment_item = f""" INSERT INTO data.segments 
            (account_model_id, segment_number, predicted_value, description) 
            VALUES {','.join(['%s'] * len(data))}
            ON CONFLICT (segment_number, account_model_id) DO 
            UPDATE SET (description, predicted_value) = (EXCLUDED.description, EXCLUDED.predicted_value)  """
            self.db_connect_data.cursor.execute(segment_item, data)
            return True
        except Exception as e:
            print(e)
            self.logger.error(f"Error add or update segment")
            return False

    def get_list_segments(self, timestamp):
        """ Get list segments """
        try:
            segment_query = f""" SELECT segments.id, models.name, segments.segment_number FROM data.segments segments 
            JOIN data.account_models account_models ON segments.account_model_id = account_models.id
            JOIN data.models models ON account_models.model_id = models.id
            WHERE account_models.model_version = '{timestamp}'"""
            self.db_connect_data.cursor.execute(segment_query)
            segment_item = self.db_connect_data.cursor.fetchall()
            if not segment_item:
                self.logger.error(f"List segment not found: model_version - {timestamp}")
            return segment_item
        except Exception as e:
            print(e)
            self.logger.error('Error getting segments list')

    def find_segment(self, segment, model):
        """ Find segment from list """
        for segment_list_item in self.list_segments:
            if segment_list_item.get('segment_number') == segment and segment_list_item.get('name') == model:
                return segment_list_item.get('id')

    def insert_markup(self, data):
        """ Insert markup to DB """
        try:
            add_markup_query = f""" INSERT INTO data.markups (segment_id, customer_profile_id, account_id) 
            VALUES {','.join(['%s'] * len(data))} """
            self.db_connect_data.cursor.execute(add_markup_query, data)
            self.db_connect_data.connection.commit()
        except Exception as e:
            print(e)
            self.logger.error('Error getting segments list')

    def load_segments(self, rules):
        """ Load segments to DB """
        self.logger.info('Start adding segments')
        print('Start adding segments')
        success_point = 0
        error_point = 0
        model_temp = None
        account_models = {}
        segments_data = []

        for index, item in pandas.read_csv(rules, delimiter=",").iterrows():
            if not model_temp or model_temp[1] != item.get('model'):
                model_item = self.insert_or_get(['id'], 'data.models', {"name": item.get('model')})
                if model_item:
                    if model_item.get('id') not in self.models and model_item.get('id') is not None:
                        self.models.append(str(model_item.get('id')))
                    model_temp = model_item.get('id'), item.get('model')
                else:
                    error_point += 1
                    continue
            if str(model_temp[0]) in account_models:
                account_models_id = account_models[str(model_temp[0])]
            else:
                account_model_data = {'account_id': str(self.account_id), 'model_id': str(model_temp[0]),
                                      'model_version': str(self.timestamp)}
                account_models_id = self.insert_or_get(['id'], 'data.account_models', account_model_data)
                if account_models_id:
                    account_models[str(model_temp[0])] = account_models_id.get('id')
                    account_models_id = account_models_id.get('id')
                else:
                    error_point += 1
                    continue
            segments_data.append((account_models_id, item.get('segment'),
                                  item.get('predicted_value'), item.get('description')))
            success_point += 1
        segment_item = self.add_or_update_segment(segments_data)
        if segment_item:
            self.db_connect_data.connection.commit()
        self.logger.info(f"Status adding segments: success - {success_point}, error - {error_point} ")

    def load_markups(self, markups):
        """ Load markups to DB """
        self.logger.info('Start adding markups')
        print('Start adding markups')
        success_point = 0
        error_point = 0
        counter = 0
        data = list()
        for markup_index, markup_item in pandas.read_csv(markups, delimiter=",").iterrows():
            if not self.list_segments:
                list_segment = self.get_list_segments(self.timestamp)
                if list_segment:
                    self.list_segments = list_segment
                else:
                    error_point += 1
                    continue

            segment_id = self.find_segment(markup_item.get('segment'), markup_item.get('model'))
            if not segment_id:
                error_point += 1
                continue

            markup_profile_id = markup_item.get('customer_profile_id') or markup_item.get('eshop_customer_id')

            if markup_profile_id:
                counter += 1
                data.append((str(segment_id), str(markup_profile_id), str(self.account_id)))
            else:
                error_point += 1
                continue

            if counter == 5000:
                self.insert_markup(data)
                data = list()
                counter = 0
            success_point += 1
        else:
            self.insert_markup(data)
        self.logger.info(f"Status adding markups: success - {success_point}, error - {error_point} ")

    def activation(self):
        account_models_for_activation, account_models_for_delete, timestamp_folders_for_delete = self.get_account_models()
        self.archiving_markups(account_models_for_delete, timestamp_folders_for_delete)
        self.remove_old_account_models(account_models_for_delete)
        self.activate_account_models(account_models_for_activation)

    def archiving_markups(self, account_models_for_delete, timestamp_folders_for_delete):
        """ Archiving old markups """
        if account_models_for_delete:
            query = f""" SELECT DISTINCT (model_version) FROM data.account_models WHERE id NOT IN ({','.join(account_models_for_delete)}) AND model_version IN ({','.join(map(lambda x: f"'{x}'", timestamp_folders_for_delete))})"""
            self.db_connect_data.cursor.execute(query)
            result = self.db_connect_data.cursor.fetchall()
            for item in timestamp_folders_for_delete:
                if [item] not in result:
                    folder_objects = self.get_list_objects_folder(f"{self.path_bucket_folder}/{item}/")
                    if folder_objects:
                        for folder_item in folder_objects:
                            new_path = folder_item.split('/')
                            new_path[0] = self.archive_folder_name
                            self.moving_file(folder_item, '/'.join(new_path))
                        self.delete_file(f"{self.path_bucket_folder}/{item}/")

    def get_account_models(self):
        """ Getting account models and generate lists with account models that need to delete and account
        models that need to activate  """
        result = list()
        if self.models:
            query = f""" SELECT * FROM  data.account_models WHERE account_id = '{self.account_id}' AND model_id in ({','.join(self.models)}) ORDER BY model_version DESC """
            self.db_connect_data.cursor.execute(query)
            result = self.db_connect_data.cursor.fetchall()
        unique_models = list()
        model_counter = dict()
        account_models_for_activation = list()
        account_models_for_delete = list()
        timestamp_folders_for_delete = list()
        for item in result:
            model_counter[item.get('model_id')] = 1 if item.get('model_id') not in model_counter else model_counter[item.get('model_id')] + 1

            if model_counter[item.get('model_id')] <= 2:
                if item.get('model_id') not in unique_models:
                    unique_models.append(item.get('model_id'))
                    account_models_for_activation.append((self.account_id, item.get('model_id'), item.get('id')))
            else:
                if item.get('id'):
                    account_models_for_delete.append(str(item.get('id')))
                if item.get('model_version') not in timestamp_folders_for_delete:
                    timestamp_folders_for_delete.append(item.get('model_version'))
        return account_models_for_activation, account_models_for_delete, timestamp_folders_for_delete

    def remove_old_account_models(self, account_models_for_delete):
        """ Remove account model and all related data """
        if account_models_for_delete:
            query_delete_old_account = f""" DELETE FROM data.account_models WHERE id IN %s  """
            self.db_connect_data.cursor.execute(query_delete_old_account, [tuple(account_models_for_delete)])
            self.db_connect_data.connection.commit()

    def activate_account_models(self, account_models_for_activation):
        """ Activate new models and deactivate previous models"""
        if account_models_for_activation:
            query_delete_old_active_account_models = f""" DELETE FROM data.active_account_models WHERE account_id = '{self.account_id}' AND model_id IN ({','.join(self.models)})"""
            self.db_connect_data.cursor.execute(query_delete_old_active_account_models)
            query_activate_account_models = f""" INSERT INTO data.active_account_models (account_id, model_id, account_model_id) VALUES  {','.join(['%s'] * len(account_models_for_activation))}"""
            self.db_connect_data.cursor.execute(query_activate_account_models, account_models_for_activation)
            self.db_connect_data.connection.commit()
