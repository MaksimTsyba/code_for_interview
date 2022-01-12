from db.db import connect_db_data
from set_logging import Logging
from dotenv import load_dotenv
from bucket import Bucket
import psycopg2.extras
import pandas
import requests
import json
import tempfile
import sys
import os

load_dotenv()


class PreprocessML(Bucket, Logging):
    """ Get markup and preprocess it before transfer to DB """
    ESHOP_PLATFORM = {
        0: 'Shopify',
        1: 'Woocommerce',
        2: 'Prestashop',
        3: 'Magento',
        4: 'Opencart3'
    }

    def __init__(self, markup_type, account_id, markup_name='markup.csv',
                 rules_name='rules.csv'):
        Bucket.__init__(self, os.getenv('AWS_BUCKET_NAME'))
        self.temp_dir = tempfile.TemporaryDirectory()
        self.markup_file_name = markup_name
        self.rules_file_name = rules_name
        self.prefix_for_preprocessed_file = 'preprocessed'
        self.type = markup_type
        self.auth_api_token = os.getenv('AUTH_API_TOKEN')
        self.auth_api_url = os.getenv('AUTH_API_URL')
        self.account_id = account_id
        eshop_data = self._get_eshop_data(account_id)
        self.eshop_id = eshop_data.get('id') if eshop_data else 803
        self.eshop_data = eshop_data
        self.eshop_prefix = self.select_prefix(markup_type)
        self.segments = dict()
        self.path_bucket_folder = f"models/{account_id}/{markup_type}"
        self.folder_list, self.timestamp = self.load_directories(self.path_bucket_folder)

    def start_preprocessing(self):
        """ Start preprocessing """
        print(f"Start preprocess scripts for type {self.type}")
        markup = self.get_file(f"{self.path_bucket_folder}/{self.timestamp}/{self.markup_file_name}")
        rules = self.get_file(f"{self.path_bucket_folder}/{self.timestamp}/{self.rules_file_name}")
        if self.eshop_id and markup and rules:
            markup_status = self.generate_new_markup(markup)
            if markup_status:
                self.generate_new_rules(rules)
                self.temp_dir.cleanup()
                return True
            else:
                self.temp_dir.cleanup()
                return False
        else:
            print(
                f"Files for preprocessing not found or eshop_id not found. "
                f"Markup: {markup}, rules: {rules}, eshop_id: {self.eshop_id}")
            self.temp_dir.cleanup()
            return False

    def select_prefix(self, markup_type):
        """ Select prefix for account """
        eshop_platform = self.ESHOP_PLATFORM[self.eshop_data.get('shop_platform_id')] if self.eshop_data else None
        prefix = ''
        if markup_type == 'crm':
            if eshop_platform == 'Shopify':
                prefix = 'customer-gid://shopify/Customer/'
            elif eshop_platform == 'Opencart3':
                prefix = 'customer-'
        return prefix

    def generate_new_markup(self, markup):
        """ Generate new markup """
        print(f"Start generate new markup for type {self.type}")
        db_connect_data = connect_db_data(cursor_factory=psycopg2.extras.DictCursor)
        success_markup_counter = 0
        error_markup_counter = 0
        counter = 0
        markup_counter = 0
        data = list()
        row_data = list()
        search_name = ''
        chunk = 1000
        markup_items = {'customer_profile_id': [], 'model': [], 'segment': [], 'eshop_customer_id': []}
        error_logs = {'id': [], 'account_id': [], 'eshop_id': [], 'model': [], 'segment': []}

        for index, row in pandas.read_csv(markup, delimiter=",").iterrows():

            markup_counter += 1

            self.collect_segments(row.get('segment'), row.get('model'), row.get('predicted_value'))

            if self.type == 'crm':
                search_name = 'eshop_customer_id'
                data.append(f"""'{self.eshop_prefix}{row.get('eshop_customer_id')}'""")
            elif self.type == 'beh':
                chunk = 500
                search_name = 'guest_id'
                data.append(f"""'{row.get('guest_id')}'""")
            else:
                continue

            row_data.append(row)
            counter += 1

            if counter == chunk:
                counter = 0
                get_result = self._search_customer_profile_id(db_connect_data, data, row_data, search_name,
                                                              error_logs, markup_items)
                success_markup_counter += get_result['success']
                error_markup_counter += get_result['error']
                row_data = list()
                data = list()
        else:
            if self.type in ['crm', 'beh']:
                get_result = self._search_customer_profile_id(db_connect_data, data, row_data, search_name,
                                                              error_logs, markup_items)
                success_markup_counter += get_result['success']
                error_markup_counter += get_result['error']

        db_connect_data.cursor.close()
        print(f"Status adding markups: success - {success_markup_counter}, error - {error_markup_counter}")
        if success_markup_counter:
            if self.type != 'mixed':
                self.save_file_in_temp(markup_items, f"{self.prefix_for_preprocessed_file}_{self.markup_file_name}")
                self.save_file_in_temp(error_logs, f"{self.prefix_for_preprocessed_file}_markup_errors.csv")
                old_markup_path = f"{self.path_bucket_folder}/{self.timestamp}/{self.markup_file_name}"
                new_markup_path = f"{self.path_bucket_folder}/{self.timestamp}/preprocessed/{self.markup_file_name}"
                self.moving_file(old_markup_path, new_markup_path)
            print(f"Markups preprocessed - {success_markup_counter}")
            return True
        else:
            print("No matching customers with markups")
            return False

    def generate_new_rules(self, rules):
        """ Generate new rules file """
        success_rules_counter = 0
        error_rules_counter = 0
        print("Start preprocess rules")
        rules_items = {'predicted_value': [], 'description': [], 'model': [], 'segment': []}
        error_logs = {'model': [], 'segment': []}
        for rules_index, rules_item in pandas.read_csv(rules, delimiter=",").iterrows():
            predicted_value = None
            if rules_item.get('model') in self.segments:
                if rules_item.get('segment') in self.segments[rules_item.get('model')]:
                    predicted_value = self.segments[rules_item.get('model')][rules_item.get('segment')]
                    success_rules_counter += 1
                else:
                    error_rules_counter += 1
                    error_logs['model'].append(rules_item.get('model'))
                    error_logs['segment'].append(rules_item.get('segment'))
            else:
                error_rules_counter += 1
                print(f"Model {rules_item.get('model')} not found")
            rules_items['predicted_value'].append(predicted_value)
            rules_items['description'].append(rules_item.get('description'))
            rules_items['model'].append(rules_item.get('model'))
            rules_items['segment'].append(rules_item.get('segment'))

        self.save_file_in_temp(rules_items, f"{self.prefix_for_preprocessed_file}_{self.rules_file_name}")
        self.save_file_in_temp(error_logs, f"{self.prefix_for_preprocessed_file}_rules_errors.csv")
        old_rule_path = f"{self.path_bucket_folder}/{self.timestamp}/{self.rules_file_name}"
        new_rule_path = f"{self.path_bucket_folder}/{self.timestamp}/preprocessed/{self.rules_file_name}"
        self.moving_file(old_rule_path, new_rule_path)
        print(f"Status adding rules: success - {success_rules_counter}, error - {error_rules_counter}")
        if success_rules_counter:
            print(f"Rules preprocessed - {success_rules_counter}")
            return True
        else:
            print(f"Preprocess of rules is failed - {error_rules_counter}")
            return False

    def save_file_in_temp(self, data, file_name):
        path = f"{self.temp_dir.name}/{file_name}"
        df_markups = pandas.DataFrame(data)
        df_markups.to_csv(path)
        self.add_file(path, f"{self.path_bucket_folder}/{self.timestamp}/{file_name}")

    def collect_segments(self, segment_number, model, predicted_value):
        """ Collecting segments """
        if model not in self.segments:
            self.segments[model] = dict()
        self.segments[model][segment_number] = predicted_value
        return self.segments

    def _search_customer_profile_id(self, db_connect_data, data, data_row, search_name, error_logs, markup_items):
        """ Search customer profile id """
        success_markup_counter = 0
        error_markup_counter = 0
        customer_profile_items = None
        try:
            query = ''
            if self.type == 'crm':
                query = f"""SELECT customer_profile_id, eshop_customer_id FROM data.customer_profile_crm 
                WHERE eshop_customer_id IN ({','.join(data)}) AND eshop_id = '{self.eshop_id}'"""
            elif self.type == 'beh':
                query = f"""SELECT customer_profile_id, guest_id FROM data.customer_profile_behaviour 
                WHERE guest_id IN ({','.join(data)}) AND account_id = '{self.account_id}'"""
            db_connect_data.cursor.execute(query)
            customer_profile_items = db_connect_data.cursor.fetchall()
        except Exception as e:
            print(e)
            error_markup_counter += len(data_row)
        if customer_profile_items:
            deleted_items = dict()
            for row_item in data_row:
                check_row = False
                customer_id = f"{self.eshop_prefix}{row_item.get(search_name)}"
                for index, item in enumerate(customer_profile_items):
                    if item[1] == customer_id:
                        check_row = True
                        markup_items['customer_profile_id'].append(item[0])
                        markup_items['model'].append(row_item.get('model'))
                        markup_items['segment'].append(row_item.get('segment'))
                        markup_items['eshop_customer_id'].append(row_item.get(search_name))
                        deleted_items[customer_id] = item[0]
                        del customer_profile_items[index]
                        success_markup_counter += 1
                        break

                if not check_row:
                    if customer_id in deleted_items:
                        markup_items['customer_profile_id'].append(deleted_items[customer_id])
                        markup_items['model'].append(row_item.get('model'))
                        markup_items['segment'].append(row_item.get('segment'))
                        markup_items['eshop_customer_id'].append(row_item.get(search_name))
                        success_markup_counter += 1
                    else:
                        error_markup_counter += 1
                        error_logs['id'].append(f"{self.eshop_prefix}{row_item.get(search_name)}")
                        error_logs['account_id'].append(self.account_id)
                        error_logs['eshop_id'].append(self.eshop_id)
                        error_logs['model'].append(row_item.get('model'))
                        error_logs['segment'].append(row_item.get('segment'))
        else:
            error_markup_counter += 1
            print(f"Error during get customer profiles: {self.type}")
        return {'success': success_markup_counter, 'error': error_markup_counter}

    def _get_eshop_data(self, account_id):
        """ Retrieve eshop data by account_id """
        url = f"{self.auth_api_url}/v1/accounts/{account_id}/eshop-api-keys"
        header = {"Authorization": self.auth_api_token}
        get_eshop_account = requests.get(url=url, headers=header)
        if get_eshop_account.status_code == 200:
            account_item = json.loads(get_eshop_account.content)
            if account_item:
                return account_item[0]
            else:
                print(f"Account not found: {account_id}")
        else:
            print(
                f"Error during get account id: {account_id}, status_code: {get_eshop_account.status_code}")
