import os
import time

from psycopg2 import extras, pool
import psycopg2
import requests
import json

import pymysql

import csv

from Mailgun import send_with_attach



# Configuration
ELASTICSEARCH_URL = "https://elasticsearch.proxy.infra.vitel.net"  # Change this if your ES runs elsewhere
SQL_ENDPOINT = f"{ELASTICSEARCH_URL}/_sql"
HEADERS = {"Content-Type": "application/json"}
SQL_DATE_FIELD = "createDt"

index = 'data02-vitel-cdrs-2025-05'
START_DATE = os.environ.get("START_DATE")
END_DATE = os.environ.get("END_DATE")
fax_limiter = "in ('INBOUND_FAX_USAGE','OUTBOUND_FAX_USAGE')"
tf_limiter = "in ('OUTBOUND_TF_USAGE', 'INBOUND_TF_USAGE')"
standard_limiter = "in ('INBOUND_USAGE', 'OUTBOUND_USAGE')"

customer = os.environ.get('customer')

if os.environ.get("split_std_usage") == "true":
    split_std_usage = True
else:
    split_std_usage = False

if os.environ.get('subaccount'):
    relevant_subaccount = os.environ.get('subaccount')
else:
    relevant_subaccount = None
# MySQL connection configuration
MYSQL_CONFIG = {
    'user': 'readonly',
    'password': 'H1ssy.f1t!',
    'host': 'ha.proxy.infra.vitel.net',
    'database': 'vitel'
}

postgres_host = "10.44.8.11"
postgres_user = "vitel_user"
postgres_password = "rG2ZcAdazsQBzHbB"

output_filename = os.path.join('/tmp', 'aggregate_report_' + os.environ.get('customer') + '.csv')

def write_aggregate_report_csv(start_date, end_date, mrc_data_dict, non_fax_es_data_dict, es_fax_data_dict, filename=output_filename):
    all_subaccounts = set(mrc_data_dict.keys()) | set(non_fax_es_data_dict.keys()) | set(es_fax_data_dict.keys())
    columns = ['accountcode', 'subaccount', 'dollarAmount', 'duration', 'productCode']

    sorted_fax_data_dict = dict(sorted(es_fax_data_dict.items()))
    sorted_non_fax_data_dict = dict(sorted(non_fax_es_data_dict.items()))
    sorted_subaccounts = sorted(all_subaccounts)
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=columns)
        title = f"Subaccount Breakdown Report for {start_date} to {end_date}: {os.environ.get('customer')}" if start_date and end_date else ""
        writer.writerow({columns[0]: title})
        writer.writeheader()

        for subaccount in sorted_subaccounts:
            # Write from es_fax_data_dict if present
            if subaccount in es_fax_data_dict:
                data = es_fax_data_dict[subaccount]
                for row in data:
                    row_to_write = {
                        'accountcode': row['accountcode'],
                        'subaccount': subaccount,
                        'dollarAmount': "{:.2f}".format(float(row.get('dollarAmount', 0))) if row.get('dollarAmount') not in (None, '') else '',
                        'duration': str(int(round(float(row.get('duration', 0))))) if row.get('duration') not in (None, '') else '',
                        'productCode': row['productCode']
                    }
                    writer.writerow(row_to_write)
            # Write from non_fax_es_data_dict if present
            if subaccount in non_fax_es_data_dict:
                data = non_fax_es_data_dict[subaccount]
                for row in data:
                    row_to_write = {
                    'accountcode': row['accountcode'],
                    'subaccount': subaccount,
                    'dollarAmount': "{:.2f}".format(float(row.get('dollarAmount', 0))) if row.get('dollarAmount') not in (None, '') else '',
                    'duration': str(int(round(float(row.get('duration', 0))))) if row.get('duration') not in (None, '') else '',
                    'productCode': row['productCode']
                    }
                    writer.writerow(row_to_write)
            # Write from mrc_data_dict if present
            if subaccount in mrc_data_dict:
                data = mrc_data_dict[subaccount]
                row = {
                    'accountcode': data.get('accountcode', ''),
                    'subaccount': subaccount,
                    'dollarAmount': "{:.2f}".format(float(data.get('dollarAmount', 0))) if data.get('dollarAmount') not in (None, '') else '',
                    'duration': str(int(round(float(data.get('duration', 0))))) if data.get('duration') not in (None, '') else '',
                    'productCode': data.get('productCode', '')
                }
                writer.writerow(row)

def query_efaxauth(account, subaccount=None):
    subaccount = relevant_subaccount if subaccount is None else subaccount

    if subaccount:
        subaccount_addendum = " and login = '" + subaccount + "'"
    else:
        subaccount_addendum = ""
    efax_query = "SELECT owns as accountcode, login as subaccount, numbers FROM efaxauth WHERE owns = '" + account + "'" + subaccount_addendum
    try:
        conn = pymysql.connect(
            host=MYSQL_CONFIG['host'],
            user=MYSQL_CONFIG['user'],
            password=MYSQL_CONFIG['password'],
            database=MYSQL_CONFIG['database'],
            cursorclass=pymysql.cursors.DictCursor
        )
        with conn.cursor() as cursor:
            cursor.execute(efax_query)
            results = cursor.fetchall()  # List of dicts: [{'owns': ..., 'login': ..., 'numbers': ...}, ...]
        conn.close()
        return results if results else []
    except pymysql.MySQLError as err:
        print(f"MySQL error: {err}")
        return []

def postgres_read_query(host, user, password, account, START_DATE, END_DATE, subaccount=None):
    postgreSQL_read_pool = psycopg2.pool.SimpleConnectionPool(1, 20,
                                                              host=host, user=user, password=password,
                                                              database="vitel_db")
    subaccount = relevant_subaccount if subaccount is None else subaccount

    if subaccount:
        subaccount_addendum = " and invoice_subaccount = '{" + subaccount + "}'"
    else:
        subaccount_addendum = ""
    query_postgres = (
        "select invoice_subaccount as subaccount, sum(amount)*-1 as dollarAmount from billing.current_balance_logs " +
        "where login = %s and invoice_subaccount is not null " + subaccount_addendum +
        "and current_balance_logs.created_at between %s and %s group by invoice_subaccount"
    )
    try:
        ps_connection = postgreSQL_read_pool.getconn()
        if ps_connection:
            ps_cursor = ps_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            ps_cursor.execute(query_postgres, (account, START_DATE, END_DATE))
            result = ps_cursor.fetchall()
            ps_cursor.close()
            postgreSQL_read_pool.putconn(ps_connection)
            return result  # List of dicts, each dict indexed by field name
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while connecting to PostgreSQL", error)
        return []


def query_elasticsearch_with_sql(sql_query: str):
    """
    Queries Elasticsearch using SQL and returns the result.
    """
    payload = {"query": sql_query}

    try:
        response = requests.post(SQL_ENDPOINT, headers=HEADERS, data=json.dumps(payload))
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error querying Elasticsearch: {e}")
        return None

def build_src_dst_query_strings(efax_element):
    """
    For each element in efax_data, takes the 'numbers' attribute (comma-separated string),
    and returns a list of query strings of the form:
    'src = <<number>> or dst = <<number>>'
    """
    query_strings = []
    numbers = efax_element.get('numbers', '')
    for number in [n.strip() for n in numbers.split(',') if n.strip()]:
        query_strings.append(f"src = '{number}' or dst = '{number}'")
    query_strings_str = " or ".join(query_strings)
    return query_strings_str
# Replace with your actual index names

#distinct_sql = f'SELECT DISTINCT productCode FROM "{indices[0]}" where createDt between \'2025-04-26T00:00:00.000\' and \'2025-04-29T23:59:59.999\''
#distinct_result = query_elasticsearch_with_sql(distinct_sql)

start_time = time.time()
readable_start_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))
print(f"Process started for customer: {customer} at [{readable_start_time}]")

print("Querying for MRCs")
mrc_data = postgres_read_query(postgres_host, postgres_user, postgres_password, customer, START_DATE, END_DATE, relevant_subaccount)
mrc_data_dict = {}
for mrc_element in mrc_data:
    subaccount = mrc_element['subaccount'][0]
    dollar_amount = mrc_element['dollaramount']
    mrc_data_dict[subaccount] = {
        'accountcode': customer,
        'subaccount': subaccount,
        'dollarAmount': dollar_amount,
        'duration': 0,
        'productCode': 'MRC'# Assuming duration is not applicable for MRCs
    }

print("Querying for efaxauth")
efaxauth_data = query_efaxauth(customer)

print("Querying ES for non-fax data")
if split_std_usage == True:
    standard_grouping = ", productCode"
else:
    standard_grouping = ""
sql_base_standard = (f'SELECT subaccount, sum(dollarAmount), sum(duration) {standard_grouping} FROM "{index}" where createDt between \'{START_DATE}T00:00:00.000\' and \'{END_DATE}T23:59:59.999\' '
            f'and accountcode = \'{customer}\' and productCode not {fax_limiter} group by subaccount'+ standard_grouping)

non_fax_es_data = query_elasticsearch_with_sql(sql_base_standard)
#Put the non_fax_data into a dict with subaccount as key
non_fax_es_data_dict = {}

for row in non_fax_es_data['rows']:
    subaccount = row[0]
    #non_fax_es_data_dict[subaccount] = []
    dollar_amount = row[1]
    duration = row[2]
    if standard_grouping != "" and len(row) > 3:
        #should get it in the same form as es_fax_data_dict
        productCode = row[3]
        dict_to_append = {
            'accountcode': customer,
            'subaccount': subaccount,
            'dollarAmount': dollar_amount,
            'duration': duration,
            'productCode': productCode  # Assuming a generic product code for non-fax usage
        }
    else:
        productCode = 'STANDARD_USAGE'
        dict_to_append = {
            'accountcode': customer,
            'subaccount': subaccount,
            'dollarAmount': dollar_amount,
            'duration': duration,
            'productCode': productCode  # Assuming a generic product code for non-fax usage
        }
    if subaccount not in non_fax_es_data_dict:
        non_fax_es_data_dict[subaccount]= [dict_to_append]
    else:
        non_fax_es_data_dict[subaccount].append(dict_to_append)

print("Querying ES for fax data")
es_fax_data_dict = {}
sql_base_fax = (f'SELECT accountcode, sum(dollarAmount), sum(duration), productCode FROM "{index}" where createDt between \'{START_DATE}T00:00:00.000\' and \'{END_DATE}T23:59:59.999\' '
            f'and accountcode = \'{customer}\' and productCode {fax_limiter} ')

#Loop through the efax_data and query ES for each subaccount
for element in efaxauth_data:
    num_string = build_src_dst_query_strings(element)
    subaccount = element['subaccount']
    es_fax_data_dict[subaccount] = []
    sql_fax_query = sql_base_fax + f" and ({num_string}) group by accountcode, productCode"
    es_fax_data_result = query_elasticsearch_with_sql(sql_fax_query)
    if es_fax_data_result['rows']:
        # If there are results, process them
        for row in es_fax_data_result['rows']:
            accountcode = row[0]
            dollar_amount = row[1]
            duration = row[2]
            product_code = row[3]
            #Getting overwritten
            es_fax_data_dict[subaccount].append({
                'accountcode': accountcode,
                'subaccount': subaccount,
                'dollarAmount': dollar_amount,
                'duration': duration,
                'productCode': product_code
            })
            #print("")
    #es_fax_data_dict[subaccount] = query_elasticsearch_with_sql(sql_fax_query)
    #print("")

print("MRC Data on [%s] subaccounts" % len(mrc_data_dict))
print("Non-fax ES Data on [%s] subaccounts" % len(non_fax_es_data_dict))
print("Fax ES Data on [%s] subaccounts" % len(es_fax_data_dict))

write_aggregate_report_csv(START_DATE, END_DATE, mrc_data_dict, non_fax_es_data_dict, es_fax_data_dict)

#api_url = os.environ.get('MAILGUN_API_URL')
#sender = os.environ.get('MAILGUN_SENDER')
#recipient = os.environ.get('MAILGUN_RECIPIENT')
#subject = f"Aggregate Report for {customer}"
#body = f"Attached is the aggregate report for {customer} from {START_DATE} to {END_DATE}."

#send_with_attach(api_url, sender, recipient, subject, body, output_filename)

end_time = time.time()
total_time = end_time - start_time
print("Total time: " + str(total_time))
exit()