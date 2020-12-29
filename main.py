
import json
import base64
import os
import requests
from google.cloud import bigquery
from dateutil.parser import parse as parse_date
import math

function_name = 'bill_transfer_report'
console_link = 'https://console.cloud.google.com/functions/details/europe-west1/{}?authuser=1&project=kudi-209818' \
    .format(function_name)
bq_client = bigquery.Client()
dataset_id = 'billing'
table_id = 'bill_transfer_reports'
table_ref = bq_client.dataset(dataset_id).table(table_id)
table = bq_client.get_table(table_ref)
slackWebHookUrl = os.getenv('SLACK_WEBHOOK_URL_CLOUD_FUNCTIONS')
slack_webhook_headers = {
    'Content-Type': 'application/json'
}
def post_to_slack(message):
    data = {
        'blocks': [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"<{console_link} | Function Name - {function_name}>"
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "plain_text",
                    "text": message
                }
            }
        ]
    }
    try:
        r = requests.post(slackWebHookUrl, json.dumps(data), headers=slack_webhook_headers)
        print('StatusCode of call to slack is {}'.format(r.status_code))
    except Exception as e:
        print(str(e))
def clean_amount(s):
    return s.replace(',', '').replace('.00', '').replace(' ', '')




def bill_transfer_report(event, context):
    if 'data' in event:
        try:
            payload = json.loads(base64.b64decode(event['data']).decode('utf-8'))
            print(payload)
            final_payload = []
            
            
            row = dict()
            row['amount'] = payload.get('amount')
            row['balance'] = payload.get('balance')
            row['created_by'] = payload.get('created_by')
            row['entry_type'] = payload.get('entry_type')
            row['kudi_vendor_reference'] = payload.get('kudi_vendor_reference')
            row['kudi_transaction_ref'] = payload.get('kudi_transaction_ref')
            row['agent_id'] = payload.get('agent_id')
            row['nibbs_session_id'] = payload.get('nibbs_session_id')
            row['raw_narration'] = payload.get('raw_narration')
            row['record_type'] = payload.get('record_type')
            row['reference'] = payload.get('reference')
            transaction_date = payload.get('transaction_date')
            if transaction_date:
                parsed_transaction_date = parse_date(transaction_date)
                row['transaction_date'] = str(parsed_transaction_date)

            row['upload_id'] = payload.get('upload_id')
            value_date = payload.get('value_date')
            if value_date:
                parsed_value_date = parse_date(value_date)
                row['value_date'] = str(parsed_value_date)
            row['vendor'] = payload.get('vendor')
            row['vendor_identifier'] = payload.get('vendor_identifier')
            row['vendor_transaction_reference'] = payload.get('vendor_transaction_reference')
            final_payload.append(row)
            
            errors = bq_client.insert_rows_json(table, final_payload)
            if errors:
                print(errors)
                raise Exception('Big query insert error => {}'.format(str(errors)))
        except Exception as e:
            error_message = f"[Queued for retry] | {str(e)}"
            post_to_slack(message=error_message)
            print(error_message)
            raise
    return 'Ok'
