#%%
import os, requests, json, pandas as pd

## Config
creds = json.loads(open('domo_credentials.json','r').read())
output_dir = 'output'
headers = {'X-DOMO-Developer-Token': creds['auth_token'], 'Content-Type': 'application/json', 'User-Agent': 'DomoR-test/1.0'}

## Functions
api_call = lambda url: json.loads(requests.get(url, headers=headers).text)

## Execution

# Get complete list of datasets
all_dataflows = pd.DataFrame(api_call(creds["list_dataflows_url"].replace('<customer_url>', creds["customer_url"])))

# Extract only SQL dataflows from that list
sql_dataflows = all_dataflows[all_dataflows["databaseType"] != 'ETL'][['name','id','description','databaseType']]

for i,dataflow in sql_dataflows.iterrows():
    dataflow_id = str(dataflow['id'])
    dataflow_name = dataflow['name'].replace(':','-')

    print('Downloading {}...'.format(dataflow_id + '-' + dataflow_name))
    response = api_call(creds["dataprocessing_url"].replace('<dataflow_id>', dataflow_id))
    
    for step in response['actions']:
        if step['type'] == 'GenerateTableAction':
            # Extract raw SQL text
            sql = step['selectStatement']
            
            # Store in the output directory
            dir_name = '{}/{}'.format(output_dir, dataflow_id + '-' + dataflow_name)
            if not os.path.exists(dir_name):
                os.mkdir(dir_name)
            f = open('{}/{}.sql'.format(dir_name, step['tableName']), 'w')
            f.write(sql)
            f.close()


