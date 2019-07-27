#%%
import os, requests, json, pandas as pd

## Config
credentials_file = 'domo_credentials.json'
output_dir = 'output3'

## Functions
api_call = lambda url: json.loads(requests.get(url, headers=headers).text)

## Execution

# Variables
creds = json.loads(open(credentials_file, 'r').read())
headers = {'X-DOMO-Developer-Token': creds['auth_token'], 'Content-Type': 'application/json', 'User-Agent': 'DomoR-test/1.0'}

# Create the output directory if it doesn't exist
if not os.path.exists(output_dir):
    os.mkdir(output_dir)

# Get complete list of Dataflows
all_dataflows = pd.DataFrame(api_call(creds["list_dataflows_url"].replace('<customer_url>', creds["customer_url"])))

# Extract only SQL Dataflows from that list
sql_dataflows = all_dataflows[all_dataflows["databaseType"] != 'ETL'][['name', 'id', 'databaseType']].sort_values('id')

for i,dataflow in sql_dataflows.iterrows():
    dataflow_id = str(dataflow['id'])
    dataflow_name = dataflow['name'].replace(':','-').replace('/','-')
    dataflow_type = dataflow['databaseType']
    dataflow_dir = dataflow_id + '-' + dataflow_type + '-' + dataflow_name

    print('Downloading {}...'.format(dataflow_dir))
    response = api_call(creds["dataprocessing_url"].replace('<dataflow_id>', dataflow_id))

    # Create the DataFlow directory if it doesn't exist
    dir_name = '{}/{}'.format(output_dir, dataflow_dir)
    if not os.path.exists(dir_name):
        os.mkdir(dir_name)
    
    for step in response['actions']:
        # If it's a transformation step that generates a 'Table'...
        if step['type'] == 'GenerateTableAction':
            
            # Extract and raw SQL text and store the script in the output directory
            f = open('{}/{}.sql'.format(dir_name, step['tableName']), 'w')
            f.write(step['selectStatement'])
            f.close()

print("\nAll the SQLs from all the Dataflows' transformation steps have been downloaded!")
