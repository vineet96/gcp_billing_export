from google.cloud import bigquery
from google.cloud import storage
def export_to_gcs(event, context):
  
  QUERY = 'SELECT project.name as project, EXTRACT(MONTH FROM usage_start_time) as month, ROUND(SUM(cost), 2) as charges, ROUND(SUM((SELECT SUM(amount) FROM UNNEST(credits))),2) as credits from `vineetagarwal.billing_data.gcp_billing_export` GROUP BY project, month ORDER by project, month'
  QUERY2 = "select distinct project.name from `google.com:ce-project-auditing.billing_data.gcp_billing_export_v1_01382E_07CCE9_615E10` LIMIT 100;"
  QUERY3 = 'SELECT distinct project.name from `vineetagarwal.billing_data.gcp_billing_export` LIMIT 10'
  bq_client = bigquery.Client()
  query_job = bq_client.query(QUERY) 
  rows_df = query_job.result().to_dataframe() 
  storage_client = storage.Client()
  bucket = storage_client.get_bucket('vineetagarwal')
  blob = bucket.blob('Add_to_Cart.csv')
  blob.upload_from_string(rows_df.to_csv(index=False,encoding='utf-8'),content_type='application/octet-stream')


  