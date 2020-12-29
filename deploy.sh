gcloud functions deploy bill_transfer_report \
 --env-vars-file env.yaml \
 --trigger-topic bill_transfer_report \
 --region europe-west1 \
 --memory 256MB \
 --runtime python37 \
 --retry \
 --project kudi-209818
