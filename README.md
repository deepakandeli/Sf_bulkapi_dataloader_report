
# Extract Salesforce Bulk dataload log files
Salesforce's Bulk api to load large volume of data is very useful but it unfortunately has non productive interface when it comes to logs.

While we learn how many batches were processed, # of records loaded succesfully and # of records that failed. But, it lacks the ability to provide a comprehensive list of failed records and corresponding failure message.

This node application retrieves the logs in following format
- Raw request and result csv files for each batch
- Merged request and result csv file for each batch
- Merged request and result csv file for all batch


## Run Locally

Install npm package

```bash
  npm i sf_bulkapi_dataloader_report
```

Command to run logs related to a job 

```bash
  node Dataloader_report_merge.js  --CredFile "CREDS_PREPROD.json" --logfile "./Logs/" --jobId "750F400000ABJeb"
```
