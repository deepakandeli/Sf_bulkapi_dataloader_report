
# Extract Salesforce Bulk dataload log files
Salesforce's Bulk api to load large volume of data is very useful but it unfortunately has non productive interface when it comes to logs.

While we learn how many batches were processed, # of records loaded succesfully and # of records that failed. But, it lacks the ability to provide a comprehensive list of failed records and corresponding failure message.

This node application retrieves the logs in following format
- Raw request and result csv files for each batch
- Merged request and result csv file for each batch
- Merged request and result csv file for all batch


## Run Locally

Clone the project

```bash
  git clone https://github.com/deepakandeli/Sf_bulkapi_dataloader_report.git
```

Command to run logs related to a job 

```bash
  node WL_dataloader_report_merge.js  --CredFile "CREDS_PREPROD.json" --logfile "./Logs/" --jobId "750F400000ABJeb"
```
