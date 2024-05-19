
# Extract Salesforce Bulk dataload log files
Salesforce's Bulk api to load large volume of data is very useful but it unfortunately has non productive interface when it comes to logs.

While we learn how many batches were processed, # of records loaded succesfully and # of records that failed. But, it lacks the ability to provide a comprehensive list of failed records and corresponding failure message.

This node application retrieves the logs in following format
- Raw request and result csv files for each batch
- Merged request and result csv file for each batch
- Merged request and result csv file for all batch

## Installation

### Salesforce Connected App

The app uses Salesforce's Oauth "Client Credential" flow.
This requires a System administrator creates a connected app which uses client credential setting as per images
[Note: Client Credentail flow enables connection to Salesforce without the need for Salesforce Username and password]

#### Connected App Screenshot
- ![Connected App Screenshot](https://d259t2jj6zp7qm.cloudfront.net/images/20230328082158/image-46-1795x1000.png)

#### Client Credentials Flow Screenshot
- ![Client Credentials Flow Screenshot](https://d259t2jj6zp7qm.cloudfront.net/images/20230328082306/image-48.png)

### Dataload_Files/CREDS_PREPROD.json
Copy the client Id and client secret generated from the above connected app and update the CREDS_PREPROD.json

## Run Locally

Install npm package

```bash
  npm i sf_bulkapi_dataloader_report
```

Create a javascript file say 'RetrieveDataLoadLogs.js with the following line
```bash
const sf_bulkapi_dataloader_report = require('sf_bulkapi_dataloader_report');
```


Create a folder 'logs'
Command to run logs related to a job
```bash
  node RetrieveDataLoadLogs.js --CredFile "CREDS_PREPROD.json" --logfile "./logs/" --jobId "750F400000ABJeb"
```
