//node WL_dataloader.js --jobId 'jobId' --CredFile SF_PREPROD.json
const fs = require('fs');
var sql = require('mssql/msnodesqlv8');
const jsforce = require('jsforce');
const https = require('https');
const querystring = require('querystring');
const { parse } = require('json2csv');
const csv = require('csv-parser');
const util = require('util'); // Import the 'util' module
const path = require('path');

// Salesforce Connected App credentials
let clientId,clientSecret,grantType,tokenEndpoint;

// Parse command line arguments
const args = process.argv.slice(2);
let tableName, salesforceObjectName, operation, extId,qry, credfilePath,jobId,logfile,mergeDir,logBatchfile;
var credFileData;

// Function to display help message
const displayHelp = () => {
    console.log('Usage: node WL_dataloader.js [--jobId <jobId>] [--CredFile <credfilePath>] [--help]');
	console.log('--jobId    : Specify the JobId');
    console.log('--CredFile    : Specify the File path to credentails.');
	console.log('--logfile    : Specify the File path to place error file.');
	
    process.exit(0);
};

// Process command line arguments
for (let i = 0; i < args.length; i++) {
    if (args[i] === '--help') {
        displayHelp();
    }
    if (args[i] === '--jobId' && i < args.length - 1) {
        jobId = args[i + 1];
    }
    if (args[i] === '--CredFile' && i < args.length - 1) {
        credfilePath = args[i + 1];
    }
    if (args[i] === '--logfile' && i < args.length - 1) {
        logfile = args[i + 1];
    }	
}

// Check if required parameters are provided
if (!jobId || !credfilePath) {
    console.error('Error: --jobId, --CredFile, --logfile parameters are required.');
    displayHelp();
}
console.log('credfilePath '+credfilePath);
console.log('jobId '+jobId);
console.log('logfile '+logfile);


// Read JSON file
fs.readFile(credfilePath, 'utf8', (err, creddata) => {
  if (err) {
	console.error(`Error reading file: ${err}`);
	process.exit(1);
  }
  
  try{
	const credFileData = JSON.parse(creddata);
	// Construct request body
	const requestBody = querystring.stringify({
	  grant_type: credFileData.grantType,
	  client_id: credFileData.clientId,
	  client_secret: credFileData.clientSecret
	});

	// Configure the HTTP request options
	const requestOptions = {
	  method: 'POST',
	  headers: {
		'Content-Type': 'application/x-www-form-urlencoded',
		'Content-Length': requestBody.length
	  }
	};
	
	tokenEndpoint=credFileData.tokenEndpoint;
	console.log('tokenEndpoint '+tokenEndpoint);
	// Make the HTTP request to authenticate
	const req = https.request(tokenEndpoint, requestOptions, (res) => {
		let data = '';

		  res.on('data', (chunk) => {
			data += chunk;
		  });

		  res.on('end', () => {
			const responseData = JSON.parse(data);
			console.log('Successfully authenticated with Salesforce using client credentials.');
			//console.log('responseData:', responseData);
			console.log('Access Token:', responseData.access_token);
			console.log('Instance URL:', responseData.instance_url);
			// You can proceed with your Salesforce operations here
			var conn = new jsforce.Connection({
				instanceUrl : responseData.instance_url,
				accessToken : responseData.access_token
			});
			conn.bulk.pollInterval = 60000; // 5 sec
			conn.bulk.pollTimeout = 60000; // 60 sec
			
			pollForRequestResult(conn,jobId);
			

		});
	});
	// Handle errors
	req.on('error', (error) => {
	  console.error('Error:', error);
	});

	// Send the request body
	req.write(requestBody);
	req.end();	
  }catch(parseError){
	console.error(`Error parsing JSON: ${parseError}`);
	process.exit(1);
  } 
});

function splitArrayIntoChunks(array, batchSize) {
    // Initialize an empty array to store chunks
    const chunks = [];

    // Iterate through the array
	console.log('batchSize '+batchSize);
    for (let i = 0; i < array.length; i += batchSize) {
        // Slice the array into chunks of batchSize and push them into the chunks array
		console.log('i '+i+' (i + batchSize) '+(i + batchSize));
        chunks.push(array.slice(i, i + batchSize));
		
    }

    return chunks;
}

function executeBulkLoad(job,recordsToInsert){
	const arrayOfBatches = [];
	record2LoadArr=splitArrayIntoChunks(recordsToInsert,batchsize);
	//record2LoadArr=recordsToInsert;
	console.log('record2LoadArr.length '+record2LoadArr.length);
	
	
	Promise.all(
		record2LoadArr.map(function(recs) {
			var batch = job.createBatch();
			console.log('recs '+recs.length);
			batch.execute(recs);
			return new Promise(function(resolve, reject) {
						batch.on("queue", function(batchInfo) {
							batchId = batchInfo.id;
							var batch = job.batch(batchId);
							batch.on('response', function(res) { resolve(res); });
							batch.on('error', function(err) { reject(err); });
							batch.poll(5*1000, 60*1000);
						});
					});	
		})
	).then(function(rets) {
			var successrec=0;
			var errorrec=0;
			var procrec=0;
			var errormsg='';
			for(var i=0;i<rets.length;i++){
				for(var ii=0;ii<rets[i].length;ii++){
					procrec++;
					if(rets[i][ii].success==true){
						successrec++;
					}else{
						errorrec++;
						if(errormsg.length==0){
							errormsg=rets[i][ii].errors[0];
						}else{
							errormsg='\n'+rets[i][ii].errors[0];
						}
					}
				}
				console.log('Processed : '+procrec+' with '+errorrec+' errors '+errormsg);
			}
			job.close();
		}, function(err) { console.log(err);});
}












//Report Functions - Start



// Function to check status of a batch
async function checkBatchStatus(jobId, batchId, conn) {
    try {
        const result = await conn.bulk.job(jobId).batch(batchId).check();
        return result;
    } catch (err) {
        console.error('Error checking batch status:', err);
        throw err;
    }
}
// Function to accumulate error messages
function accumulateErrors(errors, rowErrors) {
    rowErrors.forEach(rowError => {
        errors.push(rowError.message);
    });
}

// Main function to poll for batch status and accumulate errors
async function pollForBatchStatus(jobId, conn) {
    try {
		 jb = conn.bulk.job(jobId);
		const batches = await jb.list();
		cnt=0;
		var promiseStack = [];
		for (let batch of batches) {
			const req = logBatchfile+batch.id+'_req.csv';
			const res = logBatchfile+batch.id+'_res.csv';
			const outputFileMerge = mergeDir+batch.id+'_merged.csv';


			await makeBulkAPIRequest(conn.accessToken,jobId,batch.id,'GET',conn.instanceUrl,req);
			await makeBulkAPIResult(conn.accessToken,jobId,batch.id,'GET',conn.instanceUrl,res);
			console.log('Request file '+req+' & Result file '+res+' were created');
			const reqtMerge = mergeCSVFiles(req,res,outputFileMerge);			
			promiseStack.push(reqtMerge);
		}		
		return new Promise((resolve, reject) => {
			Promise.all(promiseStack).then(function(rets) {
				console.log('All individual batch request and result extracts merged to respective batch file');
				resolve(); // Resolve the promise when the response is received
			}, function(err) { 
				console.log(err);
				reject(`Error Retrieving Request & Result : ${err}`);
			});

		  });
    } catch (err) {
        console.error('Error polling for batch status:', err);
        throw err;
    }
}
// Main function to start the polling process
async function pollForRequestResult(conn,jobId) {
    try {
		logfile=logfile+'/'+jobId+'/';
		if (!fs.existsSync(logfile)){
			fs.mkdirSync(logfile);
		}
		console.log('Folder '+logfile+' created/found');

		logBatchfile=logfile+'/batchextract/';
		if (!fs.existsSync(logBatchfile)){
			fs.mkdirSync(logBatchfile);
		}
		console.log('Folder '+logBatchfile+' created/found');
		mergeDir = logfile+'/merged/';		
		if (!fs.existsSync(mergeDir)){
			fs.mkdirSync(mergeDir);
		}
		console.log('Folder '+mergeDir+' created/found');


        await pollForBatchStatus(jobId, conn);
		const outputFile = logfile+jobId+'_merged.csv'
		await mergeCSVFilesInFolder(mergeDir, outputFile);
		console.log('Final merged single file created '+outputFile);

    } catch (err) {
        console.error('Error:', err);
    }
}

// Function to write errorMap to a CSV file
async function writeErrorMapToCSV(errorMap, filePath) {
    try {
			const csvData = [];

			// Convert errorMap to an array of objects
			for (let batchId in errorMap) {
				const errors = errorMap[batchId];
				errors.forEach(error => {
					csvData.push({ Slno: batchId, ErrorMessage: error });
				});
			}

			// Convert array of objects to CSV string			
			console.log('csvData.length '+csvData.length);
			
			if(csvData.length>0){
				const csv = parse(csvData);
				// Write CSV string to file
				fs.writeFileSync(filePath, csv, 'utf8');
				console.log(`Error Map written to ${filePath}`);
			}else{
				console.log('No data');	
			}
    } catch (err) {
        console.error('Error writing error map to CSV:', err);
        throw err;
    }
}
//Report Functions - End

// Function to make a REST API call to Salesforce Bulk API
async function makeBulkAPIRequest(accessToken, jobId, batchId, method, instanceURL,reqLogFile) {
    try {
		requestEndpoint=instanceURL+'/services/async/60.0/job/'+jobId+'/batch/'+batchId+'/request'
		const bulkRequestOptions = {
			method: 'Get',
			headers: {
				'X-SFDC-Session': `${accessToken}`,
				'Content-Type': 'text/csv',
			}
		  };

		  return new Promise((resolve, reject) => {
			const req = https.request(requestEndpoint, bulkRequestOptions, (res) => {
				let data = '';
	
				// Accumulate data chunks as they come in
				res.on('data', chunk => {
				  data += chunk;
				});
				
			  
				// Handle end of response
				res.on('end', () => {
					requestCSV = stringToCSV(data);
					writeCSVToFile(requestCSV,reqLogFile);
					resolve(); // Resolve the promise when the response is received
				});
			});
			req.on('error', (error) => {
                reject(`Error making Bulk API request: ${error}`);
            });
			req.write('');
            req.end();
		  });
    } catch (error) {
        throw new Error(`Error making Bulk API request: ${error}`);
    }
}

async function makeBulkAPIResult(accessToken, jobId, batchId, method, instanceURL,resLogFile) {
    try {
		resultEndpoint=instanceURL+'/services/async/60.0/job/'+jobId+'/batch/'+batchId+'/result'
		const bulkRequestOptions = {
			method: 'Get',
			headers: {
				'X-SFDC-Session': `${accessToken}`,
				'Content-Type': 'text/csv',
			}
		  };
		  //console.log('resultEndpoint '+resultEndpoint);
		  return new Promise((resolve, reject) => {
			const req = https.request(resultEndpoint, bulkRequestOptions, (res) => {
				let data = '';
	
				// Accumulate data chunks as they come in
				res.on('data', chunk => {
				  data += chunk;
				});
			  
				// Handle end of response
				return res.on('end', () => {
				  requestCSV = stringToCSV(data);
				  writeCSVToFile(requestCSV,resLogFile);
				  resolve();
				});			
			});
			req.on('error', (error) => {
                reject(`Error making Bulk API request: ${error}`);
            });
			req.write('');
            req.end();
		  });
    } catch (error) {
        throw new Error(`Error making Bulk API result: ${error}`);
    }
}

// Function to convert a string to CSV format
function stringToCSV(inputString) {
    // Split the input string into rows
    const rows = inputString.trim().split('\n');
	//console.log('rows '+rows.length);
    // Initialize an empty array to store CSV rows
    const csvRows = [];
    // Iterate through each row
    rows.forEach(row => {
        // Split the row into columns
        const columns = row.split(',');
        // Enclose each column in double quotes and join them with commas
        //const csvRow = columns.map(column => `"${column}"`).join(',');
		const csvRow = columns.map(column => `${column}`).join(',');
        // Push the CSV row to the csvRows array
        csvRows.push(csvRow);
    });
    // Join the CSV rows with newlines to form the CSV string
    return csvRows.join('\n');
	//return csvRows;
}

// Function to write a CSV string to a file
function writeCSVToFile(csvData, filePath) {
	if(csvData.length>0){
		// Write CSV string to file
		fs.writeFileSync(filePath, csvData, 'utf8');
		//console.log(`written to ${filePath}`);
	}else{
		console.log('No data');	
	}
}


// Function to read a CSV file and return an array with rows
function readCSVFile(filePath) {
    return new Promise((resolve, reject) => {
        const rows = [];
		//console.log('filePath '+filePath);
        fs.createReadStream(filePath)
            .pipe(csv())
			.on('headers', (headers) => {
				rows.push(headers);
			  })
            .on('data', (row) => {
                // Push each row to the rows array
                rows.push(row);
            })
            .on('end', () => {
                // Resolve the promise with the array of rows when reading is complete
                resolve(rows);
            })
            .on('error', (error) => {
                // Reject the promise if an error occurs
                reject(error);
            });
    });
}

// Function to write an array to a CSV file
function writeCSVFile(data, filePath) {
    return new Promise((resolve, reject) => {
        const csvData = data.join('\n');
        fs.writeFile(filePath, csvData, 'utf8', (err) => {
            if (err) {
                reject(err);
            } else {
                resolve(filePath);
            }
        });
    });
}

// Function to read two CSV files into two arrays and merge them into a new CSV file
async function mergeCSVFiles(file1, file2, outputFile) {
    try {
        // Read the first CSV file into an array
        const rows1 = await readCSVFile(file1);
        //console.log('Rows from file 1:', rows1);

        // Read the second CSV file into another array
        const rows2 = await readCSVFile(file2);
        //console.log('Rows from file 2:', rows2);

        // Merge rows from rows1 and rows2 into a new array
        const mergedCSV = [];
        for (let i = 0; i < Math.min(rows1.length, rows2.length); i++) {
            mergedCSV.push(Object.values(rows2[i]).join(',')+','+ Object.values(rows1[i]).join(','));
        }

        //console.log('Merged CSV:', mergedCSV);

        // Write merged CSV data to a new file
        await writeCSVFile(mergedCSV, outputFile);
        //console.log('Merged CSV written to:', outputFile);

    } catch (error) {
        console.error('Error reading or writing CSV files:', error);
    }
}

// Function to merge all CSV files in a folder into a single file
async function mergeCSVFilesInFolder(folderPath, outputFile) {
    let headers = [];
    const outputStream = fs.createWriteStream(outputFile, { flags: 'a' });

    // Promisify fs.readdir to list files in the directory
    const readdir = util.promisify(fs.readdir);

    try {
        const files = await readdir(folderPath);

        // Process each file in the folder
        for (const file of files) {
            const filePath = path.join(folderPath, file);

            // Skip if it's not a CSV file
            if (!filePath.endsWith('.csv')) {
                continue;
            }

            // Read the CSV file and append its contents to the output file
            await new Promise((resolve, reject) => {
                fs.createReadStream(filePath)
                    .pipe(csv())
                    .on('data', (data) => {
                        if (headers.length === 0) {
                            headers = Object.keys(data);
                            outputStream.write(headers.join(',') + '\n');
                        }
                        outputStream.write(Object.values(data).join(',') + '\n');
                    })
                    .on('end', () => {
                        //console.log(`File ${file} has been processed.`);
                        resolve();
                    })
                    .on('error', (error) => {
                        console.error(`Error reading file ${file}:`, error);
                        reject(error);
                    });
            });
        }
    } catch (error) {
        console.error('Error reading directory:', error);
    } finally {
        // Close the output stream when all files are processed
        outputStream.end();
    }
}