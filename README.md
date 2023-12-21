# bqupload

bqupload is a server for streaming data to BigQuery tables. 

- It handles combining rows into batches for efficient upload
- It holds data in memory until it is uploaded to BigQuery
- It will spill data to disk if things start to back up or the server is terminated
- It will load saved data from disk and upload it to BigQuery on startup
- It will retry uploads if they fail.

bqupload has its own proprietary protocol for receiving data that uses plenc.
This means you can use ordinary Go structs to describe rows. The client is in 
the github.com/philpearl/bqupload/client package.