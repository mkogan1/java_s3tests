
 ## S3 compatibility tests

This is a set of completely unofficial Amazon AWS S3 compatibility
tests, that will hopefully be useful to people implementing software
that exposes an S3-like API.

The tests only cover the REST interface.

### Setup

The tests use the [AWS Java SDK](). The tests use TestNG framework.

### Get the source code

Clone the repository

	git clone https://github.com/nanjekyejoannah/java_s3tests

### Edit Configuration

Edit the configuration with your connection details.

	bucket_prefix = joannah
	
	[s3main]
	
	access_key = 0555b35654ad1656d804
	access_secret = h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q==
	region = mexico
	endpoint = http://localhost:8000/
	port = 8000
	display_name = somename
	email = someone@gmail.com
	is_secure = false
	SSE = AES256
	kmskeyid = barbican_key_id

### RGW

The tests connect to the Ceph RGW ,therefore you shoud have started your RGW and use the credentials you get. Details on building Ceph and starting RGW can be found in the [ceph repository](https://github.com/ceph/ceph).


### 1. Running the tests

Change to the directory having the tests. 

	cd s3tests
	
#### Install ant

Follow the procedures [here](https://www.linuxhelp.com/how-to-install-apache-ant-on-ubuntu/)

Build and run the tests with Ant. 

	ant test

