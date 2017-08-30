
 ## S3 compatibility tests

This is a set of completely unofficial Amazon AWS S3 compatibility
tests, that will hopefully be useful to people implementing software
that exposes an S3-like API.

The tests only cover the REST interface.

### Setup

The tests use the [AWS Java SDK]() and  TestNG framework.

### Get the source code

Clone the repository

	git clone https://github.com/nanjekyejoannah/java_s3tests
	cd java_s3tests

### Edit Configuration

	cp config.properties.sample config.properties

The configuration file looks something like this:

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


### 0. Install Grandle
	
Ubuntu

	sudo apt-get update
	sudo apt-get install gradle

Fedora
	
	dnf update
	dnf install gradle
	
#### 1. Build 

	gradle build

#### 2. Run the Tests

	gradle clean test

