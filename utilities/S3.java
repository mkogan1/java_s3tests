import com.amazonaws.ClientConfiguration;
import static java.util.stream.Collectors.joining;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.model.Stream;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.util.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class S3 {
	
	public AmazonS3 getCLI() {
		
		Properties prop = new Properties();
		InputStream input = null;
		try {
			input = new FileInputStream("config.properties");
			try {
				prop.load(input);
			} catch (IOException e) {
				
				e.printStackTrace();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		String endpoint = prop.getProperty("endpoint");
		String accessKey = prop.getProperty("access_key");
		String secretKey = prop.getProperty("access_secret");
		String prefix = prop.getProperty("bucket_prefix");
		Boolean issecure = Boolean.parseBoolean(prop.getProperty("is_secure"));
		
		AmazonS3 svc = getConn(endpoint, accessKey, secretKey, issecure);

		return svc;
	}
	
	public String getPrefix()
	{
		String prefix = "jnan";
		
		return prefix;
	}
	
	
	@SuppressWarnings("deprecation")
	public AmazonS3 getConn(String endpoint,String accessKey, String secretKey,Boolean issecure) {
		
		ClientConfiguration clientConfig = new ClientConfiguration();
		AmazonS3ClientBuilder.standard().setEndpointConfiguration(
				new AwsClientBuilder.EndpointConfiguration(endpoint, "mexico"));
		
		if (issecure){
			clientConfig.setProtocol(Protocol.HTTP);
		}else {
			
			clientConfig.setProtocol(Protocol.HTTP);
		}
		
		AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
		@SuppressWarnings("deprecation")
		AmazonS3 s3client = new AmazonS3Client(credentials); //

		s3client.setEndpoint(endpoint);

		s3client.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(true));
		
		return s3client;
	}
	
	public String getBucketName(String prefix) {
		
		Random rand = new Random(); 
		int num = rand.nextInt(50);
		String randomStr = UUID.randomUUID().toString();
		
		return prefix + randomStr + num;
	}
	
	public String repeat(String str, int count){
	    if(count <= 0) {return "";}
	    return new String(new char[count]).replace("\0", str);
	}
	
	public String[] EncryptionSseCustomerWrite(int file_size) {
		
		String prefix = getPrefix();
		String bucket_name = getBucketName(prefix);
		AmazonS3 svc = getCLI();
		String key ="key1";
		String data = repeat("testcontent", file_size);
		
		svc.createBucket(bucket_name);	
		
		PutObjectRequest putRequest = new PutObjectRequest(bucket_name, key, data);
		ObjectMetadata objectMetadata = new ObjectMetadata();
		objectMetadata.setContentLength(data.length());
		objectMetadata.setHeader("x-amz-server-side-encryption-customer-algorithm", "AES256");
		objectMetadata.setHeader("x-amz-server-side-encryption-customer-key", "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=");
		objectMetadata.setHeader("x-amz-server-side-encryption-customer-key-md5", "DWygnHRtgiJ77HCm+1rvHw==");
		putRequest.setMetadata(objectMetadata);
		svc.putObject(putRequest);
		
		
		String rdata = svc.getObjectAsString(bucket_name, key);
		
		//trying to return two values
		String arr[] = new String[2];
        arr[0]= data;
        arr[1] =  rdata;
		
		return arr;
				
	}
	

	public Bucket createKeys(String[] keys) {
		
		String prefix = getPrefix();
		String bucket_name = getBucketName(prefix);
		AmazonS3 svc = getCLI();
		
		Bucket bucket = svc.createBucket(bucket_name);
		
		for (String k : keys) {
			
			svc.putObject(bucket.getName(), k, k);
			
		}
		
		return bucket;		
	}
	
}
