import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.transfer.Copy;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.MultipleFileDownload;
import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.amazonaws.services.s3.transfer.PauseResult;
import com.amazonaws.services.s3.transfer.PersistableDownload;
import com.amazonaws.services.s3.transfer.PersistableTransfer;
import com.amazonaws.services.s3.transfer.PersistableUpload;
import com.amazonaws.services.s3.transfer.Transfer;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferProgress;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.util.StringUtils;

public class ObjectTest {
	
	//To do... provide singleton to these instances
	private static S3 utils =  new S3();
	AmazonS3 svc = utils.getCLI();
	String prefix = utils.getPrefix();

	@AfterMethod
	public  void tearDownAfterClass() throws Exception {
		
		utils.tearDown();	
	}


	@BeforeMethod
	public void setUp() throws Exception {
	}

	@Test
	public void testObjectWriteToNonExistantBucket() {
		
		String non_exixtant_bucket = utils.getBucketName(prefix);
		
		try {
			
			svc.putObject(non_exixtant_bucket, "key1", "echo");
		} catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode(), "NoSuchBucket");
		}
	}
	
	@Test
	public void testObjectReadNotExist() {
		
		String bucket_name = utils.getBucketName(prefix);
		svc.createBucket(new CreateBucketRequest(bucket_name));
		
		try {
			
			svc.getObject(bucket_name, "key");
		} catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode(), "NoSuchKey");
		}
	}
	
	@Test
	public void testObjectReadFromNonExistantBucket() {
		
		String bucket_name = utils.getBucketName(prefix);
		svc.createBucket(new CreateBucketRequest(bucket_name));
		
		try {
			
			svc.getObject(bucket_name, "key");
		} catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode(), "NoSuchKey");
		}
	}
	
	
	@Test
	public void testObjectWriteCheckEtag() {
		
		String bucket_name = utils.getBucketName(prefix);
		String key = "key";
		String Etag = "37b51d194a7513e45b56f6524f2d51f2";
		
		svc.createBucket(new CreateBucketRequest(bucket_name));
		
		svc.putObject(bucket_name, key, "bar");
		
		S3Object resp = svc.getObject(new GetObjectRequest(bucket_name, key));
		Assert.assertEquals(resp.getObjectMetadata().getETag(), Etag);
		
	}
	
	@Test
	public void testObjectWriteCacheControl() {
		
		String bucket_name = utils.getBucketName(prefix);
		String key = "key1";
		String content = "echo lima golf";
		String auth = "x07";
		String cache_control = "public, max-age=14400";
		
		svc.createBucket(new CreateBucketRequest(bucket_name));

		byte[] contentBytes = content.getBytes(StringUtils.UTF8);
		InputStream is = new ByteArrayInputStream(contentBytes);
		
		ObjectMetadata metadata = new ObjectMetadata();
		metadata.setContentLength(contentBytes.length);
		metadata.setHeader("Cache-Control", cache_control );
		
		svc.putObject(new PutObjectRequest(bucket_name, key, is, metadata));
		
		S3Object resp = svc.getObject(new GetObjectRequest(bucket_name, key));
		Assert.assertEquals(resp.getObjectMetadata().getCacheControl(), cache_control);
		
	}
	
	@Test
	public void testObjectWriteReadUpdateReadDelete() {
		
		String bucket_name = utils.getBucketName(prefix);
		String key = "key1";
		String content = "echo lima golf";
		
		svc.createBucket(new CreateBucketRequest(bucket_name));
		
		svc.putObject(bucket_name, key, content);
		String got = svc.getObjectAsString(bucket_name, key);
		Assert.assertEquals(got, content);
		
		//update
		String newContent = "charlie echo";
		svc.putObject(bucket_name, key, newContent);
		got = svc.getObjectAsString(bucket_name, key);
		Assert.assertEquals(got, newContent);
		
		svc.deleteObject(bucket_name, key);
		try {
			
			got = svc.getObjectAsString(bucket_name, key);
		}catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode(), "NoSuchKey");
		}

		
	}
	
	@Test
	public void testObjectCopyBucketNotFound() {
		
		String bucket1 = utils.getBucketName(prefix);
		String bucket2 = utils.getBucketName(prefix);
		String key ="key1";
		
		
		try {
			
			svc.copyObject(bucket1, key, bucket2, key);
		} catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode(), "NoSuchBucket");
		}
		
	};
	
	@Test
	public void TestObjectCopyKeyNotFound() {
		
		String bucket1 = utils.getBucketName(prefix);
		String bucket2 = utils.getBucketName(prefix);
		String key ="key1";
		
		svc.createBucket(bucket1);
		svc.createBucket(bucket2);
		
		try {
			
			svc.copyObject(bucket1, key, bucket2, key);
		} catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode(), "NoSuchKey");
		}
		
	}
	
	
	@Test
	public void testObjectCreateBadContenttypevalid() {
		
		String bucket_name = utils.getBucketName(prefix);
		String key = "key1";
		String content = "echo lima golf";
		
		svc.createBucket(new CreateBucketRequest(bucket_name));

		byte[] contentBytes = content.getBytes(StringUtils.UTF8);
		InputStream is = new ByteArrayInputStream(contentBytes);
		
		ObjectMetadata metadata = new ObjectMetadata();
		metadata.setContentType("text/plain");
		metadata.setContentLength(contentBytes.length);
		
		svc.putObject(new PutObjectRequest(bucket_name, key, is, metadata));
		
		AssertJUnit.assertEquals(svc.getObject(bucket_name, key).getObjectMetadata().getContentType(), "text/plain");
		
	}
	
	@Test
	public void testObjectCreateBadContenttypeEmpty() {
		
		String bucket_name = utils.getBucketName(prefix);
		String key = "key1";
		String content = "echo lima golf";
		String contType = " ";
		
		svc.createBucket(new CreateBucketRequest(bucket_name));

		byte[] contentBytes = content.getBytes(StringUtils.UTF8);
		InputStream is = new ByteArrayInputStream(contentBytes);
		
		try {
			
			ObjectMetadata metadata = new ObjectMetadata();
			metadata.setContentType(contType);
			metadata.setContentLength(contentBytes.length);
			
			svc.putObject(new PutObjectRequest(bucket_name, key, is, metadata));
			
		} catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode(), "400 Bad Request");
		}
	}
	
	@Test
	public void testObjectCreateBadContenttypeNone() {
		
		String bucket_name = utils.getBucketName(prefix);
		String key = "key1";
		String content = "echo lima golf";
		String contType = "";
		
		svc.createBucket(new CreateBucketRequest(bucket_name));

		byte[] contentBytes = content.getBytes(StringUtils.UTF8);
		InputStream is = new ByteArrayInputStream(contentBytes);
		
		try {
			
			ObjectMetadata metadata = new ObjectMetadata();
			metadata.setContentType(contType);
			metadata.setContentLength(contentBytes.length);
			
			svc.putObject(new PutObjectRequest(bucket_name, key, is, metadata));
			
		} catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode(), "400 Bad Request");
		}
		
	}
	
	@Test
	public void testObjectCreateBadContenttypeUnreadable() {
		
		String bucket_name = utils.getBucketName(prefix);
		String key = "key1";
		String content = "echo lima golf";
		String contType = "\\x08";
		
		svc.createBucket(new CreateBucketRequest(bucket_name));

		byte[] contentBytes = content.getBytes(StringUtils.UTF8);
		InputStream is = new ByteArrayInputStream(contentBytes);
		
		try {
			
			ObjectMetadata metadata = new ObjectMetadata();
			metadata.setContentType(contType);
			metadata.setContentLength(contentBytes.length);
			
			svc.putObject(new PutObjectRequest(bucket_name, key, is, metadata));
			
		} catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode(), "400 Bad Request");
		}
		
	}
	
	@Test
	public void testObjectCreateBadAuthorizationUnreadable() {
		
		try {
			
			String bucket_name = utils.getBucketName(prefix);
			String key = "key1";
			String content = "echo lima golf";
			String auth = "x07";
			
			svc.createBucket(new CreateBucketRequest(bucket_name));

			byte[] contentBytes = content.getBytes(StringUtils.UTF8);
			InputStream is = new ByteArrayInputStream(contentBytes);
			
			ObjectMetadata metadata = new ObjectMetadata();
			metadata.setContentLength(contentBytes.length);
			metadata.setHeader("Authorization", auth );
			
			svc.putObject(new PutObjectRequest(bucket_name, key, is, metadata));
			
		} catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode(), "400 Bad Request");
		}
		
	}
	
	@Test
	public void testObjectCreateBadAuthorizationEmpty() {
		
		try {
			
			String bucket_name = utils.getBucketName(prefix);
			String key = "key1";
			String content = "echo lima golf";
			String auth = " ";
			
			svc.createBucket(new CreateBucketRequest(bucket_name));

			byte[] contentBytes = content.getBytes(StringUtils.UTF8);
			InputStream is = new ByteArrayInputStream(contentBytes);
			
			ObjectMetadata metadata = new ObjectMetadata();
			metadata.setContentLength(contentBytes.length);
			metadata.setHeader("Authorization", auth );
			
			svc.putObject(new PutObjectRequest(bucket_name, key, is, metadata));
			
		} catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode(), "400 Bad Request");
		}
		
	}
	
	@Test
	public void testObjectCreateBadAuthorizationNone() {
		
		try {
			
			String bucket_name = utils.getBucketName(prefix);
			String key = "key1";
			String content = "echo lima golf";
			String auth = "";
			
			svc.createBucket(new CreateBucketRequest(bucket_name));

			byte[] contentBytes = content.getBytes(StringUtils.UTF8);
			InputStream is = new ByteArrayInputStream(contentBytes);
			
			ObjectMetadata metadata = new ObjectMetadata();
			metadata.setContentLength(contentBytes.length);
			metadata.setHeader("Authorization", auth );
			
			svc.putObject(new PutObjectRequest(bucket_name, key, is, metadata));
			
		} catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode(), "400 Bad Request");
		}
		
	}
	
	@Test
	public void testObjectCreateBadContentlengthNegative() {
		
		try {
			
			String bucket_name = utils.getBucketName(prefix);
			String key = "key1";
			String content = "echo lima golf";
			long contlength = -1;
			
			svc.createBucket(new CreateBucketRequest(bucket_name));

			byte[] contentBytes = content.getBytes(StringUtils.UTF8);
			InputStream is = new ByteArrayInputStream(contentBytes);
			
			ObjectMetadata metadata = new ObjectMetadata();
			metadata.setHeader("Content-Length", contlength );
			
			svc.putObject(new PutObjectRequest(bucket_name, key, is, metadata));
			
		} catch (IllegalArgumentException err) {
			AssertJUnit.assertNotSame(err.getLocalizedMessage(), null);
		}
		
	}
	
	@Test
	public void testObjectCreateBadExpectEmpty() {
		
		try {
			
			String bucket_name = utils.getBucketName(prefix);
			String key = "key1";
			String content = "echo lima golf";
			String expected = " ";
				
			svc.createBucket(new CreateBucketRequest(bucket_name));

			byte[] contentBytes = content.getBytes(StringUtils.UTF8);
			InputStream is = new ByteArrayInputStream(contentBytes);
				
			ObjectMetadata metadata = new ObjectMetadata();
			metadata.setContentLength(contentBytes.length);
			metadata.setHeader("Expect", expected);
				
			svc.putObject(new PutObjectRequest(bucket_name, key, is, metadata));
			
		} catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode(), "SignatureDoesNotMatch");
		}
		
	}
	
	@Test
	public void testObjectCreateBadExpectUnreadable() {
		
		try {
			
			String bucket_name = utils.getBucketName(prefix);
			String key = "key1";
			String content = "echo lima golf";
			String expected = "\\x07";
				
			svc.createBucket(new CreateBucketRequest(bucket_name));

			byte[] contentBytes = content.getBytes(StringUtils.UTF8);
			InputStream is = new ByteArrayInputStream(contentBytes);
				
			ObjectMetadata metadata = new ObjectMetadata();
			metadata.setContentLength(contentBytes.length);
			metadata.setHeader("Expect", expected);
				
			svc.putObject(new PutObjectRequest(bucket_name, key, is, metadata));
			
		} catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode(), "SignatureDoesNotMatch");
		}
		
	}
	
	@Test
	public void testObjectCreateBadExpectMismatch() {
		
		String bucket_name = utils.getBucketName(prefix);
		String key = "key1";
		String content = "echo lima golf";
		String expected = "200";
			
		svc.createBucket(new CreateBucketRequest(bucket_name));

		byte[] contentBytes = content.getBytes(StringUtils.UTF8);
		InputStream is = new ByteArrayInputStream(contentBytes);
			
		ObjectMetadata metadata = new ObjectMetadata();
		metadata.setContentLength(contentBytes.length);
		metadata.setHeader("Expect", expected);
			
		svc.putObject(new PutObjectRequest(bucket_name, key, is, metadata));	
	}
	
	@Test
	public void TestObjectCreateBadExpectNone() {
		
		String bucket_name = utils.getBucketName(prefix);
		String key = "key1";
		String content = "echo lima golf";
		String expected = "";
			
		svc.createBucket(new CreateBucketRequest(bucket_name));

		byte[] contentBytes = content.getBytes(StringUtils.UTF8);
		InputStream is = new ByteArrayInputStream(contentBytes);
			
		ObjectMetadata metadata = new ObjectMetadata();
		metadata.setContentLength(contentBytes.length);
		metadata.setHeader("Expect", expected);
			
		svc.putObject(new PutObjectRequest(bucket_name, key, is, metadata));	
	}
	
	@Test
	public void testObjectCreateBadMd5InvalidShort() {
		
		try {
			
			String bucket_name = utils.getBucketName(prefix);
			String key = "key1";
			String content = "echo lima golf";
			String md5 = "WJyYWNhZGFicmE=";
				
			svc.createBucket(new CreateBucketRequest(bucket_name));

			byte[] contentBytes = content.getBytes(StringUtils.UTF8);
			InputStream is = new ByteArrayInputStream(contentBytes);
				
			ObjectMetadata metadata = new ObjectMetadata();
			metadata.setContentLength(contentBytes.length);
			metadata.setHeader("Content-MD5", md5);
				
			svc.putObject(new PutObjectRequest(bucket_name, key, is, metadata));
			
		} catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode(), "InvalidDigest");
		}
		
	}
	
	@Test
	public void testObjectCreateBadMd5Bad() {
		
		try {
			
			String bucket_name = utils.getBucketName(prefix);
			String key = "key1";
			String content = "echo lima golf";
			String md5 = "rL0Y20zC+Fzt72VPzMSk2A==";
				
			svc.createBucket(new CreateBucketRequest(bucket_name));

			byte[] contentBytes = content.getBytes(StringUtils.UTF8);
			InputStream is = new ByteArrayInputStream(contentBytes);
				
			ObjectMetadata metadata = new ObjectMetadata();
			metadata.setContentLength(contentBytes.length);
			metadata.setHeader("Content-MD5", md5);
				
			svc.putObject(new PutObjectRequest(bucket_name, key, is, metadata));
			
		} catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode(), "BadDigest");
		}
		
	}
	
	@Test
	public void testObjectCreateBadMd5Unreadable() {
		
		try {
			
			String bucket_name = utils.getBucketName(prefix);
			String key = "key1";
			String content = "echo lima golf";
			String md5 = "\\x07";
				
			svc.createBucket(new CreateBucketRequest(bucket_name));

			byte[] contentBytes = content.getBytes(StringUtils.UTF8);
			InputStream is = new ByteArrayInputStream(contentBytes);
				
			ObjectMetadata metadata = new ObjectMetadata();
			metadata.setContentLength(contentBytes.length);
			metadata.setHeader("Content-MD5", md5);
				
			svc.putObject(new PutObjectRequest(bucket_name, key, is, metadata));
			
		} catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode(), "AccessDenied");
		}
		
	}
	
	@Test
	public void testObjectCreateBadMd5None() {
		
		try {
			
			String bucket_name = utils.getBucketName(prefix);
			String key = "key1";
			String content = "echo lima golf";
			String md5 = "";
				
			svc.createBucket(new CreateBucketRequest(bucket_name));

			byte[] contentBytes = content.getBytes(StringUtils.UTF8);
			InputStream is = new ByteArrayInputStream(contentBytes);
				
			ObjectMetadata metadata = new ObjectMetadata();
			metadata.setContentLength(contentBytes.length);
			metadata.setHeader("Content-MD5", md5);
				
			svc.putObject(new PutObjectRequest(bucket_name, key, is, metadata));
			
		} catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode(), "InvalidDigest");
		}
		
	}
	
	
	
	@Test
	public void testEncryptedTransfer1b() {
		try {
			
			String arr[] = utils.EncryptionSseCustomerWrite(1);
			Assert.assertEquals(arr[0], arr[1]);
		} catch (IllegalArgumentException err) {
			String expected_error = "HTTPS must be used when sending customer encryption keys (SSE-C) to S3, in order to protect your encryption keys.";
			AssertJUnit.assertEquals(err.getMessage(), expected_error);
		}
	}
	
	@Test
	public void testEncryptedTransfer1kb() {
		
		try {
			
			String arr[] = utils.EncryptionSseCustomerWrite(1024);
			Assert.assertEquals(arr[0], arr[1]);
		} catch (IllegalArgumentException err) {
			String expected_error = "HTTPS must be used when sending customer encryption keys (SSE-C) to S3, in order to protect your encryption keys.";
			AssertJUnit.assertEquals(err.getMessage(), expected_error);
		}
	}
	
	@Test
	public void testEncryptedTransfer1MB() {
		
		try {
			
			String arr[] = utils.EncryptionSseCustomerWrite(1024*1024);
			Assert.assertEquals(arr[0], arr[1]);
		} catch (IllegalArgumentException err) {
			String expected_error = "HTTPS must be used when sending customer encryption keys (SSE-C) to S3, in order to protect your encryption keys.";
			AssertJUnit.assertEquals(err.getMessage(), expected_error);
		}
	}
	
	@Test
	//@Description("Do not declare SSE-C but provide key and MD5. operation successfull, no encryption")
	public void testEncryptionKeyNoSSEC() {
		
		try {
			
			String bucket_name = utils.getBucketName(prefix);
			String key ="key1";
			String data = utils.repeat("testcontent", 100);
			
			svc.createBucket(bucket_name);	
			
			PutObjectRequest putRequest = new PutObjectRequest(bucket_name, key, data);
			ObjectMetadata objectMetadata = new ObjectMetadata();
			//objectMetadata.setContentLength(data.length());
			objectMetadata.setHeader("x-amz-server-side-encryption-customer-key", "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=");
			objectMetadata.setHeader("x-amz-server-side-encryption-customer-key-md5", "DWygnHRtgiJ77HCm+1rvHw==");
			putRequest.setMetadata(objectMetadata);
			svc.putObject(putRequest);
			
			String rdata = svc.getObjectAsString(bucket_name, key);
			Assert.assertEquals(rdata, data);
			
		} catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode(), "XAmzContentSHA256Mismatch");
		}
		
		
	}
	
	@Test
	//@Description("declare SSE-C but do not provide key. operation fails")
	public void testEncryptionKeySSECNoKey() {
		
		try {
			
			String bucket_name = utils.getBucketName(prefix);
			String key ="key1";
			String data = utils.repeat("testcontent", 100);
			
			svc.createBucket(bucket_name);	
			
			PutObjectRequest putRequest = new PutObjectRequest(bucket_name, key, data);
			ObjectMetadata objectMetadata = new ObjectMetadata();
			//objectMetadata.setContentLength(data.length());
			objectMetadata.setHeader("x-amz-server-side-encryption-customer-algorithm", "AES256");
			putRequest.setMetadata(objectMetadata);
			svc.putObject(putRequest);
			
			try {
				
				String rdata = svc.getObjectAsString(bucket_name, key);
			} catch (AmazonServiceException err) {
				AssertJUnit.assertEquals(err.getErrorCode().isEmpty(), false);
			}
		} catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode(), "InvalidRequest");
		}
	}
	
	@Test
	//@Description("write encrypted with SSE-C, but dont provide MD5. operation fails")
	public void testEncryptionKeySSECNoMd5() {
		
		try {
			
			String bucket_name = utils.getBucketName(prefix);
			String key ="key1";
			String data = utils.repeat("testcontent", 100);
			
			svc.createBucket(bucket_name);	
			
			PutObjectRequest putRequest = new PutObjectRequest(bucket_name, key, data);
			ObjectMetadata objectMetadata = new ObjectMetadata();
			//objectMetadata.setContentLength(data.length());
			objectMetadata.setHeader("x-amz-server-side-encryption-customer-algorithm", "AES256");
			objectMetadata.setHeader("x-amz-server-side-encryption-customer-key", "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=");
			putRequest.setMetadata(objectMetadata);
			svc.putObject(putRequest);
			
			try {
				
				String rdata = svc.getObjectAsString(bucket_name, key);
			} catch (AmazonServiceException err) {
				AssertJUnit.assertEquals(err.getErrorCode().isEmpty(), false);
			}
		} catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode(), "InvalidDigest");
		}
	}
	
	@Test
	//@Description("write encrypted with SSE-C, but md5 is bad. operation fails")
	public void testEncryptionKeySSECInvalidMd5() {
		
		try {
			
			String bucket_name = utils.getBucketName(prefix);
			String key ="key1";
			String data = utils.repeat("testcontent", 100);
			
			svc.createBucket(bucket_name);	
			
			PutObjectRequest putRequest = new PutObjectRequest(bucket_name, key, data);
			ObjectMetadata objectMetadata = new ObjectMetadata();
			//objectMetadata.setContentLength(data.length());
			objectMetadata.setHeader("x-amz-server-side-encryption-customer-algorithm", "AES256");
			objectMetadata.setHeader("x-amz-server-side-encryption-customer-key", "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=");
			objectMetadata.setHeader("x-amz-server-side-encryption-customer-key-md5", "AAAAAAAAAAAAAAAAAAAAAA==");
			putRequest.setMetadata(objectMetadata);
			svc.putObject(putRequest);
			
			try {
				
				String rdata = svc.getObjectAsString(bucket_name, key);
			} catch (AmazonServiceException err) {
				AssertJUnit.assertEquals(err.getErrorCode().isEmpty(), false);
			}
		} catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode(), "InvalidDigest");
		}
	}
	
	@Test
	//@Description("Test SSE-KMS encrypted transfer 1 byte.success")
	public void testSSEKMSTransfer1b() {
		try {
			
			String arr[] = utils.EncryptionSseKMSCustomerWrite(1, "");
			Assert.assertEquals(arr[0], arr[1]);
		} catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode(), "XAmzContentSHA256Mismatch");
		}
	}
	
	@Test
	//@Description("Test SSE-KMS encrypted transfer 1KB.success")
	public void testSSEKMSTransfer1Kb() {
		try {
			
			String arr[] = utils.EncryptionSseKMSCustomerWrite(1024, "");
			Assert.assertEquals(arr[0], arr[1]);
		} catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode(), "XAmzContentSHA256Mismatch");
		}
	}
	
	@Test
	//@Description("Test SSE-KMS encrypted transfer 1MB.success")
	public void testSSEKMSTransfer1MB() {
		try {
			
			String arr[] = utils.EncryptionSseKMSCustomerWrite(1024*1024, "");
			Assert.assertEquals(arr[0], arr[1]);
		} catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode(), "XAmzContentSHA256Mismatch");
		}
	}
	
	@Test
	//@Description("Test SSE-KMS encrypted transfer 13 bytes")
	public void testSSEKMSTransfer13B() {
		try {
			
			String arr[] = utils.EncryptionSseKMSCustomerWrite(13, "");
			Assert.assertEquals(arr[0], arr[1]);
		} catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode(), "XAmzContentSHA256Mismatch");
		}
	}
	
	@Test
	//@Description("Test SSE-KMS encrypted transfer 13 bytes, sucess")
	public void testSSEKMSPresent() {
		try {
			
			String bucket_name = utils.getBucketName(prefix);
			String key ="key1";
			String data = utils.repeat("testcontent", 100);
			String keyId = "testkey-1";
			
			svc.createBucket(bucket_name);	
			
			PutObjectRequest putRequest = new PutObjectRequest(bucket_name, key, data);
			ObjectMetadata objectMetadata = new ObjectMetadata();
			//objectMetadata.setContentLength(data.length());
			objectMetadata.setHeader("x-amz-server-side-encryption", "aws:kms");
			objectMetadata.setHeader("x-amz-server-side-encryption-aws-kms-key-id", keyId );
			putRequest.setMetadata(objectMetadata);
			svc.putObject(putRequest);
			
			String rdata = svc.getObjectAsString(bucket_name, key);
			Assert.assertEquals(rdata, data);
		} catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode(), "XAmzContentSHA256Mismatch");
		}
	}
	
	@Test
	//@Description("declare SSE-KMS but do not provide key_id, operation fails")
	public void testSSEKMSNoKey() {
		try {
			
			String bucket_name = utils.getBucketName(prefix);
			String key ="key1";
			String data = utils.repeat("testcontent", 100);
			
			svc.createBucket(bucket_name);	
			
			PutObjectRequest putRequest = new PutObjectRequest(bucket_name, key, data);
			ObjectMetadata objectMetadata = new ObjectMetadata();
			//objectMetadata.setContentLength(data.length());
			objectMetadata.setHeader("x-amz-server-side-encryption", "aws:kms");
			putRequest.setMetadata(objectMetadata);
			svc.putObject(putRequest);
			
			try {
				
				String rdata = svc.getObjectAsString(bucket_name, key);
			} catch (AmazonServiceException err) {
				AssertJUnit.assertEquals(err.getErrorCode().isEmpty(), false);
			}
		} catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode(), "InvalidAccessKeyId");
		}
	}
	
	@Test
	//@Description("Do not declare SSE-KMS but provide key_id, operation sucessful no encryption")
	public void testSSEKMSNotDeclared() {
		try {
			
			String bucket_name = utils.getBucketName(prefix);
			String key ="key1";
			String data = utils.repeat("testcontent", 100);
			String keyId = "testkey-1";
			
			svc.createBucket(bucket_name);	
			
			PutObjectRequest putRequest = new PutObjectRequest(bucket_name, key, data);
			ObjectMetadata objectMetadata = new ObjectMetadata();
			//objectMetadata.setContentLength(data.length());
			objectMetadata.setHeader("x-amz-server-side-encryption-aws-kms-key-id", keyId );
			putRequest.setMetadata(objectMetadata);
			svc.putObject(putRequest);
			
			String rdata = svc.getObjectAsString(bucket_name, key);
			Assert.assertEquals(rdata, data);
		} catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode(), "XAmzContentSHA256Mismatch");
		}
	}
	
	@Test
	//@Description("Test SSE-KMS encrypted transfer 1 byte.success")
	public void testSSEKMSBarbTransfer1b() {
		try {
			
			utils.checkKeyId();
			String arr[] = utils.EncryptionSseKMSCustomerWrite(1, utils.prop.getProperty("kmskeyid"));
			Assert.assertEquals(arr[0], arr[1]);
			
		} catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode(), "InvalidArgument");
		}
	}
	
	@Test
	//@Description("Test SSE-KMS encrypted transfer 1KB.success")
	public void testSSEKMSBarbTransfer1Kb() {
		
		try {
			
			utils.checkKeyId();
			String arr[] = utils.EncryptionSseKMSCustomerWrite(1024, utils.prop.getProperty("kmskeyid"));
			Assert.assertEquals(arr[0], arr[1]);
			
		} catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode(), "InvalidArgument");
		}
	}
	
	@Test
	//@Description("Test SSE-KMS encrypted transfer 1MB.success")
	public void testSSEKMSBarbTransfer1MB() {
		try {
			
			utils.checkKeyId();
			String arr[] = utils.EncryptionSseKMSCustomerWrite(1024*1024, "testkey-1");
			Assert.assertEquals(arr[0], arr[1]);
			
		} catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode(), "XAmzContentSHA256Mismatch");
		}
	}
	
	@Test
	//@Description("Test SSE-KMS encrypted transfer 13 bytes")
	public void testSSEKMSBarbTransfer13B() {
		try {
			
			utils.checkKeyId();
			String arr[] = utils.EncryptionSseKMSCustomerWrite(13, "testkey-1");
			Assert.assertEquals(arr[0], arr[1]);
			
		} catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode(), "XAmzContentSHA256Mismatch");
		}
	}
	
	@Test
	public void testMultipartUploadMultipleSizesLLAPI() {
		
		String bucket_name = utils.getBucketName(prefix);
		String key = "key1";
		svc.createBucket(new CreateBucketRequest(bucket_name));
		
		String filePath = "./data/file.mpg";
		
		CompleteMultipartUploadRequest resp = utils.multipartUploadLLAPI(svc, bucket_name, key, 5 * 1024 * 1024, filePath);
		svc.completeMultipartUpload(resp);
		
		CompleteMultipartUploadRequest resp2 = utils.multipartUploadLLAPI(svc, bucket_name, key, 5 * 1024 * 1024 + 100 * 1024, filePath);
		svc.completeMultipartUpload(resp2);
		
		CompleteMultipartUploadRequest resp3 = utils.multipartUploadLLAPI(svc, bucket_name, key, 5 * 1024 * 1024 + 600 * 1024, filePath);
		svc.completeMultipartUpload(resp3);
		
		CompleteMultipartUploadRequest resp4 = utils.multipartUploadLLAPI(svc, bucket_name, key, 10 * 1024 * 1024 + 100 * 1024, filePath);
		svc.completeMultipartUpload(resp4);
		
		CompleteMultipartUploadRequest resp5 = utils.multipartUploadLLAPI(svc, bucket_name, key, 10 * 1024 * 1024 + 600 * 1024, filePath);
		svc.completeMultipartUpload(resp5);
		
		CompleteMultipartUploadRequest resp6 = utils.multipartUploadLLAPI(svc, bucket_name, key, 10 * 1024 * 1024, filePath);
		svc.completeMultipartUpload(resp6);
	}
	
	@Test
	public void testMultipartUploadEmptyLLAPI() {
		
		String bucket_name = utils.getBucketName(prefix);
		String key = "key1";
		svc.createBucket(new CreateBucketRequest(bucket_name));
		
		String filePath = "./data/file.mpg";
		long size = 0;
		
		try {
			
			CompleteMultipartUploadRequest resp = utils.multipartUploadLLAPI(svc, bucket_name, key, size, filePath);
			svc.completeMultipartUpload(resp);
		}catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode(), "XAmzContentSHA256Mismatch");
		}
		
	}
	
	@Test
	public void testMultipartUploadSmallLLAPI() {
		
		String bucket_name = utils.getBucketName(prefix);
		String key = "key1";
		svc.createBucket(new CreateBucketRequest(bucket_name));
		
		String filePath = "./data/file.mpg";
		long size = 5242880;
			
		CompleteMultipartUploadRequest resp = utils.multipartUploadLLAPI(svc, bucket_name, key, size, filePath);
		svc.completeMultipartUpload(resp);
		
	}
	
	@Test
	public void testMultipartUploadIncorrectEtagLLAPI() {
		
		String bucket_name = utils.getBucketName(prefix);
		String key = "key1";
		svc.createBucket(new CreateBucketRequest(bucket_name));
		
		String filePath = "./data/file.mpg";;
		long size = 5242880;
			
		List<PartETag> partETags = new ArrayList<PartETag>();

		InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(bucket_name, key);
		InitiateMultipartUploadResult initResponse = svc.initiateMultipartUpload(initRequest);

		File file = new File(filePath);
		long contentLength = file.length();
		long partSize = size;
		
		long filePosition = 0;
		for (int i = 1; filePosition < contentLength; i++) {
		    	
		   partSize = Math.min(partSize, (contentLength - filePosition));
		   UploadPartRequest uploadRequest = new UploadPartRequest()
				   .withBucketName(bucket_name).withKey(key)
		           .withUploadId(initResponse.getUploadId()).withPartNumber(i)
		           .withFileOffset(filePosition)
		           .withFile(file)
		           .withPartSize(partSize)
		           ;
		   svc.uploadPart(uploadRequest).setETag("ffffffffffffffffffffffffffffffff");
		   partETags.add((PartETag) svc.uploadPart(uploadRequest).getPartETag());

		   filePosition += partSize;
		}
		
		CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest(bucket_name, key, 
                initResponse.getUploadId(), 
                (List<com.amazonaws.services.s3.model.PartETag>) partETags);

		try {
			
			svc.completeMultipartUpload(compRequest);
		} catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode(), "InvalidPart");
		}
		
	}
	
	@Test
	public void testMultipartUploadIncorrectMissingPartLLAPI() {
		
		String bucket_name = utils.getBucketName(prefix);
		String key = "key1";
		svc.createBucket(new CreateBucketRequest(bucket_name));
		
		String filePath = "./data/file.mpg";
		long size = 5242880;
			
		List<PartETag> partETags = new ArrayList<PartETag>();

		InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(bucket_name, key);
		InitiateMultipartUploadResult initResponse = svc.initiateMultipartUpload(initRequest);

		File file = new File(filePath);
		long contentLength = file.length();
		long partSize = size;
		
		long filePosition = 0;
		for (int i = 1; filePosition < contentLength; i++) {
		    	
		   partSize = Math.min(partSize, (contentLength - filePosition));
		   UploadPartRequest uploadRequest = new UploadPartRequest()
				   .withBucketName(bucket_name).withKey(key)
		           .withUploadId(initResponse.getUploadId()).withPartNumber(i)
		           .withFileOffset(filePosition)
		           .withFile(file)
		           .withPartSize(partSize)
		           ;
		   svc.uploadPart(uploadRequest).setPartNumber(9999);
		   partETags.add((PartETag) svc.uploadPart(uploadRequest).getPartETag());

		   filePosition += partSize;
		}
		
		CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest(bucket_name, key, 
                initResponse.getUploadId(), 
                (List<com.amazonaws.services.s3.model.PartETag>) partETags);

		try {
			
			svc.completeMultipartUpload(compRequest);
		} catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode(), "InvalidPart");
		}
		
	}
	
	
	@Test
	public void testAbortMultipartUploadNotFoundLLAPI() {
		
		String bucket_name = utils.getBucketName(prefix);
		String key = "key1";
		svc.createBucket(new CreateBucketRequest(bucket_name));
		
		String filePath = "./data/file.mpg";
		long size = 5242880;
		
		try {
			
			svc.abortMultipartUpload( new AbortMultipartUploadRequest(bucket_name, key, "1"));
		} catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode(), "NoSuchUpload");
		}
		
	}
	
	@Test
	public void testAbortMultipartUploadLLAPI() {
		
		String bucket_name = utils.getBucketName(prefix);
		String key = "key1";
		svc.createBucket(new CreateBucketRequest(bucket_name));
		
		String filePath = "./data/file.mpg";
		long size = 5242880;
		
		CompleteMultipartUploadRequest resp = utils.multipartUploadLLAPI(svc, bucket_name, key, 5 * 1024 * 1024, filePath);
		svc.abortMultipartUpload( new AbortMultipartUploadRequest(bucket_name, key, resp.getUploadId()));
		
	}
	
	@Test
	public void testMultipartUploadOverwriteExistingObjectLLAPI() {
		
		String bucket_name = utils.getBucketName(prefix);
		String key = "key1";
		svc.createBucket(new CreateBucketRequest(bucket_name));
		
		String filePath = "./data/file.mpg";
		long size = 5242880;
		
		svc.putObject(bucket_name, key, "foo");
		
		CompleteMultipartUploadRequest resp = utils.multipartUploadLLAPI(svc, bucket_name, key, size, filePath);
		svc.completeMultipartUpload(resp);
		
		Assert.assertNotEquals(svc.getObjectAsString(bucket_name, key), "foo");
		
	}
	
	@Test
	public void testMultipartUploadFileTooSmallFileLLAPI() {
		
		String bucket_name = utils.getBucketName(prefix);
		String key = "key1";
		svc.createBucket(new CreateBucketRequest(bucket_name));
		
		String filePath = "./data/sample.txt";
		long size = 5242880;
			
		try {
			
			CompleteMultipartUploadRequest resp = utils.multipartUploadLLAPI(svc, bucket_name, key, size, filePath);
			svc.completeMultipartUpload(resp);
		}catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode(), "EntityTooSmall");
		}
		
	}
	
	@Test
	public void testMultipartCopyMultipleSizesLLAPI() {
		
		String src_bkt = utils.getBucketName(prefix);
		String dst_bkt = utils.getBucketName(prefix);
		String dir = "./data";
		String key = "key1";
		
		svc.createBucket(new CreateBucketRequest(src_bkt));
		svc.createBucket(new CreateBucketRequest(dst_bkt));
		
		String filePath = "./data/file.mpg";
		Upload upl = utils.UploadFileHLAPI(svc, src_bkt, key, filePath );
		Assert.assertEquals(upl.isDone(), true);
		
		
		CompleteMultipartUploadRequest resp = utils.multipartCopyLLAPI(svc, dst_bkt, key, src_bkt, key, 5 * 1024 * 1024);
		svc.completeMultipartUpload(resp);
		
		CompleteMultipartUploadRequest resp2 = utils.multipartCopyLLAPI(svc, dst_bkt, key, src_bkt, key, 5 * 1024 * 1024 + 100 * 1024);
		svc.completeMultipartUpload(resp2);
		
		CompleteMultipartUploadRequest resp3 = utils.multipartCopyLLAPI(svc, dst_bkt, key, src_bkt, key, 5 * 1024 * 1024 + 600 * 1024);
		svc.completeMultipartUpload(resp3);
		
		CompleteMultipartUploadRequest resp4 = utils.multipartCopyLLAPI(svc, dst_bkt, key, src_bkt, key, 10 * 1024 * 1024 + 100 * 1024);
		svc.completeMultipartUpload(resp4);
		
		CompleteMultipartUploadRequest resp5 = utils.multipartCopyLLAPI(svc, dst_bkt, key, src_bkt, key, 10 * 1024 * 1024 + 600 * 1024);
		svc.completeMultipartUpload(resp5);
		
		CompleteMultipartUploadRequest resp6 = utils.multipartCopyLLAPI(svc, dst_bkt, key, src_bkt, key, 10 * 1024 * 1024);
		svc.completeMultipartUpload(resp6);
		
	}
	
	
	@Test
	public void testUploadFileHLAPIBigFile() {
	
		String bucket_name = utils.getBucketName(prefix);
		String key = "key1";
		svc.createBucket(new CreateBucketRequest(bucket_name));
		
		String filePath = "./data/file.mpg";
		
		Upload upl = utils.UploadFileHLAPI(svc, bucket_name, key, filePath );
		
		Assert.assertEquals(upl.isDone(), true);
		
	}
	
	@Test
	public void testUploadFileHLAPISmallFile() {
		
		String bucket_name = utils.getBucketName(prefix);
		String key = "key1";
		svc.createBucket(new CreateBucketRequest(bucket_name));
		
		String filePath = "./data/sample.txt";
		
		Upload upl = utils.UploadFileHLAPI(svc, bucket_name, key, filePath );
		
		Assert.assertEquals(upl.isDone(), true);
		
	}
	
	@Test
	public void testUploadFileHLAPINonExistantBucket() {
		
		String bucket_name = utils.getBucketName(prefix);
		String key = "key1";
		
		String filePath = "./data/sample.txt";
		
		try {
			Upload upl = utils.UploadFileHLAPI(svc, bucket_name, key, filePath );
		} catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode(), "NoSuchBucket");
		}
		
	}
	
	@Test
	public void testMultipartUploadHLAPIA() throws AmazonServiceException, AmazonClientException, InterruptedException {
		
		String bucket_name = utils.getBucketName(prefix);
	
		svc.createBucket(new CreateBucketRequest(bucket_name));
		
		String dir = "./data";
		
		Transfer upl = utils.multipartUploadHLAPI(svc, bucket_name, null, dir);
		
		Assert.assertEquals(upl.isDone(), true);
		
	}
	
	@Test
	public void testMultipartUploadHLAPINonEXistantBucket() throws AmazonServiceException, AmazonClientException, InterruptedException {
		
		String bucket_name = utils.getBucketName(prefix);
		
		String dir = "./data";
		
		try {
			
			Transfer upl = utils.multipartUploadHLAPI(svc, bucket_name, null, dir);
			
		} catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode(), "NoSuchBucket");
		}
		
	}
	
	@Test
	public void testMultipartUploadWithPause() throws AmazonServiceException, AmazonClientException, InterruptedException, IOException {
		
		String bucket_name = utils.getBucketName(prefix);
		
		svc.createBucket(new CreateBucketRequest(bucket_name));
		
		String dir = "./data/file.mpg";
		String key = "key1";
		
		TransferManager tm = new TransferManager(svc);
		Upload myUpload = tm.upload(bucket_name, key, new File(dir));
		
		//pause upload
		long MB = 10;
		TransferProgress progress = myUpload.getProgress();
		while( progress.getBytesTransferred() < MB ) Thread.sleep(2000);
		boolean forceCancel = true;
		PauseResult<PersistableUpload> pauseResult = myUpload.tryPause(forceCancel);
		Assert.assertEquals(pauseResult.getPauseStatus().isPaused(), true);
		
		//persist PersistableUpload info to a file
		PersistableUpload persistableUpload = pauseResult.getInfoToResume();
		File f = new File("resume-upload");
		if( !f.exists() ) f.createNewFile();
		FileOutputStream fos = new FileOutputStream(f);
		persistableUpload.serialize(fos);
		fos.close(); 
		
		// Resume upload
		FileInputStream fis = new FileInputStream(new File("resume-upload"));
		PersistableUpload persistableUpload1 = PersistableTransfer.deserializeFrom(fis);
		tm.resumeUpload(persistableUpload1);
		fis.close();
		
	}
	
	@Test
	public void testMultipartCopyHLAPIAWS4() throws AmazonServiceException, AmazonClientException, InterruptedException {
		
		String src_bkt = utils.getBucketName(prefix);
		String dst_bkt = utils.getBucketName(prefix);
		String dir = "./data";
		String key = "key1";
		
		svc.createBucket(new CreateBucketRequest(src_bkt));
		svc.createBucket(new CreateBucketRequest(dst_bkt));
		
		String filePath = "./data/sample.txt";
		Upload upl = utils.UploadFileHLAPI(svc, src_bkt, key, filePath );
		Assert.assertEquals(upl.isDone(), true);
		
		Copy cpy = utils.multipartCopyHLAPI(svc, dst_bkt, key, src_bkt, key );
		Assert.assertEquals(cpy.isDone(), true);
	}
	
	@Test
	public void testMultipartCopyNoDSTBucketHLAPIA() throws AmazonServiceException, AmazonClientException, InterruptedException {
		
		String src_bkt = utils.getBucketName(prefix);
		String dst_bkt = utils.getBucketName(prefix);
		String dir = "./data";
		String key = "key1";
		
		svc.createBucket(new CreateBucketRequest(src_bkt));
		
		String filePath = "./data/sample.txt";
		Upload upl = utils.UploadFileHLAPI(svc, src_bkt, key, filePath );
		Assert.assertEquals(upl.isDone(), true);
		
		try {
			
			Copy cpy = utils.multipartCopyHLAPI(svc, dst_bkt, key, src_bkt, key );
		} catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode(), "NoSuchBucket");
		}
		
	}
	
	@Test
	public void testMultipartCopyNoSRCBucketHLAPIA() throws AmazonServiceException, AmazonClientException, InterruptedException {
		
		String src_bkt = utils.getBucketName(prefix);
		String dst_bkt = utils.getBucketName(prefix);
		String dir = "./data";
		String key = "key1";
		
		svc.createBucket(new CreateBucketRequest(dst_bkt));
		
		try {
			
			Copy cpy = utils.multipartCopyHLAPI(svc, dst_bkt, key, src_bkt, key );
		} catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode(), "404 Not Found");
		}
	}
	
	@Test
	public void testMultipartCopyNoSRCKeyHLAPIAWS4() throws AmazonServiceException, AmazonClientException, InterruptedException {
		
		String src_bkt = utils.getBucketName(prefix);
		String dst_bkt = utils.getBucketName(prefix);
		String dir = "./data";
		String key = "key1";
		
		svc.createBucket(new CreateBucketRequest(src_bkt));
		svc.createBucket(new CreateBucketRequest(dst_bkt));
		
		try {
			
			Copy cpy = utils.multipartCopyHLAPI(svc, dst_bkt, key, src_bkt, key );
		} catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode(), "404 Not Found");
		}
	}
	
	@Test
	public void testDownloadHLAPI() throws AmazonServiceException, AmazonClientException, InterruptedException {
		
		String bucket_name = utils.getBucketName(prefix);
		svc.createBucket(new CreateBucketRequest(bucket_name));
		String key = "key1";
		
		String filePath = "./data/sample.txt";
		Upload upl = utils.UploadFileHLAPI(svc, bucket_name, key, filePath );
		Assert.assertEquals(upl.isDone(), true);
		
		Download download = utils.downloadHLAPI(svc, bucket_name, key, new File(filePath));
		Assert.assertEquals(download.isDone(), true);
		
	}
	
	@Test
	public void testDownloadNoBucketHLAPIA() throws AmazonServiceException, AmazonClientException, InterruptedException {
		
		String bucket_name = utils.getBucketName(prefix);
		String key = "key1";
		String filePath = "./data/sample.txt";
		
		try {
			
			Download download = utils.downloadHLAPI(svc, bucket_name, key, new File(filePath));
		} catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode(), "404 Not Found");
		}
		
	}
	
	@Test
	public void testDownloadNoKeyHLAPI() throws AmazonServiceException, AmazonClientException, InterruptedException {
		
		String bucket_name = utils.getBucketName(prefix);
		svc.createBucket(new CreateBucketRequest(bucket_name));
		String key = "key1";
		
		String filePath = "./data/sample.txt";
		
		try {
			
			Download download = utils.downloadHLAPI(svc, bucket_name, key, new File(filePath));
		} catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode(), "404 Not Found");
		}
		
	}
	
	@Test
	public void testMultipartDownloadHLAPI() throws AmazonServiceException, AmazonClientException, InterruptedException {
		
		String bucket_name = utils.getBucketName(prefix);
		svc.createBucket(new CreateBucketRequest(bucket_name));
		String key = "key1";
		String dstDir = "./downloads";
		
		String filePath = "./data/file.mpg";
		Upload upl = utils.UploadFileHLAPI(svc, bucket_name, key, filePath );
		Assert.assertEquals(upl.isDone(), true);
		
		MultipleFileDownload download = utils.multipartDownloadHLAPI(svc, bucket_name, key, new File(dstDir));
		Assert.assertEquals(download.isDone(), true);
		
		File f = new File("./downloads/file.mpg");
		Assert.assertEquals(f.exists(), true);
	}
	
	@Test
	public void testDownloadWithPauseHLAPI() throws AmazonServiceException, AmazonClientException, InterruptedException, IOException {
		
		String bucket_name = utils.getBucketName(prefix);
		svc.createBucket(new CreateBucketRequest(bucket_name));
		String key = "key1";
		String filePath = "./data/file.mpg";
		
		TransferManager tm = new TransferManager(svc);
		
		Upload upl = utils.UploadFileHLAPI(svc, bucket_name, key, filePath );
		Assert.assertEquals(upl.isDone(), true);
		
		Download myDownload = tm.download(bucket_name, key, new File(filePath));

		long MB = 20;
		TransferProgress progress = myDownload.getProgress();
		while( progress.getBytesTransferred() < MB ) Thread.sleep(2000);

		// Pause the download and create file to store download info
		PersistableDownload persistableDownload = myDownload.pause();
		File f = new File("resume-download");
		if( !f.exists() ) f.createNewFile();
		FileOutputStream fos = new FileOutputStream(f);
		persistableDownload.serialize(fos);
		fos.close();
		
		//resume download
		FileInputStream fis = new FileInputStream(new File("resume-download"));
		PersistableDownload persistDownload = PersistableTransfer.deserializeFrom(fis);
		tm.resumeDownload(persistDownload);

		fis.close();
		
		
	}
	
	@Test
	public void testMultipartDownloadNoBucketHLAPIAWS4() throws AmazonServiceException, AmazonClientException, InterruptedException {
		
		String bucket_name = utils.getBucketName(prefix);
		
		String key = "key1";
		String dstDir = "./downloads";
		
		try {
			
			MultipleFileDownload download = utils.multipartDownloadHLAPI(svc, bucket_name, key, new File(dstDir));
		} catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode(), "NoSuchBucket");
		}
	}
	
	@Test
	public void testMultipartDownloadNoKeyHLAPI() throws AmazonServiceException, AmazonClientException, InterruptedException {
		
		String bucket_name = utils.getBucketName(prefix);
		svc.createBucket(new CreateBucketRequest(bucket_name));
		String key = "key1";
		String dstDir = "./downloads";
		
		
		try {
			
			MultipleFileDownload download = utils.multipartDownloadHLAPI(svc, bucket_name, key, new File(dstDir));
		} catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode(), "404 Not Found");
		}
	}
	
	@Test
	public void testUploadFileListHLAPI() throws AmazonServiceException, AmazonClientException, InterruptedException {
		
		String bucket_name = utils.getBucketName(prefix);
		svc.createBucket(new CreateBucketRequest(bucket_name));
		String key = "key1";
		String dstDir = "./downloads";
		
		MultipleFileUpload upl= utils.UploadFileListHLAPI(svc, bucket_name, key);
		Assert.assertEquals(upl.isDone(), true);
		
		ObjectListing listing = svc.listObjects( bucket_name);
		List<S3ObjectSummary> summaries = listing.getObjectSummaries();
		while (listing.isTruncated()) {
		   listing = svc.listNextBatchOfObjects (listing);
		   summaries.addAll (listing.getObjectSummaries());
		}
		Assert.assertEquals(summaries.size(), 2);
		
	}
	
	@Test
	public void testUploadFileListNoBucketHLAPI() throws AmazonServiceException, AmazonClientException, InterruptedException {
		
		String bucket_name = utils.getBucketName(prefix);
		svc.createBucket(new CreateBucketRequest(bucket_name));
		String key = "key1";
		String dstDir = "./downloads";
		
		try {
			
			MultipleFileUpload upl= utils.UploadFileListHLAPI(svc, bucket_name, key);
		} catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode(), "NoSuchBucket");
		}
		
	}
	
}
