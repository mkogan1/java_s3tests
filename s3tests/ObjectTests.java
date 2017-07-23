
import java.io.ByteArrayInputStream;
import java.io.InputStream;

import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.StringUtils;

public class ObjectTests {
	
	private static S3 utils =  new S3();
	AmazonS3 svc = utils.getCLI();
	
	String prefix = utils.getPrefix();

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
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
	public void testMultiObjectDelete() {
		
		String bucket_name = utils.getBucketName(prefix);
		svc.createBucket(new CreateBucketRequest(bucket_name));
		
		svc.putObject(bucket_name, "key1", "echo");
		
		svc.deleteObjects(new DeleteObjectsRequest(bucket_name));
		ObjectListing list = svc.listObjects(new ListObjectsRequest()
				.withBucketName(bucket_name));
		AssertJUnit.assertEquals(list.getObjectSummaries().isEmpty(), true);
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
	public void testObjectWriteReadUpdateReadDelete() {
		
		String bucket_name = utils.getBucketName(prefix);
		String key = "key1";
		svc.createBucket(new CreateBucketRequest(bucket_name));
		
		svc.putObject(bucket_name, key, "echo");
		S3Object obj = svc.getObject(bucket_name, key);
		
		svc.putObject(bucket_name, key, "lima");
		S3Object obj2 = svc.getObject(bucket_name, key);
		
		AssertJUnit.assertNotSame(obj.getObjectContent(), obj2.getObjectContent());
		
		svc.deleteObject(bucket_name, key);
		ObjectListing list = svc.listObjects(new ListObjectsRequest()
				.withBucketName(bucket_name));
		AssertJUnit.assertEquals(list.getObjectSummaries().isEmpty(), true);
		
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
	public void TestObjectCreateBadMd5Empty() {
		
		try {
			
			String bucket_name = utils.getBucketName(prefix);
			String key = "key1";
			String content = "echo lima golf";
			String md5 = " ";
				
			svc.createBucket(new CreateBucketRequest(bucket_name));

			byte[] contentBytes = content.getBytes(StringUtils.UTF8);
			InputStream is = new ByteArrayInputStream(contentBytes);
				
			ObjectMetadata metadata = new ObjectMetadata();
			metadata.setContentLength(contentBytes.length);
			metadata.setHeader("Content-MD5", md5);
				
			svc.putObject(new PutObjectRequest(bucket_name, key, is, metadata));
			
		} catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode(), "400 Bad Requ]est");
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
	
	//...........................................SSE and KMS..............................................................
	
	@Test
	public void testEncryptedTransfer1b() {
		
		String arr[] = utils.EncryptionSseCustomerWrite(1);
		AssertJUnit.assertEquals(arr[0], arr[1]);
	}
	
	@Test
	public void testEncryptedTransfer1kb() {
		
		String arr[] = utils.EncryptionSseCustomerWrite(1024);
		AssertJUnit.assertEquals(arr[0], arr[1]);
	}
	
	@Test
	public void testEncryptedTransfer1MB() {
		
		String arr[] = utils.EncryptionSseCustomerWrite(1024*1024);
		AssertJUnit.assertEquals(arr[0], arr[1]);
	}
	
	@Test
	public void testEncryptedTransfer13b() {
		
		String arr[] = utils.EncryptionSseCustomerWrite(13);
		AssertJUnit.assertEquals(arr[0], arr[1]);
	}
	
}
