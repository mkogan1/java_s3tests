import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;

import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.StringUtils;

public class ObjectTests {
	
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
	public void testObjectListDelimiterPercentage() {
		
		String [] keys = {"b%ar", "b%az", "c%ab", "foo"};
		String delim = "%";
		java.util.List<String> expected_prefixes = Arrays.asList("b%", "c%");
		java.util.List<String> expected_keys = Arrays.asList("foo");
		
		com.amazonaws.services.s3.model.Bucket bucket = utils.createKeys(keys);
		final ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucket.getName()).withDelimiter(delim);
        ListObjectsV2Result result = svc.listObjectsV2(req);
        
        Assert.assertEquals(result.getDelimiter(), delim);
        
        Assert.assertEquals(result.getCommonPrefixes(), expected_prefixes);
        
		Object[] k = new Object[] {};
		ArrayList<Object> list = new ArrayList<Object>(Arrays.asList(k));
		for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
			list.add(objectSummary.getKey());
        }
		Assert.assertEquals(list, expected_keys);
		
	}
	
	@Test
	public void testObjectListDelimiterWhitespace() {
		
		String [] keys = {"bar", "baz", "cab", "foo"};
		String delim = " ";
        try {
        	
    		com.amazonaws.services.s3.model.Bucket bucket = utils.createKeys(keys);
    		final ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucket.getName()).withDelimiter(delim);
            ListObjectsV2Result result = svc.listObjectsV2(req);
            
		} catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode(), "SignatureDoesNotMatch");
		}
		
	}
	
	@Test
	public void testObjectListDelimiterDot() {
		
		String [] keys = {"b.ar", "b.az", "c.ab", "foo"};
		String delim = ".";
		java.util.List<String> expected_prefixes = Arrays.asList("b.", "c.");
		java.util.List<String> expected_keys = Arrays.asList("foo");
		
		com.amazonaws.services.s3.model.Bucket bucket = utils.createKeys(keys);
		final ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucket.getName()).withDelimiter(delim);
        ListObjectsV2Result result = svc.listObjectsV2(req);
        
        Assert.assertEquals(result.getDelimiter(), delim);
        
        Assert.assertEquals(result.getCommonPrefixes(), expected_prefixes);
        
		Object[] k = new Object[] {};
		ArrayList<Object> list = new ArrayList<Object>(Arrays.asList(k));
		for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
			list.add(objectSummary.getKey());
        }
		Assert.assertEquals(list, expected_keys);
		
	}
	
	@Test
	public void testObjectListDelimiterUnreadable() {
		
		String [] keys = {"bar", "baz", "cab", "foo"};
		String delim = "\\x0a";
        try {
        	
    		com.amazonaws.services.s3.model.Bucket bucket = utils.createKeys(keys);
    		final ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucket.getName()).withDelimiter(delim);
            ListObjectsV2Result result = svc.listObjectsV2(req);
            
		} catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode(), "InvalidArgument");
		}
		
	}
	
	@Test
	public void testObjectListDelimiterNotExist() {
		
		String [] keys = {"bar", "baz", "cab", "foo"};
		String delim = "/";
		java.util.List<String> expected_keys = Arrays.asList("bar", "baz", "cab", "foo");
		
		com.amazonaws.services.s3.model.Bucket bucket = utils.createKeys(keys);
		final ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucket.getName()).withDelimiter(delim);
        ListObjectsV2Result result = svc.listObjectsV2(req);
        
        Assert.assertEquals(result.getDelimiter(), delim);
        
        Assert.assertEquals((result.getCommonPrefixes()).isEmpty(), true);
        
		Object[] k = new Object[] {};
		ArrayList<Object> list = new ArrayList<Object>(Arrays.asList(k));
		for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
			list.add(objectSummary.getKey());
        }
		Assert.assertEquals(list, expected_keys);
		
	}
	
	@Test
	public void testObjectListPrefixBasic() {
		
		String [] keys = {"foo/bar", "foo/baz", "quux"};
		String prefix = "foo/";
		java.util.List<String> expected_keys = Arrays.asList("foo/bar", "foo/baz");
		
		com.amazonaws.services.s3.model.Bucket bucket = utils.createKeys(keys);
		final ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucket.getName()).withPrefix(prefix);
        ListObjectsV2Result result = svc.listObjectsV2(req);
        
        Assert.assertEquals(result.getPrefix(), prefix);
        
        Assert.assertEquals((result.getCommonPrefixes()).isEmpty(), true);
        
		Object[] k = new Object[] {};
		ArrayList<Object> list = new ArrayList<Object>(Arrays.asList(k));
		for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
			list.add(objectSummary.getKey());
        }
		Assert.assertEquals(list, expected_keys);
		
	}
	
	@Test
	public void testObjectListPrefixAlt() {
		
		String [] keys = {"bar", "baz", "foo"};
		String prefix = "ba";
		java.util.List<String> expected_keys = Arrays.asList("bar", "baz");
		
		com.amazonaws.services.s3.model.Bucket bucket = utils.createKeys(keys);
		final ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucket.getName()).withPrefix(prefix);
        ListObjectsV2Result result = svc.listObjectsV2(req);
        
        Assert.assertEquals(result.getPrefix(), prefix);
        
        Assert.assertEquals((result.getCommonPrefixes()).isEmpty(), true);
        
		Object[] k = new Object[] {};
		ArrayList<Object> list = new ArrayList<Object>(Arrays.asList(k));
		for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
			list.add(objectSummary.getKey());
        }
		Assert.assertEquals(list, expected_keys);
		
	}
	
	@Test
	public void testObjectListPrefixEmpty() {
		
		String [] keys = {"foo/bar", "foo/baz", "quux"};
		String prefix = "";
		java.util.List<String> expected_keys = Arrays.asList("foo/bar", "foo/baz", "quux");
		
		com.amazonaws.services.s3.model.Bucket bucket = utils.createKeys(keys);
		final ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucket.getName()).withPrefix(prefix);
        ListObjectsV2Result result = svc.listObjectsV2(req);
        
        Assert.assertEquals((result.getCommonPrefixes()).isEmpty(), true);
        
		Object[] k = new Object[] {};
		ArrayList<Object> list = new ArrayList<Object>(Arrays.asList(k));
		for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
			list.add(objectSummary.getKey());
        }
		Assert.assertEquals(list, expected_keys);
		
	}
	
	@Test
	public void testObjectListPrefixNone() {
		
		String [] keys = {"foo/bar", "foo/baz", "quux"};
		String prefix = "";
		java.util.List<String> expected_keys = Arrays.asList("foo/bar", "foo/baz", "quux");
		
		com.amazonaws.services.s3.model.Bucket bucket = utils.createKeys(keys);
		final ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucket.getName()).withPrefix(prefix);
        ListObjectsV2Result result = svc.listObjectsV2(req);
        
        Assert.assertEquals((result.getCommonPrefixes()).isEmpty(), true);
        
		Object[] k = new Object[] {};
		ArrayList<Object> list = new ArrayList<Object>(Arrays.asList(k));
		for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
			list.add(objectSummary.getKey());
        }
		Assert.assertEquals(list, expected_keys);
		
	}
	
	@Test
	public void testObjectListPrefixNotExist() {
		
		String [] keys = {"foo/bar", "foo/baz", "quux"};
		String prefix = "d";
		java.util.List<String> expected_keys = Arrays.asList();
		
		com.amazonaws.services.s3.model.Bucket bucket = utils.createKeys(keys);
		final ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucket.getName()).withPrefix(prefix);
        ListObjectsV2Result result = svc.listObjectsV2(req);
        
        Assert.assertEquals(result.getPrefix(), prefix);
        
        Assert.assertEquals((result.getCommonPrefixes()).isEmpty(), true);
        
		Object[] k = new Object[] {};
		ArrayList<Object> list = new ArrayList<Object>(Arrays.asList(k));
		for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
			list.add(objectSummary.getKey());
        }
		Assert.assertEquals(list, expected_keys);
		
	}
	
	@Test
	public void testObjectListPrefixUnreadable() {
		
		String [] keys = {"foo/bar", "foo/baz", "quux"};
		String prefix = "\\x0a";
		java.util.List<String> expected_keys = Arrays.asList();
		
		com.amazonaws.services.s3.model.Bucket bucket = utils.createKeys(keys);
		final ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucket.getName()).withPrefix(prefix);
        ListObjectsV2Result result = svc.listObjectsV2(req);
        
        Assert.assertEquals(result.getPrefix(), prefix);
        
        Assert.assertEquals((result.getCommonPrefixes()).isEmpty(), true);
        
		Object[] k = new Object[] {};
		ArrayList<Object> list = new ArrayList<Object>(Arrays.asList(k));
		for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
			list.add(objectSummary.getKey());
        }
		Assert.assertEquals(list, expected_keys);
		
	}
	
	@Test
	public void testObjectListPrefixDelimiterBasic() {
		
		String [] keys = {"foo/bar", "foo/baz/xyzzy", "quux/thud", "asdf"};
		String prefix = "foo/";
		String delim = "/";
		java.util.List<String> expected_keys = Arrays.asList("foo/bar");
		java.util.List<String> expected_prefixes = Arrays.asList( "foo/baz/");
		
		com.amazonaws.services.s3.model.Bucket bucket = utils.createKeys(keys);
		final ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucket.getName()).withPrefix(prefix).withDelimiter(delim);
        ListObjectsV2Result result = svc.listObjectsV2(req);
        
        Assert.assertEquals(result.getPrefix(), prefix);
        Assert.assertEquals(result.getDelimiter(), delim);
        
        Assert.assertEquals((result.getCommonPrefixes()), expected_prefixes);
        
		Object[] k = new Object[] {};
		ArrayList<Object> list = new ArrayList<Object>(Arrays.asList(k));
		for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
			list.add(objectSummary.getKey());
        }
		Assert.assertEquals(list, expected_keys);
		
	}
	
	@Test
	public void testObjectListPrefixDelimiterAlt() {
		
		String [] keys = {"bar", "bazar", "cab", "foo"};
		String prefix = "ba";
		String delim = "a";
		java.util.List<String> expected_keys = Arrays.asList("bar");
		java.util.List<String> expected_prefixes = Arrays.asList( "baza");
		
		com.amazonaws.services.s3.model.Bucket bucket = utils.createKeys(keys);
		final ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucket.getName()).withPrefix(prefix).withDelimiter(delim);
        ListObjectsV2Result result = svc.listObjectsV2(req);
        
        Assert.assertEquals(result.getPrefix(), prefix);
        Assert.assertEquals(result.getDelimiter(), delim);
        
        Assert.assertEquals((result.getCommonPrefixes()), expected_prefixes);
        
		Object[] k = new Object[] {};
		ArrayList<Object> list = new ArrayList<Object>(Arrays.asList(k));
		for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
			list.add(objectSummary.getKey());
        }
		Assert.assertEquals(list, expected_keys);
		
	}
	
	@Test
	public void testObjectListPrefixDelimiterPrefixNotExist() {
		
		String [] keys = {"b/a/r", "b/a/c", "b/a/g", "g"};
		String prefix = "d";
		String delim = "/";
		
		com.amazonaws.services.s3.model.Bucket bucket = utils.createKeys(keys);
		final ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucket.getName()).withPrefix(prefix).withDelimiter(delim);
        ListObjectsV2Result result = svc.listObjectsV2(req);
        
        Assert.assertEquals(result.getPrefix(), prefix);
        Assert.assertEquals(result.getDelimiter(), delim);
        
        Assert.assertEquals((result.getCommonPrefixes()).isEmpty(), true);
        
		Object[] k = new Object[] {};
		ArrayList<Object> list = new ArrayList<Object>(Arrays.asList(k));
		for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
			list.add(objectSummary.getKey());
        }
		Assert.assertEquals(list.isEmpty(), true);
		
	}
	
	@Test
	public void testObjectListPrefixDelimiterDelimiterNotExist() {
		
		String [] keys = {"b/a/c", "b/a/g", "b/a/r", "g"};
		String prefix = "b";
		String delim = "z";
		java.util.List<String> expected_keys = Arrays.asList("b/a/c", "b/a/g", "b/a/r");
		
		com.amazonaws.services.s3.model.Bucket bucket = utils.createKeys(keys);
		final ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucket.getName()).withPrefix(prefix).withDelimiter(delim);
        ListObjectsV2Result result = svc.listObjectsV2(req);
        
        Assert.assertEquals(result.getPrefix(), prefix);
        Assert.assertEquals(result.getDelimiter(), delim);
        
        Assert.assertEquals((result.getCommonPrefixes()).isEmpty(), true);
        
		Object[] k = new Object[] {};
		ArrayList<Object> list = new ArrayList<Object>(Arrays.asList(k));
		for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
			list.add(objectSummary.getKey());
        }
		Assert.assertEquals(list, expected_keys);
		
	}
	
	@Test
	public void testObjectListPrefixDelimiterPrefixDelimiterNotExist() {
		
		String [] keys = {"b/a/c", "b/a/g", "b/a/r", "g"};
		String prefix = "y";
		String delim = "z";
		
		com.amazonaws.services.s3.model.Bucket bucket = utils.createKeys(keys);
		final ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucket.getName()).withPrefix(prefix).withDelimiter(delim);
        ListObjectsV2Result result = svc.listObjectsV2(req);
        
        Assert.assertEquals(result.getPrefix(), prefix);
        Assert.assertEquals(result.getDelimiter(), delim);
        
        Assert.assertEquals((result.getCommonPrefixes()).isEmpty(), true);
        
		Object[] k = new Object[] {};
		ArrayList<Object> list = new ArrayList<Object>(Arrays.asList(k));
		for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
			list.add(objectSummary.getKey());
        }
		Assert.assertEquals(list.isEmpty(), true);
		
	}
	
	@Test
	public void testObjectListMaxkeysOne() {
		
		String [] keys = {"bar", "baz", "foo", "quxx"};
		java.util.List<String> expected_keys  = Arrays.asList("bar");
		
		int max_keys = 1;
		
		com.amazonaws.services.s3.model.Bucket bucket = utils.createKeys(keys);
		final ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucket.getName()).withMaxKeys(max_keys);
        ListObjectsV2Result result = svc.listObjectsV2(req);
        
        Assert.assertEquals(result.getMaxKeys(), max_keys);
        Assert.assertEquals(result.isTruncated(), true);
       
		Object[] k = new Object[] {};
		ArrayList<Object> list = new ArrayList<Object>(Arrays.asList(k));
		for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
			list.add(objectSummary.getKey());
        }
		Assert.assertEquals(list, expected_keys);
		
	}
	
	@Test
	public void testObjectListMaxkeysZero() {
		
		String [] keys = {"bar", "baz", "foo", "quxx"};
		
		int max_keys = 0;
		
		com.amazonaws.services.s3.model.Bucket bucket = utils.createKeys(keys);
		final ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucket.getName()).withMaxKeys(max_keys);
        ListObjectsV2Result result = svc.listObjectsV2(req);
        
        Assert.assertEquals(result.getMaxKeys(), max_keys);
        Assert.assertEquals(result.isTruncated(), false);
       
		Object[] k = new Object[] {};
		ArrayList<Object> list = new ArrayList<Object>(Arrays.asList(k));
		for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
			list.add(objectSummary.getKey());
        }
		Assert.assertEquals(list.isEmpty(), true);
		
	}
	
	@Test
	public void testObjectListMaxkeysNone() {
		
		String [] keys = {"bar", "baz", "foo", "quxx"};
		java.util.List<String> expected_keys = Arrays.asList("bar", "baz", "foo", "quxx");
		
		com.amazonaws.services.s3.model.Bucket bucket = utils.createKeys(keys);
		final ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucket.getName());
        ListObjectsV2Result result = svc.listObjectsV2(req);
        
        Assert.assertEquals(result.getMaxKeys(), 1000);
        Assert.assertEquals(result.isTruncated(), false);
       
		Object[] k = new Object[] {};
		ArrayList<Object> list = new ArrayList<Object>(Arrays.asList(k));
		for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
			list.add(objectSummary.getKey());
        }
		Assert.assertEquals(list, expected_keys);
		
	}
	
	@Test
	public void testObjectListMarkerEmpty() {
		
		String [] keys = {"bar", "baz", "foo", "quxx"};
		String marker = " ";
		
		com.amazonaws.services.s3.model.Bucket bucket = utils.createKeys(keys);
		
		try {
			
			ListObjectsRequest req = new ListObjectsRequest();
			req.setBucketName(bucket.getName());
			req.setMarker(marker);
			
			ObjectListing result = svc.listObjects(req);
			
		} catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode(), "SignatureDoesNotMatch");
		}
	}
	
	@Test
	public void testObjectListMarkerUnreadable() {
		
		String [] keys = {"bar", "baz", "foo", "quxx"};
		java.util.List<String> expected_keys = Arrays.asList("bar", "baz", "foo", "quxx");
		String marker = "\\x0a";
		
		com.amazonaws.services.s3.model.Bucket bucket = utils.createKeys(keys);
		ListObjectsRequest req = new ListObjectsRequest();
		req.setBucketName(bucket.getName());
		req.setMarker(marker);
		ObjectListing result = svc.listObjects(req); 

        Assert.assertEquals(result.getMarker(), marker);
        Assert.assertEquals(result.isTruncated(), false);
        
        
        Object[] k = new Object[] {};
		ArrayList<Object> list = new ArrayList<Object>(Arrays.asList(k));
		for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
			list.add(objectSummary.getKey());
        }
		Assert.assertEquals(list, expected_keys);
		
	}
	
	@Test
	public void testObjectListMarkerNotInList() {
		
		String [] keys = {"bar", "baz", "foo", "quxx"};
		java.util.List<String> expected_keys = Arrays.asList("foo", "quxx");
		String marker = "blah";
		
		com.amazonaws.services.s3.model.Bucket bucket = utils.createKeys(keys);
		ListObjectsRequest req = new ListObjectsRequest();
		req.setBucketName(bucket.getName());
		req.setMarker(marker);
		ObjectListing result = svc.listObjects(req); 

        Assert.assertEquals(result.getMarker(), marker);
        
        Object[] k = new Object[] {};
		ArrayList<Object> list = new ArrayList<Object>(Arrays.asList(k));
		for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
			list.add(objectSummary.getKey());
        }
		Assert.assertEquals(list, expected_keys);
		
	}
	
	@Test
	public void testObjectListMarkerAfterList() {
		
		String [] keys = {"bar", "baz", "foo", "quxx"};
		String marker = "zzz";
		
		com.amazonaws.services.s3.model.Bucket bucket = utils.createKeys(keys);
		ListObjectsRequest req = new ListObjectsRequest();
		req.setBucketName(bucket.getName());
		req.setMarker(marker);
		ObjectListing result = svc.listObjects(req); 

        Assert.assertEquals(result.getMarker(), marker);
        Assert.assertEquals(result.isTruncated(), false);
        
        
        Object[] k = new Object[] {};
		ArrayList<Object> list = new ArrayList<Object>(Arrays.asList(k));
		for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
			list.add(objectSummary.getKey());
        }
		Assert.assertEquals(list.isEmpty(), true);
		
	}
	
	@Test
	public void testObjectListMarkerBeforeList() {
		
		String [] keys = {"bar", "baz", "foo", "quxx"};
		java.util.List<String> expected_keys = Arrays.asList("bar", "baz", "foo", "quxx");
		String marker = "aaa";
		
		com.amazonaws.services.s3.model.Bucket bucket = utils.createKeys(keys);
		ListObjectsRequest req = new ListObjectsRequest();
		req.setBucketName(bucket.getName());
		req.setMarker(marker);
		ObjectListing result = svc.listObjects(req); 

        Assert.assertEquals(result.getMarker(), marker);
        Assert.assertEquals(result.isTruncated(), false);
        
        
        Object[] k = new Object[] {};
		ArrayList<Object> list = new ArrayList<Object>(Arrays.asList(k));
		for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
			list.add(objectSummary.getKey());
        }
		Assert.assertEquals(list, expected_keys);
		
	}
	
	@Test
	public void testEncryptedTransfer1b() {
		
		utils.checkSSL();
		String arr[] = utils.EncryptionSseCustomerWrite(1);
		Assert.assertEquals(arr[0], arr[1]);
	}
	
	@Test
	public void testEncryptedTransfer1kb() {
		
		utils.checkSSL();
		String arr[] = utils.EncryptionSseCustomerWrite(1024);
		Assert.assertEquals(arr[0], arr[1]);
	}
	
	@Test
	public void testEncryptedTransfer1MB() {
		
		utils.checkSSL();
		String arr[] = utils.EncryptionSseCustomerWrite(1024*1024);
		Assert.assertEquals(arr[0], arr[1]);
	}
	
	@Test
	//@Description("Do not declare SSE-C but provide key and MD5. operation successfull, no encryption")
	public void testEncryptionKeyNoSSEC() {
		
		utils.checkSSL();
		String bucket_name = utils.getBucketName(prefix);
		String key ="key1";
		String data = utils.repeat("testcontent", 100);
		
		svc.createBucket(bucket_name);	
		
		PutObjectRequest putRequest = new PutObjectRequest(bucket_name, key, data);
		ObjectMetadata objectMetadata = new ObjectMetadata();
		objectMetadata.setContentLength(data.length());
		objectMetadata.setHeader("x-amz-server-side-encryption-customer-key", "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=");
		objectMetadata.setHeader("x-amz-server-side-encryption-customer-key-md5", "DWygnHRtgiJ77HCm+1rvHw==");
		putRequest.setMetadata(objectMetadata);
		svc.putObject(putRequest);
		
		String rdata = svc.getObjectAsString(bucket_name, key);
		Assert.assertEquals(rdata, data);
	}
	
	@Test
	//@Description("declare SSE-C but do not provide key. operation fails")
	public void testEncryptionKeySSECNoKey() {
		
		utils.checkSSL();
		String bucket_name = utils.getBucketName(prefix);
		String key ="key1";
		String data = utils.repeat("testcontent", 100);
		
		svc.createBucket(bucket_name);	
		
		PutObjectRequest putRequest = new PutObjectRequest(bucket_name, key, data);
		ObjectMetadata objectMetadata = new ObjectMetadata();
		objectMetadata.setContentLength(data.length());
		objectMetadata.setHeader("x-amz-server-side-encryption-customer-algorithm", "AES256");
		putRequest.setMetadata(objectMetadata);
		svc.putObject(putRequest);
		
		try {
			
			String rdata = svc.getObjectAsString(bucket_name, key);
		} catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode().isEmpty(), false);
		}
	}
	
	@Test
	//@Description("write encrypted with SSE-C, but dont provide MD5. operation fails")
	public void testEncryptionKeySSECNoMd5() {
		
		utils.checkSSL();
		String bucket_name = utils.getBucketName(prefix);
		String key ="key1";
		String data = utils.repeat("testcontent", 100);
		
		svc.createBucket(bucket_name);	
		
		PutObjectRequest putRequest = new PutObjectRequest(bucket_name, key, data);
		ObjectMetadata objectMetadata = new ObjectMetadata();
		objectMetadata.setContentLength(data.length());
		objectMetadata.setHeader("x-amz-server-side-encryption-customer-algorithm", "AES256");
		objectMetadata.setHeader("x-amz-server-side-encryption-customer-key", "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=");
		putRequest.setMetadata(objectMetadata);
		svc.putObject(putRequest);
		
		try {
			
			String rdata = svc.getObjectAsString(bucket_name, key);
		} catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode().isEmpty(), false);
		}
	}
	
	@Test
	//@Description("write encrypted with SSE-C, but md5 is bad. operation fails")
	public void testEncryptionKeySSECInvalidMd5() {
		
		utils.checkSSL();
		String bucket_name = utils.getBucketName(prefix);
		String key ="key1";
		String data = utils.repeat("testcontent", 100);
		
		svc.createBucket(bucket_name);	
		
		PutObjectRequest putRequest = new PutObjectRequest(bucket_name, key, data);
		ObjectMetadata objectMetadata = new ObjectMetadata();
		objectMetadata.setContentLength(data.length());
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
	}
	
	@Test
	//@Description("Test SSE-KMS encrypted transfer 1 byte.success")
	public void testSSEKMSTransfer1b() {
		
		utils.checkSSL();
		String arr[] = utils.EncryptionSseKMSCustomerWrite(1, "");
		Assert.assertEquals(arr[0], arr[1]);
	}
	
	@Test
	//@Description("Test SSE-KMS encrypted transfer 1KB.success")
	public void testSSEKMSTransfer1Kb() {
		
		utils.checkSSL();
		String arr[] = utils.EncryptionSseKMSCustomerWrite(1024, "");
		Assert.assertEquals(arr[0], arr[1]);
	}
	
	@Test
	//@Description("Test SSE-KMS encrypted transfer 1MB.success")
	public void testSSEKMSTransfer1MB() {
		
		utils.checkSSL();
		String arr[] = utils.EncryptionSseKMSCustomerWrite(1024*1024, "");
		Assert.assertEquals(arr[0], arr[1]);
	}
	
	@Test
	//@Description("Test SSE-KMS encrypted transfer 13 bytes")
	public void testSSEKMSTransfer13B() {
		
		utils.checkSSL();
		String arr[] = utils.EncryptionSseKMSCustomerWrite(13, "");
		Assert.assertEquals(arr[0], arr[1]);
	}
	
	@Test
	//@Description("Test SSE-KMS encrypted transfer 13 bytes, sucess")
	public void testSSEKMSPresent() {
		utils.checkSSL();
		String bucket_name = utils.getBucketName(prefix);
		String key ="key1";
		String data = utils.repeat("testcontent", 100);
		String keyId = "testkey-1";
		
		svc.createBucket(bucket_name);	
		
		PutObjectRequest putRequest = new PutObjectRequest(bucket_name, key, data);
		ObjectMetadata objectMetadata = new ObjectMetadata();
		objectMetadata.setContentLength(data.length());
		objectMetadata.setHeader("x-amz-server-side-encryption", "aws:kms");
		objectMetadata.setHeader("x-amz-server-side-encryption-aws-kms-key-id", keyId );
		putRequest.setMetadata(objectMetadata);
		svc.putObject(putRequest);
		
		String rdata = svc.getObjectAsString(bucket_name, key);
		Assert.assertEquals(rdata, data);
	}
	
	@Test
	//@Description("declare SSE-KMS but do not provide key_id, operation fails")
	public void testSSEKMSNoKey() {
		utils.checkSSL();
		String bucket_name = utils.getBucketName(prefix);
		String key ="key1";
		String data = utils.repeat("testcontent", 100);
		
		svc.createBucket(bucket_name);	
		
		PutObjectRequest putRequest = new PutObjectRequest(bucket_name, key, data);
		ObjectMetadata objectMetadata = new ObjectMetadata();
		objectMetadata.setContentLength(data.length());
		objectMetadata.setHeader("x-amz-server-side-encryption", "aws:kms");
		putRequest.setMetadata(objectMetadata);
		svc.putObject(putRequest);
		
		try {
			
			String rdata = svc.getObjectAsString(bucket_name, key);
		} catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode().isEmpty(), false);
		}
	}
	
	@Test
	//@Description("Do not declare SSE-KMS but provide key_id, operation sucessful no encryption")
	public void testSSEKMSNotDeclared() {
		utils.checkSSL();
		String bucket_name = utils.getBucketName(prefix);
		String key ="key1";
		String data = utils.repeat("testcontent", 100);
		String keyId = "testkey-1";
		
		svc.createBucket(bucket_name);	
		
		PutObjectRequest putRequest = new PutObjectRequest(bucket_name, key, data);
		ObjectMetadata objectMetadata = new ObjectMetadata();
		objectMetadata.setContentLength(data.length());
		objectMetadata.setHeader("x-amz-server-side-encryption-aws-kms-key-id", keyId );
		putRequest.setMetadata(objectMetadata);
		svc.putObject(putRequest);
		
		String rdata = svc.getObjectAsString(bucket_name, key);
		Assert.assertEquals(rdata, data);
	}
	
	@Test
	//@Description("Test SSE-KMS encrypted transfer 1 byte.success")
	public void testSSEKMSBarbTransfer1b() {
		
		utils.checkSSL();
		utils.checkKeyId();
		String arr[] = utils.EncryptionSseKMSCustomerWrite(1, utils.prop.getProperty("kmskeyid"));
		Assert.assertEquals(arr[0], arr[1]);
	}
	
	@Test
	//@Description("Test SSE-KMS encrypted transfer 1KB.success")
	public void testSSEKMSBarbTransfer1Kb() {
		
		utils.checkSSL();
		utils.checkKeyId();
		String arr[] = utils.EncryptionSseKMSCustomerWrite(1024, utils.prop.getProperty("kmskeyid"));
		Assert.assertEquals(arr[0], arr[1]);
	}
	
	@Test
	//@Description("Test SSE-KMS encrypted transfer 1MB.success")
	public void testSSEKMSBarbTransfer1MB() {
		
		utils.checkSSL();
		utils.checkKeyId();
		String arr[] = utils.EncryptionSseKMSCustomerWrite(1024*1024, utils.prop.getProperty("kmskeyid"));
		Assert.assertEquals(arr[0], arr[1]);
	}
	
	@Test
	//@Description("Test SSE-KMS encrypted transfer 13 bytes")
	public void testSSEKMSBarbTransfer13B() {
		
		utils.checkSSL();
		utils.checkKeyId();
		String arr[] = utils.EncryptionSseKMSCustomerWrite(13, utils.prop.getProperty("kmskeyid"));
		Assert.assertEquals(arr[0], arr[1]);
	}
	
	//.......................................Get Object in Range....................................................
			@Test
			public void testRangedRequestEmptyObject() {
				
				String bucket_name = utils.getBucketName(prefix);
				String key = "key1";
				String content = " ";
				
				svc.createBucket(new CreateBucketRequest(bucket_name));
				svc.putObject(bucket_name, key, content);
				
				try {
					
		            GetObjectRequest request = new GetObjectRequest(bucket_name, key);
		            request.withRange(40, 50);
		            S3Object s3Object = svc.getObject(request);
					
				} catch (AmazonServiceException err) {
					AssertJUnit.assertEquals(err.getErrorCode(), "InvalidRange");
				}
					
					
			}	
			
			@Test
			public void testRangedRequestInvalidRange() {
				
				String bucket_name = utils.getBucketName(prefix);
				String key = "key1";
				String content = "testcontent";
				
				svc.createBucket(new CreateBucketRequest(bucket_name));
				svc.putObject(bucket_name, key, content);
				
				try {
					
		            GetObjectRequest request = new GetObjectRequest(bucket_name, key);
		            request.withRange(40, 50);
		            S3Object s3Object = svc.getObject(request);
					
				} catch (AmazonServiceException err) {
					AssertJUnit.assertEquals(err.getErrorCode(), "InvalidRange");
				}
					
					
			}
			
			@Test
			public void testRangedReturnTrailingBytesResponseCode() throws IOException {
				
				String bucket_name = utils.getBucketName(prefix);
				String key = "key1";
				String content = "testcontent";
				
				svc.createBucket(new CreateBucketRequest(bucket_name));
				svc.putObject(bucket_name, key, content);
				
				GetObjectRequest request = new GetObjectRequest(bucket_name, key);
	            request.withRange(4, 10);
	            S3Object obj = svc.getObject(request);
	            
	            BufferedReader reader = new BufferedReader(new InputStreamReader(obj.getObjectContent()));
	            while (true) {
	            	String line = reader.readLine();
	                if (line == null) break;
	                String str = content.substring(4);
	                Assert.assertEquals(line, str);
	            }
		        	
			}
			
			@Test
			public void testRangedSkipLeadingBytesResponseCode() throws IOException {
				
				String bucket_name = utils.getBucketName(prefix);
				String key = "key1";
				String content = "testcontent";
				
				svc.createBucket(new CreateBucketRequest(bucket_name));
				svc.putObject(bucket_name, key, content);
				
				GetObjectRequest request = new GetObjectRequest(bucket_name, key);
	            request.withRange(4, 10);
	            S3Object obj = svc.getObject(request);
	            
	            BufferedReader reader = new BufferedReader(new InputStreamReader(obj.getObjectContent()));
	            while (true) {
	            	String line = reader.readLine();
	                if (line == null) break;
	                String str = content.substring(4);
	                Assert.assertEquals(line, str);
	            }	
			}
			
			@Test
			public void testRangedrequestResponseCode() throws IOException {
				
				String bucket_name = utils.getBucketName(prefix);
				String key = "key1";
				String content = "testcontent";
				
				svc.createBucket(new CreateBucketRequest(bucket_name));
				svc.putObject(bucket_name, key, content);
				
				GetObjectRequest request = new GetObjectRequest(bucket_name, key);
	            request.withRange(4, 7);
	            S3Object obj = svc.getObject(request);
	            
	            BufferedReader reader = new BufferedReader(new InputStreamReader(obj.getObjectContent()));
	            while (true) {
	            	String line = reader.readLine();
	                if (line == null) break;
	                String str = content.substring(4,8);
	                Assert.assertEquals(line, str);
	            }	
					
			}
	
}
