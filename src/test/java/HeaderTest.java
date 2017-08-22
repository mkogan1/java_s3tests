import java.io.BufferedReader;
import java.io.IOException;
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
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class HeaderTest {
	
	private static S3 utils =  new S3();
	AmazonS3 svc = utils.getCLI();
	String prefix = utils.getPrefix();
		
  @AfterMethod
	public  void tearDownAfterClass() throws Exception {
		
		utils.tearDown(svc);	
	}

	@BeforeMethod
	public void setUp() throws Exception {
	}
	
	@Test
	public void testObjectListDelimiterPercentage() {
		
		String [] keys = {"b%ar", "b%az", "c%ab", "foo"};
		String delim = "%";
		java.util.List<String> expected_prefixes = Arrays.asList("b%", "c%");
		java.util.List<String> expected_keys = Arrays.asList("foo");
		
		com.amazonaws.services.s3.model.Bucket bucket = utils.createKeys(svc, keys);
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
        	
    		com.amazonaws.services.s3.model.Bucket bucket = utils.createKeys(svc, keys);
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
		
		com.amazonaws.services.s3.model.Bucket bucket = utils.createKeys(svc, keys);
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
        	
    		com.amazonaws.services.s3.model.Bucket bucket = utils.createKeys(svc, keys);
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
		
		com.amazonaws.services.s3.model.Bucket bucket = utils.createKeys(svc, keys);
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
		
		com.amazonaws.services.s3.model.Bucket bucket = utils.createKeys(svc, keys);
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
		
		com.amazonaws.services.s3.model.Bucket bucket = utils.createKeys(svc, keys);
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
		
		com.amazonaws.services.s3.model.Bucket bucket = utils.createKeys(svc, keys);
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
		
		com.amazonaws.services.s3.model.Bucket bucket = utils.createKeys(svc, keys);
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
		
		com.amazonaws.services.s3.model.Bucket bucket = utils.createKeys(svc, keys);
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
		
		com.amazonaws.services.s3.model.Bucket bucket = utils.createKeys(svc, keys);
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
		
		com.amazonaws.services.s3.model.Bucket bucket = utils.createKeys(svc, keys);
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
		
		com.amazonaws.services.s3.model.Bucket bucket = utils.createKeys(svc, keys);
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
		
		com.amazonaws.services.s3.model.Bucket bucket = utils.createKeys(svc, keys);
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
		
		com.amazonaws.services.s3.model.Bucket bucket = utils.createKeys(svc, keys);
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
		
		com.amazonaws.services.s3.model.Bucket bucket = utils.createKeys(svc, keys);
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
	public void testObjectListMaxkeysNegative() {
		
		//passes..wonder blandar
		
		String [] keys = {"bar", "baz", "foo", "quxx"};
		
		com.amazonaws.services.s3.model.Bucket bucket = utils.createKeys(svc, keys);
		final ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucket.getName()).withMaxKeys(-1);
        ListObjectsV2Result result = svc.listObjectsV2(req);
        
        Assert.assertEquals(result.getMaxKeys(), -1);
        Assert.assertEquals(result.isTruncated(), true);
       
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
		
		com.amazonaws.services.s3.model.Bucket bucket = utils.createKeys(svc, keys);
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
		
		com.amazonaws.services.s3.model.Bucket bucket = utils.createKeys(svc, keys);
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
		
		com.amazonaws.services.s3.model.Bucket bucket = utils.createKeys(svc, keys);
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
		
		com.amazonaws.services.s3.model.Bucket bucket = utils.createKeys(svc, keys);
		
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
		
		com.amazonaws.services.s3.model.Bucket bucket = utils.createKeys(svc, keys);
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
		
		com.amazonaws.services.s3.model.Bucket bucket = utils.createKeys(svc, keys);
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
		
		com.amazonaws.services.s3.model.Bucket bucket = utils.createKeys(svc, keys);
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
		
		com.amazonaws.services.s3.model.Bucket bucket = utils.createKeys(svc, keys);
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
