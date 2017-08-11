import org.testng.AssertJUnit;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

public class BucketTest {
	
	//To Do ... provide singleton to these instances
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
	public void testBucketListEmpty() {
		
		String bucket_name = utils.getBucketName(prefix);
		svc.createBucket(new CreateBucketRequest(bucket_name));
		
		ObjectListing list = svc.listObjects(new ListObjectsRequest()
								.withBucketName(bucket_name));
		AssertJUnit.assertEquals(list.getObjectSummaries().isEmpty(), true);
	}
	
	@Test 
	public void testBucketDeleteNotExist() {
		
		String bucket_name = utils.getBucketName(prefix);
		AssertJUnit.assertEquals(svc.doesBucketExist(bucket_name), false);
		
		try {
			
			svc.deleteBucket(bucket_name);
		} catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode(), "NoSuchBucket");
		}
		
	}
	
	@Test 
	public void testBucketDeleteNonEmpty() {
		
		String bucket_name = utils.getBucketName(prefix);
		svc.createBucket(new CreateBucketRequest(bucket_name));
		
		svc.putObject(bucket_name, "key1", "echo");
		
		try {
			
			svc.deleteBucket(bucket_name);
		} catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode(), "BucketNotEmpty");
		}
		
	}
	
	@Test 
	public void testBucketCreateReadDelete() {
		
		String bucket_name = utils.getBucketName(prefix);
		svc.createBucket(new CreateBucketRequest(bucket_name));
		AssertJUnit.assertEquals(svc.doesBucketExist(bucket_name), true);
		
		svc.deleteBucket(bucket_name);
		AssertJUnit.assertEquals(svc.doesBucketExist(bucket_name), false);
		
	}
	
	@Test
	public void testBucketListDistinct() {
		
		String bucket1 = utils.getBucketName(prefix);
		String bucket2 = utils.getBucketName(prefix);
		
		svc.createBucket(new CreateBucketRequest(bucket1));
		svc.createBucket(new CreateBucketRequest(bucket2));
		
		svc.putObject(bucket1, "key1", "echo");
		
		ObjectListing list = svc.listObjects(new ListObjectsRequest()
				.withBucketName(bucket2));
		AssertJUnit.assertEquals(list.getObjectSummaries().isEmpty(), true);
	}
	
	@Test
	public void testBucketNotExist() {
		
		String bucket_name = utils.getBucketName(prefix);
		try {
			
			svc.getBucketAcl(bucket_name);
		} catch (AmazonServiceException err) {
			AssertJUnit.assertEquals(err.getErrorCode(), "NoSuchBucket");
		}
	}
	
	@Test
	//@Description("create w/expect 200, garbage but S3 succeeds!")
	public void testBucketCreateBadExpectMismatch() {
		
		String bucket_name = utils.getBucketName(prefix);
		
		CreateBucketRequest bktRequest = new CreateBucketRequest(bucket_name);
		bktRequest.putCustomRequestHeader("Expect", "200");
		 svc.createBucket(bktRequest);
	}
	
	@Test
	//@Description("create w/expect empty, garbage but S3 succeeds!")
	public void testBucketCreateBadExpectEmpty() {
		
		String bucket_name = utils.getBucketName(prefix);
		
		CreateBucketRequest bktRequest = new CreateBucketRequest(bucket_name);
		bktRequest.putCustomRequestHeader("Expect", "");
		svc.createBucket(bktRequest);
	}
	
	@Test
	//@Description("create w/expect empty, garbage but S3 succeeds!")
	public void testBucketCreateBadExpectUnreadable() {
		
		String bucket_name = utils.getBucketName(prefix);
		
		CreateBucketRequest bktRequest = new CreateBucketRequest(bucket_name);
		bktRequest.putCustomRequestHeader("Expect", "\\x07");
		svc.createBucket(bktRequest);
	}
	
}
