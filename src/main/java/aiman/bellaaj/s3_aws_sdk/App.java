package aiman.bellaaj.s3_aws_sdk;

import java.io.File;
import java.util.List;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

/**
 * Hello world!
 *
 */
public class App {
	private static AmazonS3 s3;

	public static void main(String[] args) {
		final String USAGE = "\n" + "S3Client - create an S3 bucket\n\n" + "Usage: S3Client <bucketname>\n\n"
				+ "Where:\n" + "  bucketname - the name of the bucket to create.\n\n"
				+ "The bucket name must be unique, or an error will result.\n";

		/*
		 * if (args.length < 1) { System.out.println(USAGE); System.exit(1); }
		 */

		String bucket_name = "test-xa-bf-nor-ls";
		System.out.format("\nCreating S3 bucket: %s\n", bucket_name);

		// initialiser le service S3
		initS3();

		Bucket b = createBucket(bucket_name);
		if (b == null) {
			System.out.println("Error creating bucket!\n");
		} else {
			System.out.println("Done!\n");
		}
		System.out.printf("uploding file");
		uploadFile("test-xa", "home/vagrant/Downloads/xxx.pdf", "/home/vagrant/Downloads/pdf-test.pdf");
		System.out.printf("Done!");
	}

	public static void uploadFile(String bucketName, String fileObjKeyName, String fileName) {
		// Upload a file as a new object with ContentType and title specified.
		PutObjectRequest request = new PutObjectRequest(bucketName, fileObjKeyName, new File(fileName));
		ObjectMetadata metadata = new ObjectMetadata();
		metadata.setContentType("application/pdf");
		metadata.addUserMetadata("x-amz-meta-title", "someTitle");
		request.setMetadata(metadata);
		s3.putObject(request);
	}

	public static Bucket getBucket(String bucket_name) {
		return s3.listBuckets().stream().filter(b -> b.getName().equals(bucket_name)).findFirst().get();
	}

	private static void initS3() {
		// AWSCredentials credentials = new BasicAWSCredentials("minioadmin",
		// "minioadmin");
		AWSCredentials credentials = new BasicAWSCredentials("AKIAZOEAWRGFV7BR4LHR",
				"UoQvlUijl+Rr+yJlF9fEMuR/1jwQVhlUM07Rp0me");
		ClientConfiguration clientConfiguration = new ClientConfiguration();
		clientConfiguration.setSignerOverride("AWSS3V4SignerType");
		AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
		// builder.setForceGlobalBucketAccessEnabled(true);
		s3 = builder
				.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("s3.eu-west-3.amazonaws.com",
						Regions.EU_WEST_3.getName()))
				.withPathStyleAccessEnabled(true).withClientConfiguration(clientConfiguration)
				.withCredentials(new AWSStaticCredentialsProvider(credentials)).build();

	}

	public static Bucket createBucket(String bucket_name) {
		Bucket b = null;
		if (s3.doesBucketExistV2(bucket_name)) {
			System.out.format("Bucket %s already exists.\n", bucket_name);
			b = getBucket(bucket_name);
		} else {
			try {
				b = s3.createBucket(bucket_name);
			} catch (AmazonS3Exception e) {
				System.err.println(e.getErrorMessage());
			}
		}
		return b;
	}
}