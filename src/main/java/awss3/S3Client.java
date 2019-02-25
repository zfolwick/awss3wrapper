package awss3;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A class to handle CRUD operations for AWS s3 clients, holds the credentials, etc.  Typically, users will be
 * operating within a single bucket, so we will hold a global reference to the bucket.  If users wish to perform
 * operations on a different bucket, they can set a new bucket, or create a new instance of a client.
 *
 */
public class S3Client {
    private Regions clientRegion = Regions.US_WEST_2;
    private final AmazonS3 client;
    private String bucketName;

    public S3Client(String bucketName) {
            client = AmazonS3ClientBuilder.standard()
                    .withCredentials( DefaultAWSCredentialsProviderChain.getInstance())
                    .withRegion(this.clientRegion)
                    .build();

        setBucket(bucketName);
        if ( !client.doesBucketExist(getBucketName())) {
            print("Bucket needs creation before additional methods can be called.");
        }
    }

    /**
     * Most basic form of authentication.  This will likely never be used, as it is unsecure and terrible security.
     *
     * @param bucketName
     * @param credentials
     */
    public S3Client(String bucketName, BasicAWSCredentials credentials) {
        client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion(this.clientRegion)
                .build();

        setBucket(bucketName);
        if ( !client.doesBucketExist(getBucketName())) {
            print("Bucket needs creation before additional methods can be called.");
        }
    }

    public S3Client(String bucketName, BasicAWSCredentials credentials, Regions region) {
        this.clientRegion = region;
        client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion(this.clientRegion)
                .build();

        setBucket(bucketName);
        if ( !client.doesBucketExist(getBucketName())) {
            print("Bucket needs creation before additional methods can be called.");
        }
    }
    /**
     * Returns the stored reference name.
     *
     * @return
     */
    public String getBucketName() {
        return this.bucketName;
    }

    /**
     * Sets the bucket name to be acted upon.
     *
     * @param value
     */
    public void setBucket(String value) {
        this.bucketName = value;
    }

    /**
     * Exposing the underlying client for more advanced, low-level operations users may wish to perform.
     *
     * @return
     */
    public AmazonS3 getClient() {
        return client;
    }

    /**
     * Deletes the bucket with the given screen name.
     *
     * @param bucketName
     * @return
     */
    public boolean safeDeleteBucket(String bucketName) {
        if (client.doesBucketExist(bucketName)) {
            client.deleteBucket(bucketName);
            setBucket(null);
            return true;
        } else {
            // TODO: log
            print("Bucket " + bucketName + " does not exist.");
            // set to null if not already.
            if (getBucketName() != null) {
                setBucket(null);
            }
        }
        return false;
    }

    /**
     * safely creates a bucket with the given screen name.  If a bucket with that name already exists, it returns
     * that bucket.
     *
     * @param bucketName
     * @return
     */
    public S3Client safeCreateBucket(String bucketName) {
        if (client.doesBucketExist(bucketName)) {
            // TODO: log
            print("Bucket already exists.  Returning bucket: " + bucketName);

            List<Bucket> returnBucket = client.listBuckets().stream()
                    .filter(bucket -> bucket.getName().equals(bucketName))
                    .collect(Collectors.toList());

            assertTrue(returnBucket.size() > 0,
                    "Expected at least one bucket!");
            assertTrue(returnBucket.size() == 1,
                    "Unexpected bucket list size!");
            assertTrue(returnBucket.get(0).getName().equals(bucketName),
                    "Unexpected bucket name!");

            setBucket(returnBucket.get(0).getName());
            return this;

        }

        setBucket(client.createBucket(bucketName).getName());
        return this;
    }

    /**
     * Copies a file to a specified target directory.
     *
     * @param targetDirectory
     * @param resource
     */
    public void copyFileTo(String targetDirectory, File resource) {
        client.putObject(getBucketName(), targetDirectory, resource);
    }

    /**
     * Creates a directory in the root of the s3 bucket, and uploads all the files and subfolders.
     *
     * @param localDirectory - the
     *
     */
    public void copyDirectoryTo(File localDirectory) {
        copyDirectoryTo(localDirectory.getName(), localDirectory);
    }

    /**
     * Copies a directory to a target directory.
     * Ex. Usage:
     *
     * s3.copyDirectoryTo(
     *                 "Documents/" + directoryToTransfer,
     *                 new File(getClass().getResource("/" + directoryToTransfer).toURI())
     *         );
     * @param targetDirectory
     * @param directoryToTransfer
     */
    public void copyDirectoryTo(String targetDirectory, File directoryToTransfer) {
        TransferManager xfer_mgr = TransferManagerBuilder.standard().withS3Client(client).build();

        assertTrue(
                directoryToTransfer.exists(),
                "Local directory does not exist.  Only valid directories may be " +
                        "uploaded to s3."
        );

        MultipleFileUpload xfer = xfer_mgr.uploadDirectory(
                getBucketName(),
                targetDirectory,
                directoryToTransfer,
                true
        );

        try {
            xfer.waitForCompletion();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Deletes an object from the s3 bucket.
     *
     * @param target
     */
    public void deleteObject(String target) {
        getClient().deleteObject(getBucketName(), target);
    }

    /**
     * Lists all the objects within a bucket.
     * @return
     */
    public ObjectListing listObjects() {
        return getClient().listObjects(getBucketName());
    }

    /**
     * Checks if an object exists or not.
     *
     * @param target
     * @return
     */
    public boolean doesObjectExist(String target) {
        return client.doesObjectExist(getBucketName(), target);
    }

    /**
     * Handles the accessing of objects within an s3 bucket and caches a local copy in a target directory.
     *
     * @param source
     * @param target
     * @throws IOException
     */
    public File copyObjectToFile(String source, String target) throws IOException {
        S3Object object = getClient().getObject(getBucketName(), source);
        S3ObjectInputStream inputStream = object.getObjectContent();

        File createdFile = new File(target);
        FileUtils.copyInputStreamToFile(inputStream, createdFile.getAbsoluteFile());
        if (createdFile.exists()) {
            return createdFile;
        } else {
            throw new FileNotFoundException("Expected " + createdFile.getAbsolutePath() + " to exist!");
        }
    }

    public void print(String value) {
        System.out.println(">>> " + value);
    }
}
