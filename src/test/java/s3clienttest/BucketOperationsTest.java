package s3clienttest;

import awss3.S3Client;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Base64;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;


public class BucketOperationsTest {
    private static final String accessKey = "QUtJQUpSR0sySFc0RlFXN0JOSVE=";
    private static final String secretKey = "dGxRZVUxQkJXNm1XcWpLT3JwOXlCT2tValhTejlXTHFZOVIwamppNg==";
    private static BasicAWSCredentials credentials =
            new BasicAWSCredentials(
                    new String(Base64.getDecoder().decode(accessKey)),
                    new String(Base64.getDecoder().decode(secretKey))
            );

    @Test
    public void canMoveAnObjectIntoBucket() {
        String bucketName = "zach-java-s3-bucket";
        S3Client s3 = new S3Client(bucketName, credentials, Regions.US_WEST_1);

        s3.safeCreateBucket(bucketName);

        String target = "Documents/testfile.txt";
        File resource = new File(getClass().getResource("/testfile.txt").getFile());
        s3.copyFileTo(target, resource);
    }

    @Test
    public void canMoveADirectoryIntoBucket() throws URISyntaxException {
        String bucketName = "zach-java-s3-bucket";
        S3Client s3 = new S3Client(bucketName, credentials, Regions.US_WEST_1);

        s3.copyDirectoryTo(new File(getClass().getResource("/testDir").toURI()));
        assertTrue(
                s3.listObjects().getObjectSummaries().size() > 1,
                "OOPS!"
        );
    }

    @Test
    public void canMoveALocalDirectoryIntoADirectoryInABucket() throws URISyntaxException {
        String bucketName = "zach-java-s3-bucket";
        S3Client s3 = new S3Client(bucketName, credentials, Regions.US_WEST_1);
        String directoryToTransfer = "testDir";

        s3.copyDirectoryTo(
                String.format("Documents/%s", directoryToTransfer),
                new File(getClass().getResource("/" + directoryToTransfer).toURI())
        );

        assertTrue(
                s3.listObjects().getObjectSummaries().size() > 1,
                "Unexpected number of objects in the bucket!"
        );
    }

    @Test
    public void canListObjectsInBucket() {
        String bucketName = "zach-java-s3-bucket";
        S3Client s3 = new S3Client(bucketName, credentials, Regions.US_WEST_1);

        assertTrue(
                s3.listObjects().getObjectSummaries().size() > 0,
                "Documents/testfile.txt"
                );
    }

    // This test ensures first that a testfile is in a Documents folder.  The documents folder was created by
    // zfolwick with a file inserted into it which this IAM role doesn't have access to. this ensures that the
    // Documents folder can't be deleted.
    @Test
    public void canDeleteAnObjectFromBucket() {
        String bucketName = "zach-java-s3-bucket";
        S3Client s3 = new S3Client(bucketName, credentials, Regions.US_WEST_1);
        String destination = "Documents/testfile.txt";
        File resource = new File(getClass().getResource("/testfile.txt").getFile());
        s3.copyFileTo(destination, resource);

        s3.deleteObject(destination);

        assertEquals(
                "Documents/",
                s3.listObjects().getObjectSummaries().get(0).getKey(),
                "Unexpected result when trying to delete object from s3 bucket."
        );
    }

    @Test
    public void canRetrieveObjectsFromBucketWithoutDestroyingObject() throws IOException {
        String bucketName = "zach-java-s3-bucket";
        S3Client s3 = new S3Client(bucketName, credentials, Regions.US_WEST_1);
        String sourceFile = "Documents/testfile.txt";
        File resource = new File(getClass().getResource("/testfile.txt").getFile());
        // set up bucket into good test state
        s3.copyFileTo(sourceFile, resource);

        // perform testable action
        String target = "testCreatedFile.txt";
        File createdFile = s3.copyObjectToFile(sourceFile, target);

        // verify file exists locally and contents are correct
        assertTrue(
                createdFile.exists(),
                "Expected file to exist"
        );
        String expectedContent = "This is a test file.";
        BufferedReader br = new BufferedReader(new FileReader(createdFile));
        String line;
        while ( (line = br.readLine() ) != null ) {
            assertEquals(
                    expectedContent,
                    line,
                    "Unexpected content in test file brought in from AWS s3 bucket!"
            );
        }
        br.close();

        // verify file still exists in s3
        assertTrue(
                s3.doesObjectExist(sourceFile),
                "Expected files to still exist in s3 bucket."
        );

        // clean up test artifacts
        assertTrue(
                createdFile.delete(),
                "Expected delete to succeed"
        );
        assertFalse(
                createdFile.exists(),
                "Expected test artifacts to be deleted."
        );
    }

}