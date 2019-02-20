package s3clienttest;

import awss3.S3Client;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.UUID;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class S3ClientTest {

    @Test
    public void canCreateNewS3Client() {
        String bucketName = "zach-java-s3-bucket";
        S3Client client = new S3Client(bucketName);

        assertNotNull(
                client,
                "Expected client to be not null."
        );
    }

    @Test
    public void canCreateABucket() {
        String bucketName = "zach-java-s3-bucket";
        S3Client client = new S3Client(bucketName);

        String actual = client.safeCreateBucket(bucketName).getBucketName();
        assertEquals(
                bucketName,
                actual,
                "Unexpected bucket name"
        );
    }

    @Test
    public void canDeleteABucket() {
        String bucketName = "zach-java-safe-to-delete";
        S3Client client = new S3Client(bucketName);
        client.safeCreateBucket(bucketName);
        assertTrue(
                client.safeDeleteBucket(bucketName),
                "Unexpected bucket deletion issue"
        );
    }

    @Test
    public void canMoveObjectsIntoBucket() throws IOException, URISyntaxException {
        String bucketName = "zach-java-s3-bucket";
        S3Client s3 = new S3Client(bucketName);

        s3.safeCreateBucket(bucketName);

        String target = "Documents/testfile.txt";
        File resource = new File(getClass().getResource("/testfile.txt").getFile());
        s3.copyFileToBucket(target, resource);
    }

    @Test
    public void canListObjectsInBucket() {
        String bucketName = "zach-java-s3-bucket";
        S3Client s3 = new S3Client(bucketName);

        assertEquals(
                "Documents/",
                s3.listObjects().getObjectSummaries().get(0).getKey()
        );
    }

    // This test ensures first that a testfile is in a Documents folder.  The documents folder was created by
    // zfolwick with a file inserted into it which this IAM role doesn't have access to. this ensures that the
    // Documents folder can't be deleted.
    @Test
    public void canDeleteObjectsFromBucket() {
        String bucketName = "zach-java-s3-bucket";
        S3Client s3 = new S3Client(bucketName);
        String destination = "Documents/testfile.txt";
        File resource = new File(getClass().getResource("/testfile.txt").getFile());
        s3.copyFileToBucket(destination, resource);

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
        S3Client s3 = new S3Client(bucketName);
        String sourceFile = "Documents/testfile.txt";
        File resource = new File(getClass().getResource("/testfile.txt").getFile());
        s3.copyFileToBucket(sourceFile, resource);

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

    @Test
    public void twoClientsCanExistAtTheSameTime() {
        String b1 = "zach-bucket-1" + UUID.randomUUID();
        String b2 = "zach-bucket-2" + UUID.randomUUID();
        S3Client c1 = new S3Client(b1);
        S3Client c2 = new S3Client(b2);
        assertEquals(
                b1,
                c1.getBucketName(),
                "Unexpected bucket name"
        );
        assertEquals(
                b2,
                c2.getBucketName(),
                "Unexpected bucket name"
        );
    }
}