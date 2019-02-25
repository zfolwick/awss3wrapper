package s3clienttest;

import awss3.S3Client;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import java.util.Base64;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class BucketCreateDeleteTest {
    private static final String accessKey = "QUtJQUpSR0sySFc0RlFXN0JOSVE=";
    private static final String secretKey = "dGxRZVUxQkJXNm1XcWpLT3JwOXlCT2tValhTejlXTHFZOVIwamppNg==";
    private static BasicAWSCredentials credentials =
            new BasicAWSCredentials(
                    new String(Base64.getDecoder().decode(accessKey)),
                    new String(Base64.getDecoder().decode(secretKey))
            );

    @Test
    public void canCreateABucket() {
        String bucketName = "zach-java-s3-bucket";
        S3Client client = new S3Client(bucketName, credentials, Regions.US_WEST_1);

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
        S3Client client = new S3Client(bucketName, credentials, Regions.US_WEST_1);
        client.safeCreateBucket(bucketName);
        assertTrue(
                client.safeDeleteBucket(bucketName),
                "Unexpected bucket deletion issue"
        );
    }

}