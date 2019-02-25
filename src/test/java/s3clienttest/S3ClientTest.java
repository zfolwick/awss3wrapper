package s3clienttest;

import awss3.S3Client;
import com.amazonaws.auth.BasicAWSCredentials;
import java.util.Base64;
import java.util.UUID;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class S3ClientTest {
    public static final String bucketName = "qa-team-test-bucket";

    @Test
    public void canCreateNewS3ClientUsingIAMRole() {
        S3Client client = new S3Client(bucketName);

        assertNotNull(
                client.getClient(),
                "Expected client to be not null."
        );
    }

    @Test
    public void canCreateNewS3ClientUsingBasicCreds() {
        final String accessKey = "QUtJQUpSR0sySFc0RlFXN0JOSVE=";
        final String secretKey = "dGxRZVUxQkJXNm1XcWpLT3JwOXlCT2tValhTejlXTHFZOVIwamppNg==";
        BasicAWSCredentials credentials =
                new BasicAWSCredentials(
                        new String(Base64.getDecoder().decode(accessKey)),
                        new String(Base64.getDecoder().decode(secretKey))
                );

        S3Client client = new S3Client(bucketName, credentials);

        assertNotNull(
                client.getClient(),
                "Expected client to be not null."
        );
    }

    @Test
    public void twoClientsCanExistAtTheSameTime() {
        String b1 = bucketName + UUID.randomUUID();
        String b2 = bucketName + UUID.randomUUID();
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