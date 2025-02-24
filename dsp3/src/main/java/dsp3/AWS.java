package dsp3;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import software.amazon.awssdk.services.s3.model.BucketLocationConstraint;
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;


public class AWS {
    private final SqsClient sqs;
    private final S3Client s3;
    public static Region region = Region.US_WEST_2;
    // public static Region region = Region.US_EAST_1;
    private static final AWS instance = new AWS();

    private AWS() {
        sqs = SqsClient.builder().region(region).build();
        s3 = S3Client.builder().region(region).build();
    }

    public static AWS getInstance() {
        return instance;
    } 

    public void createSqsQueue(String queueName) {
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(queueName)
                .build();
        sqs.createQueue(createQueueRequest);
    }

    public void createBucketIfNotExists(String bucketName) {
        try {
            // Create the bucket with the specified configuration
            s3.createBucket(CreateBucketRequest
                    .builder()
                    .bucket(bucketName)
                    .createBucketConfiguration(
                            CreateBucketConfiguration.builder()
                                    .locationConstraint(BucketLocationConstraint.US_WEST_2)
                                    .build())
                    .build());

            // Wait until the bucket exists
            s3.waiter().waitUntilBucketExists(HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build());
        } catch (S3Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public void sendSQSMessage(String queueName, String message) {
        try{
                SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                        .queueUrl(getQueueUrl(queueName))
                        .messageBody(message)
                        .build();
                        getInstance().sqs.sendMessage(sendMessageRequest);
        }catch (SqsException e){
                System.err.println("[DEBUG]: Error trying to send message to queue " + queueName + ", Error Message: " + e.awsErrorDetails().errorMessage());
        }
    }

    public String uploadFileToS3(String input_file_path, String bucketName) {
        String s3Key = Paths.get(input_file_path).getFileName().toString();

        try {
            // First, check if the file exists in the bucket
            HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
                    .bucket(bucketName)
                    .key(s3Key)
                    .build();

            try {
                // Try to fetch the file's metadata to check if it exists
                getInstance().s3.headObject(headObjectRequest);
                System.out.println("File already exists in S3: " + s3Key);
                return s3Key; // File exists, return the key
            } catch (NoSuchKeyException e) {
                // If the file doesn't exist, proceed to upload
                System.out.println("File does not exist in S3. Proceeding with upload...");
            }

            // File doesn't exist, proceed with upload
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(s3Key)
                    .build();

            Path path = Paths.get(input_file_path);
            getInstance().s3.putObject(putObjectRequest, path);
            System.out.println("File uploaded successfully to S3: " + s3Key);
            return s3Key;

        } catch (Exception e) {
            System.err.println("Unexpected error during file upload: " + e.getMessage());
            return null;
        }
    }

    public static String getQueueUrl(String QueueName) {
        // Get the URL of the SQS queue by name
        GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder()
                .queueName(QueueName)
                .build();
        GetQueueUrlResponse getQueueUrlResponse = getInstance().sqs.getQueueUrl(getQueueUrlRequest);
        return getQueueUrlResponse.queueUrl();
    }

    public String checkSQSQueue(String queueName) {
        try {
            // Retrieve the queue URL
            GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder()
                    .queueName(queueName)
                    .build();

            GetQueueUrlResponse queueUrlResponse = getInstance().sqs.getQueueUrl(getQueueUrlRequest);
            String queueUrl = queueUrlResponse.queueUrl();

            // Receive up to 1 messages
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(1)
                    .visibilityTimeout(0)
                    .build();

            ReceiveMessageResponse receiveMessageResponse = getInstance().sqs.receiveMessage(receiveMessageRequest);

            // Loop through the received messages
            for (Message message : receiveMessageResponse.messages()) {
                return message.body();
            }
        } catch (SqsException e) {
            System.err.println("Error checking SQS queue: " + e.awsErrorDetails().errorMessage());
        }
        return ""; // Return this if no matching message is found
    }

    

    public String downloadFromS3(String bucketName, String s3Key, String localPath) {
        try {
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .build();

        String fileName = Paths.get(s3Key).getFileName().toString();
        Path destination = Paths.get(localPath, fileName);
        ResponseInputStream<GetObjectResponse> response = s3.getObject(getObjectRequest);
        
        File file = destination.toFile();
        try (FileOutputStream fos = new FileOutputStream(file);
             InputStream is = response) {
            byte[] buffer = new byte[4096];
            int bytesRead;
            while ((bytesRead = is.read(buffer)) != -1) {
                fos.write(buffer, 0, bytesRead);
            }
        }
        
        System.out.println("[SUCCESS] File downloaded: " + destination);
        return destination.toString();

        } catch (S3Exception s3e) {
            System.err.println("[S3 ERROR] Bucket: " + bucketName + ", Key: " + s3Key);
            System.err.println("[S3 ERROR] Message: " + s3e.awsErrorDetails().errorMessage());
            System.err.println("[S3 ERROR] Code: " + s3e.awsErrorDetails().errorCode());
            return null;
        } catch (IOException ioe) {
            System.err.println("[IO ERROR] Failed to write to " + localPath);
            System.err.println("[IO ERROR] Message: " + ioe.getMessage());
            return null;
        } catch (Exception e) {
            System.err.println("[GENERAL ERROR] " + e.getClass().getName() + ": " + e.getMessage());
            return null;
        }
    }
}
