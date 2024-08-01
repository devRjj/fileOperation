package com.devraj.fileOperation.services;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class S3Service {

    private static final Logger logger = LoggerFactory.getLogger(S3Service.class);

    private final AmazonS3 s3Client;

    @Value("${aws.s3.bucket}")
    private String bucketName;

    public S3Service(@Value("${aws.accessKey}") String accessKey,
                     @Value("${aws.secretKey}") String secretKey,
                     @Value("${aws.region}") String region) {
        logger.info("Initializing S3Service with region: {}", region);
        try {
            BasicAWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
            this.s3Client = AmazonS3ClientBuilder.standard()
                    .withRegion(region)
                    .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                    .build();
            logger.info("S3Client initialized successfully");
        } catch (Exception e) {
            logger.error("Error initializing S3Client", e);
            throw new RuntimeException("Failed to initialize S3Client", e);
        }
    }

    public byte[] downloadFile(String fileName) {
        logger.info("Attempting to download file: {}", fileName);
        if (s3Client == null) {
            logger.error("S3Client is null");
            throw new RuntimeException("S3Client is not initialized");
        }
        try {
            S3Object s3Object = s3Client.getObject(bucketName, fileName);
            S3ObjectInputStream inputStream = s3Object.getObjectContent();
            return inputStream.readAllBytes();
        } catch (Exception e) {
            logger.error("Error downloading file: {}", fileName, e);
            throw new RuntimeException("Failed to download file", e);
        }
    }

    public String uploadFile(MultipartFile file) {
        logger.info("Attempting to upload file: {}", file.getOriginalFilename());
        if (s3Client == null) {
            logger.error("S3Client is null");
            throw new RuntimeException("S3Client is not initialized");
        }
        
        try {
            File fileObj = convertMultiPartFileToFile(file);
            String fileName = System.currentTimeMillis() + "_" + file.getOriginalFilename();
            logger.info("Uploading file {} to bucket {}", fileName, bucketName);
            s3Client.putObject(bucketName, fileName, fileObj);
            fileObj.delete(); // Delete the temp file
            logger.info("File uploaded successfully: {}", fileName);
            return "File uploaded: " + fileName;
        } catch (Exception e) {
            logger.error("Error uploading file: {}", file.getOriginalFilename(), e);
            throw new RuntimeException("Failed to upload file", e);
        }
    }

    private File convertMultiPartFileToFile(MultipartFile file) throws IOException {
        File convertedFile = new File(file.getOriginalFilename());
        try (FileOutputStream fos = new FileOutputStream(convertedFile)) {
            fos.write(file.getBytes());
        }
        return convertedFile;
    }
}
