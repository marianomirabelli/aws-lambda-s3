package com.uoc.aws.lambda;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MapReduceFunction implements RequestHandler<Map<String, String>, String> {

    private final static String BUCKET_NAME = "bucket_name";
    private final static String INPUT_FILE = "input_file";
    private final static String OUTPUT_FILE = "output_file";

    public String handleRequest(Map<String, String> input, Context context) {

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.US_EAST_2)
                .build();
        String bucketName = input.get(BUCKET_NAME);
        String inputFile = input.get(INPUT_FILE);
        S3Object file = s3Client.getObject(new GetObjectRequest(bucketName, inputFile));
        Stream<String> streamReader = new BufferedReader(new InputStreamReader(file.getObjectContent())).lines();
        Map<String, Long> result = streamReader.flatMap(line -> Stream.of(line.trim().split(" "))).filter(word -> !(word.isEmpty() && word.equals("")))
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        byte[] fileContentBytes = result.toString().getBytes(StandardCharsets.UTF_8);
        InputStream fileInputStream = new ByteArrayInputStream(fileContentBytes);
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentType("plain/text");
        metadata.setContentLength(fileContentBytes.length);
        String outputFile = input.get(OUTPUT_FILE);
        PutObjectRequest request = new PutObjectRequest(bucketName, outputFile, fileInputStream, metadata);
        s3Client.putObject(request);
        return "OK";


    }
}
