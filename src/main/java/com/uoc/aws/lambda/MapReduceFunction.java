package com.uoc.aws.lambda;

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MapReduceFunction implements RequestHandler<String, Map<String, Long>> {

    public Map<String, Long> handleRequest(String s, Context context) {

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.US_EAST_2)
                .build();
        S3Object file = s3Client.getObject(new GetObjectRequest("uoc-words-count", "prueba.txt"));
        Stream<String> streamReader = new BufferedReader(new InputStreamReader(file.getObjectContent())).lines();
        Map<String, Long> result = streamReader.flatMap(line -> Stream.of(line.trim().split(" "))).filter(word -> !(word.isEmpty() && word.equals("")))
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        return result;


    }
}
