package com.uoc.aws.lambda;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class MapReduceFunctionTest {


    private Map<String, String> input;

    @Before
    public void setUp() {
        this.input = new HashMap<String, String>();
        this.input.put("bucket_name", "uoc-words-count");
        this.input.put("input_file", "prueba.txt");
        this.input.put("output_file", "output.txt");
    }


    @Test
    public void mapReduceExecution() {
        MapReduceFunction mapReduceFunction = new MapReduceFunction();
        String result = mapReduceFunction.handleRequest(this.input, null);
        Assert.assertEquals("OK",result);

    }
}
