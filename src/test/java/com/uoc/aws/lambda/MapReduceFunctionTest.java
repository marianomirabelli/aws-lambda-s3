package com.uoc.aws.lambda;

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class MapReduceFunctionTest {

    @Test
    public void mapReduceExecution(){
        MapReduceFunction mapReduceFunction = new MapReduceFunction();
        Map<String,Long> result = mapReduceFunction.handleRequest("",null);
        Assert.assertNotNull(result);
        Assert.assertEquals(result.size(),75);

    }
}
