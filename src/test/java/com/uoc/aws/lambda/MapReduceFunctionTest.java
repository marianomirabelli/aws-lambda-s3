package com.uoc.aws.lambda;

import org.junit.Test;

public class MapReduceFunctionTest {

    @Test
    public void mapReduceExecution(){
        MapReduceFunction mapReduceFunction = new MapReduceFunction();
        mapReduceFunction.handleRequest(null,null,null);

    }
}
