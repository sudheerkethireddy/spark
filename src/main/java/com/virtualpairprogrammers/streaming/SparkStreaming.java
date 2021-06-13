package com.virtualpairprogrammers.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.logging.Logger;

public class SparkStreaming {

    public static void main(String[] args) throws InterruptedException {


        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark Streaming");
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(15));

        // hooking up the stram with socket
        JavaReceiverInputDStream<String> inputData = streamingContext.socketTextStream("localhost", 8989);



        JavaDStream<Object> results = inputData.map(item -> item);
        results.print();

        // this will start processing the termination
        streamingContext.start();

        // this will wait for the termination
        streamingContext.awaitTermination();
    }
}
