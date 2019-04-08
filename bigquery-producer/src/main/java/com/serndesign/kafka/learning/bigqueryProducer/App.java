package com.serndesign.kafka.learning.bigqueryProducer;

public class App {
    public static void main(String[] args) throws InterruptedException {
        var consumer = new BigQueryConsumer(10, 1000, 5000);
        consumer.Consume();
    }
}
