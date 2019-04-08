package com.serndesign.kafka.learning.bigqueryProducer;

import com.google.cloud.bigquery.*;

class BigQueryConsumer {

    private int batchSize;
    private int recordDelayMs;
    private int batchDelayMs;

    BigQueryConsumer(int batchSize, int recordDelayMs, int batchDelayMs) {
        this.batchSize = batchSize;
        this.recordDelayMs = recordDelayMs;
        this.batchDelayMs = batchDelayMs;
    }

    void Consume() throws InterruptedException {
        var bigquery = BigQueryOptions.getDefaultInstance().getService();
        int maxOffset = batchSize * 1000;
        for(int offset = 0; offset < maxOffset; offset += batchSize) {
            var query = String.format(
                    "select fullVisitorId, hits_unnested.hitNumber, hits_unnested.page.pagePath\n" +
                            "from `bigquery-public-data.google_analytics_sample.ga_sessions_20160801`,\n" +
                            "unnest(hits) as hits_unnested\n" +
                            "order by fullVisitorId, visitNumber, hitNumber\n" +
                            "limit %d offset %d",
                    batchSize,
                    offset);
            var queryConfig = QueryJobConfiguration.newBuilder(query).build();
            var table = bigquery.query(queryConfig);

            var isFirstField = true;

            for (FieldValueList row : table.iterateAll()) {
                var fullVisitorId = row.get(0).getLongValue();
                var hitNumber = row.get(1).getLongValue();
                var pagePath = row.get(2).getStringValue();

                System.out.println(String.format("%d,%d,%s", fullVisitorId, hitNumber, pagePath));
                Thread.sleep(recordDelayMs);
            }
        }
        Thread.sleep(batchDelayMs);
    }
}
