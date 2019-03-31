package com.serndesign.kafka.learning.bigqueryProducer;
import com.google.cloud.bigquery.*;
import org.apache.kafka.clients.producer.KafkaProducer;

public class App {
    public static void main(String[] args) throws InterruptedException {
        var bigquery = BigQueryOptions.getDefaultInstance().getService();
        var query =
                "select fullVisitorId, hits_unnested.hitNumber, hits_unnested.page.pagePath\n" +
                "from `bigquery-public-data.google_analytics_sample.ga_sessions_20160801`,\n" +
                "unnest(hits) as hits_unnested\n" +
                "order by fullVisitorId, visitNumber, hitNumber\n" +
                "limit 1000";
        var queryConfig = QueryJobConfiguration.newBuilder(query).build();
        var table = bigquery.query(queryConfig);

        var isFirstField = true;
        for (Field field : table.getSchema().getFields()) {
            if (isFirstField) {
                isFirstField = false;
            } else {
                System.out.print(",");
            }
            System.out.print(field.getName());
        }
        System.out.println();

        for (FieldValueList row : table.iterateAll()) {
            isFirstField = true;
            for (FieldValue val : row) {
                if (isFirstField) {
                    isFirstField = false;
                } else {
                    System.out.print(",");
                }

                System.out.print(
                        val.isNull()
                                ? "NULL"
                                : val.getAttribute() == FieldValue.Attribute.PRIMITIVE
                                    ? val.getStringValue()
                                    : "COMPLEX");
            }
            System.out.println();
        }
    }
}
