package com.virtualpairprogrammers.sparksql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import javax.xml.crypto.Data;
import java.util.ArrayList;
import java.util.List;

public class SparkSQLAggregations {

    public static void main(String[] args) {

        SparkSession session = SparkSession.builder().appName("Aggregations-SparkSQL")
                .master("local[*]").getOrCreate();

        /*List<Row> inMemory = new ArrayList<>();

        inMemory.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
        inMemory.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
        inMemory.add(RowFactory.create("INFO", "2016-10-31 03:21:21"));
        inMemory.add(RowFactory.create("FATAL", "2016-12-31 14:32:21"));
        inMemory.add(RowFactory.create("WARN", "2016-04-15 19:23:20"));


        StructField[] structFields = new StructField[] {
                new StructField("level", DataTypes.StringType, false, Metadata.empty()),
                new StructField("datetime", DataTypes.StringType, false, Metadata.empty()),
        };

        StructType schema = new StructType(structFields);
        Dataset<Row> dataSet = session.createDataFrame(inMemory, schema);*/

        Dataset<Row> dataSet = session.read().option("header", true).csv("src/main/resources/biglog.txt");
        dataSet.createOrReplaceTempView("logging_table");

        Dataset<Row> groupedByMonth = session.sql("select level,date_format(datetime, 'MMMM') as month, count(1) as total from logging_table group by level, month order by month");
        groupedByMonth.show(100);

        groupedByMonth.createOrReplaceTempView("sum_records");

        Dataset<Row> output = session.sql("select sum(total) from sum_records");

        output.show();
        session.close();
    }
}
