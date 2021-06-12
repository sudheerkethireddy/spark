package com.virtualpairprogrammers.sparksql;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;

public class SparkSQL {

    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession.builder()
                .appName("Saample SparkSQL")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> dataset = sparkSession
                .read()
                .option("header", true)
                .csv("src/main/resources/exams/students.csv");

        dataset.show();

        long count = dataset.count();
        System.out.println("There are "+count+ " rows");

        // to retrive one row in the datasert
        Row firstRow = dataset.first();

        // now to retrive columns within row we need to use get with columnIndex or getAs("columnName")
        String subject = firstRow.get(2).toString();
        int score = Integer.valueOf(firstRow.getAs("score"));

        System.out.println("1st row 3rd column value= "+subject);
        System.out.println(score);

        // filtering

        // Filtering #1 doing using string expression
        //Dataset<Row> modernArt = dataset.filter("subject = 'Modern Art' AND year >= 2007");
        //modernArt.show();

        // Filtering #2 using Lambdas
       /* Dataset<Row> modernArts = dataset.filter((FilterFunction<Row>) row -> row.getAs("subject").toString().equals("Modern Art")
                                                  && Integer.valueOf(row.getAs("year")) >= 2007);*/


        // Filtering #3 using Column
        Dataset<Row> modernArts = dataset.filter(col("subject").equalTo("Modern Art").and(col("year").geq("2007")));
        modernArts.show();


        // we can also create views on the dataset and after that use for querying data with sql type queries
        dataset.createOrReplaceTempView("student");
        Dataset<Row> results = sparkSession.sql("select avg(score) AS average_score from student where subject='Modern Art' AND year >=2007");

        results.show();
        sparkSession.close();
    }
}
