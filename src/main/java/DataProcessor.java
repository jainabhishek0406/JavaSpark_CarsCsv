package main.java;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @author Abhishek Jain
 *
 * https://allaboutscala.com/big-data/spark/
 * https://spark.apache.org/docs/latest/sql-getting-started.html
 */
public class DataProcessor {
    public void processListData(SparkSession sparkSession) {
        System.out.println("##### Inside processListData #####");
        System.out.println("sparkSession = " + sparkSession);

        Dataset<Row> df = sparkSession
                .read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .option("dateFormat","yyyy-MM-dd HH:mm:ss")
                .load("cars.csv");

        df.printSchema();

        df.createOrReplaceTempView("car");
        df = sparkSession.sql("select model, maker, max(mileage) from car group by maker, model");
        df.show();
    }
}


/*  working
    df.printSchema();

    df.select("maker", "model").show();
    df.groupBy("maker").count().show(10);
    df.show();
    df.show(5);

    df.createOrReplaceTempView("car");
    df = sparkSession.sql("select * from car");
    df.show(5);
 */