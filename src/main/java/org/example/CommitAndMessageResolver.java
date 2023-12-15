package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.jetbrains.annotations.NotNull;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

public class CommitAndMessageResolver
{
    private SparkSession spark;

    public void process(String filepath) {
        spark = SparkSession
                .builder()
                .appName("Java Spark CSV Example")
                .config("spark.master", "local")
                .getOrCreate();

        Dataset<Row> data = read(spark, filepath);

        data = filter(data);
        data = processRows(data);
        List<Row> list = data.collectAsList();
        writeToFile("result.txt", list);
    }

    private Dataset<Row> read(SparkSession spark, String filepath) {
        return spark.sqlContext().read().format("com.databricks.spark.csv")
                .load(filepath);
    }

    private Dataset<Row> filter(Dataset<Row> data) {
        return data.filter("_c1 = '\"type\":\"PushEvent\"' AND _c20 LIKE '%message%'")
                .select("_c3", "_c20");
    }

    private Dataset<Row> processRows(Dataset<Row> data) {
        spark.udf().register("getNameAndMessages",
                 (String login, String message) -> getNameAndMessages(login, message), DataTypes.StringType);

        data = data.withColumn("name_and_messages", functions.callUDF("getNameAndMessages",
                data.col("_c3"),
                data.col("_c20")));

        data = data.select(data.col("name_and_messages"));

        return data;
    }

    @NotNull
    private static String getNameAndMessages(String login, String message)
    {
        String name = login.split(":")[1].replace("\"", "").trim();

        String[] words = message.toLowerCase().replaceAll("[^a-z ]", "").split("\\s+");
        List<String> threeGrams = new ArrayList<>();

        for (int i = 0; i < words.length - 2; i++) {
            String threeGram = words[i] + " " + words[i + 1] + " " + words[i + 2];
            threeGrams.add(threeGram);
        }

        StringBuilder result = new StringBuilder(name);
        for (String threeGram : threeGrams) {
            result.append(", ").append(threeGram);
        }

        return result.toString();
    }

    private void writeToFile(String filePath, List<Row> lines) {
        try {
            FileWriter writer = new FileWriter(filePath);
            for (Row line : lines) {
                writer.write(line.toString() + "\n");
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
