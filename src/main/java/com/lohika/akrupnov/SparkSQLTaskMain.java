package com.lohika.akrupnov;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

public class SparkSQLTaskMain {

    public static void main(String[] args) {
        String topWordsFile = "/Users/akrupnov/IdeaProjects/SparkLearn/src/main/resources/top100words.txt";
        String textFile = "/Users/akrupnov/IdeaProjects/SparkLearn/src/main/resources/hamlet.txt";

        SparkSession session = SparkSession.builder()
                .master("local")
                .appName("sparkSQL")
                .getOrCreate();

        session.read().textFile(topWordsFile).createOrReplaceTempView("top_words");
        session.read().textFile(textFile)
                .flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(
                        s.replaceAll("[^A-Za-z]", " ")
                                .split(" ")).iterator(), Encoders.STRING())
                .createOrReplaceTempView("text");

        List<Row> wordsCount = session.sql("select count(value) from text").collectAsList();
        List<Row> longestWords = session.sql("select value from text order by char_length(value) desc limit 10").collectAsList();
        List<Row> mostPopular = session.sql("select value, count(*) as wordscount from top_words join text using(value) group by value order by wordscount desc limit 10").collectAsList();
        List<Row> unpopularCount = session.sql("select count(distinct *) from text left join top_words using(value) where top_words.value is NULL").collectAsList();

        System.out.println(wordsCount);
        System.out.println(longestWords);
        System.out.println(mostPopular);
        System.out.println(unpopularCount);

    }
}
