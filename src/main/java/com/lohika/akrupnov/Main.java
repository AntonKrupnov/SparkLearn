package com.lohika.akrupnov;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Main {

    public static void main(String[] args) {
        String topWordsFile = "/Users/akrupnov/IdeaProjects/SparkLearn/src/main/resources/top100words.txt";
        String textFile = "/Users/akrupnov/IdeaProjects/SparkLearn/src/main/resources/hamlet.txt";

        SparkSession session = SparkSession.builder().appName("hamlet").master("local").getOrCreate();

        Dataset<String> topWords = session.read().textFile(topWordsFile);
        Dataset<String> textWords = session.read().textFile(textFile)
                .flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(
                        s.replaceAll("[^A-Za-z]", " ")
                                .split(" ")).iterator(), Encoders.STRING());

        long textWordsCount = textWords.count();

        List<String> longestWords =
                textWords
                        .sort(functions.length(new Column("value"))
                                .desc())
                        .limit(10)
                        .collectAsList();

        List<String> mostFrequentWordsFromText =
                topWords.join(textWords.groupBy(new Column("value")).count(), "value")
                        .sort(functions.desc("count"))
                        .limit(10)
                        .select("value")
                        .as(Encoders.STRING())
                        .collectAsList();

        long notFrequentWordsFromText = textWords.except(topWords).count();

        System.out.println("How many words are in text file “/data/hamlet.txt” - " + textWordsCount);
        System.out.println("What are top 10 longest words in the text: \n\t" + String.join("\n\t", longestWords));
        System.out.println("For each word from file with most frequently used English words count " +
                "get top 10 words are used in the text “/data/hamlet.txt” most frequently: \n\t" + String.join(", ", mostFrequentWordsFromText));
        System.out.println("Count how many words are in the text “/data/hamlet.txt” " +
                "but exclude most frequently used words. - " + notFrequentWordsFromText);
    }
}
