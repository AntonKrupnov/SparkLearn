package com.lohika.akrupnov;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class RDDTaskMain {

    public static void main(String[] args) {
        String topWordsFile = "/Users/akrupnov/IdeaProjects/SparkLearn/src/main/resources/top100words.txt";
        String textFile = "/Users/akrupnov/IdeaProjects/SparkLearn/src/main/resources/hamlet.txt";


        SparkSession localSession = SparkSession.builder()
                .appName("rdd-task")
                .master("local")
                .getOrCreate();

        JavaRDD<String> topWords = localSession.sparkContext().textFile(topWordsFile, 1).toJavaRDD()
                .flatMap(s -> Arrays.asList(s.replaceAll("[^A-Za-z]", " ")
                        .split(" ")).iterator());
        JavaRDD<String> text = localSession.sparkContext().textFile(textFile, 1).toJavaRDD()
                .flatMap(s -> Arrays.asList(s.replaceAll("[^A-Za-z]", " ")
                        .split(" ")).iterator());

        long count = text.count();
        List<String> topLongest = text.sortBy(String::length, false, 1).take(10);

        JavaPairRDD<String, Integer> wordsCount = text.mapToPair(s -> new Tuple2<>(s, 1))
                .aggregateByKey(0, Integer::sum, Integer::sum);

        List<String> mostFrequentWordsFromText = topWords.mapToPair(s -> new Tuple2<>(s, s))
                .join(wordsCount)
                .takeOrdered(10, new PairComparator().reversed())
                .stream().map(Tuple2::_1).collect(Collectors.toList());

        long countNotFrequentWords = text.subtract(topWords).distinct().count();

        System.out.println(count);
        System.out.println(String.join("\n", topLongest));
        System.out.println(mostFrequentWordsFromText);
        System.out.println(countNotFrequentWords);
    }

    private static class PairComparator implements Comparator<Tuple2<String, Tuple2<String, Integer>>>, Serializable {

        @Override
        public int compare(Tuple2<String, Tuple2<String, Integer>> o1, Tuple2<String, Tuple2<String, Integer>> o2) {
            return o1._2._2.compareTo(o2._2._2);
        }
    }
}
