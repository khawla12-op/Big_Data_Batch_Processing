package ma.enset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class App2 {
    public static void main(String[] args) {
        // Configuration de Spark
        SparkConf conf = new SparkConf().setAppName("TP 1").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Lecture du fichier texte "words.txt"
        JavaRDD<String> rddLines = sc.textFile("words.txt");

        // Transformation : chaque ligne est divisée en mots
        JavaRDD<String> rddWords = rddLines.flatMap(line -> Arrays.asList(line.split("\\s+")).iterator());

        // Création de paires (mot, 1) pour chaque mot
        JavaPairRDD<String, Integer> wordsPair = rddWords.mapToPair(word -> new Tuple2<>(word, 1));

        // Calcul du nombre d'occurrences de chaque mot avec reduceByKey
        JavaPairRDD<String, Integer> wordCount = wordsPair.reduceByKey((a, b) -> a + b);

        // Afficher les résultats
        wordCount.foreach(e -> System.out.println(e._1() + " " + e._2()));

        // Arrêter le contexte Spark
        sc.close();
    }
}
