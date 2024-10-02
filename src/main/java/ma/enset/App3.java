package ma.enset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class App3 {
    public static void main(String[] args) {
        // Configuration Spark
        SparkConf conf = new SparkConf().setAppName("VentesParVille").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Lecture du fichier ventes.txt
        JavaRDD<String> ventesRDD = sc.textFile("ventes.txt");

        // Transformation : extraire (ville, prix) à partir de chaque ligne et créer un JavaPairRDD
        JavaPairRDD<String, Double> villePrixRDD = ventesRDD.mapToPair(line -> {
            String[] parts = line.split(" ");
            String ville = parts[1];  // Deuxième colonne : ville
            Double prix = Double.parseDouble(parts[3]);  // Quatrième colonne : prix
            return new Tuple2<>(ville, prix);  // Créer un Tuple2 (ville, prix)
        });

        // Calcul du total des ventes par ville avec reduceByKey
        JavaPairRDD<String, Double> totalVentesParVilleRDD = villePrixRDD.reduceByKey((prix1, prix2) -> prix1 + prix2);

        // Afficher les résultats
        totalVentesParVilleRDD.foreach(result -> System.out.println("Ville: " + result._1 + ", Total des ventes: " + result._2));

        // Arrêter le contexte Spark
        sc.close();
    }
}
