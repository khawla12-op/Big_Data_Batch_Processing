package ma.enset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class App4 {
    public static void main(String[] args) {
        String annee ="2024";
        // Configuration Spark
        SparkConf conf = new SparkConf().setAppName("VentesProduitsParVille").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Lecture du fichier ventes.txt
        JavaRDD<String> ventesRDD = sc.textFile("ventes.txt");

        // Filtrer les ventes par année
        JavaRDD<String> ventesAnneeRDD = ventesRDD.filter(line -> line.startsWith(annee));

        // Transformation : extraire ((ville, produit), prix) à partir de chaque ligne et créer un JavaPairRDD
        JavaPairRDD<Tuple2<String, String>, Double> villeProduitPrixRDD = ventesAnneeRDD.mapToPair(line -> {
            String[] parts = line.split(" ");
            String ville = parts[1];  // Deuxième colonne : ville
            String produit = parts[2];  // Troisième colonne : produit
            Double prix = Double.parseDouble(parts[3]);  // Quatrième colonne : prix
            return new Tuple2<>(new Tuple2<>(ville, produit), prix);
        });

        // Calcul du total des ventes par produit et par ville avec reduceByKey
        JavaPairRDD<Tuple2<String, String>, Double> totalVentesProduitsParVilleRDD = villeProduitPrixRDD.reduceByKey(Double::sum);

        // Afficher les résultats
        totalVentesProduitsParVilleRDD.foreach(result ->
                System.out.println("Ville: " + result._1._1 + ", Produit: " + result._1._2 + ", Total des ventes: " + result._2));

        // Arrêter le contexte Spark
        sc.close();
    }
}
