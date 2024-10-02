
## App1 - Manipulation de données avec RDD
Cette application utilise Apache Spark pour effectuer des opérations de transformation et d'action sur une liste d'entiers. Elle montre comment utiliser des RDDs pour appliquer des transformations simples comme `map` et `filter` sur des collections de données, et comment effectuer des actions avec `foreach`.

## App2 - Compte de mots dans un fichier texte
Cette application lit un fichier texte, divise chaque ligne en mots, et calcule le nombre d'occurrences de chaque mot en utilisant des RDDs dans Apache Spark. Elle montre l'utilisation de transformations comme `flatMap` et `reduceByKey` pour compter les mots et afficher les résultats.

## App3 - Calcul du total des ventes par ville
Cette application calcule le total des ventes par ville à partir d'un fichier texte contenant des données de ventes. Elle montre comment utiliser `mapToPair` pour créer des paires `(ville, prix)` et comment utiliser `reduceByKey` pour agréger les résultats par ville.

## App4 - Calcul des ventes de produits par ville pour une année spécifique
Cette application filtre les données de ventes pour une année spécifique et calcule le total des ventes par produit et par ville. Elle démontre l'utilisation de transformations avancées comme la création de paires imbriquées `((ville, produit), prix)` et l'agrégation avec `reduceByKey`.
