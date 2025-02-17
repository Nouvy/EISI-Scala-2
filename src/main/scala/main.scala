import org.apache.spark.sql.{SparkSession, functions}

import scala.io.StdIn

@main
def main(): Unit =

  val spark = SparkSession.builder()
    .appName("Analyse de stock massive")
    .master("local[*]")
    .getOrCreate()

  val stocksDF = spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv("transactions_massives_scala_spark.csv")


  var choix = 1;
  while (choix != 0) {
    println("-------MENU--------")
    println("1- Afficher le nombre de lignes")
    println("2- Calculer la moyenne, le min et le max du prix unitaire.")
    println("3- Grouper les transactions par catégorie et calculer la somme totale des montants.")
    println("4- Identifier les 5 produits les plus vendus avec leur nombre de vente en fonction du nombre de transactions.")
    println("5- Analyse temporelle des ventes : ")
    println("6- Calculer le chiffre d’affaires pour chaque mois")
    println("7- Afficher le top 6 des mois les plus rentables")
    println("0- QUITTER")
    choix = StdIn.readLine().toInt
    choix match {
      case 0 =>
        println("Bonne journée ! :)")
      case 1 =>
        println("Nombre de lignes : " + stocksDF.count())
      case 2 =>
        stocksDF.select(
          functions.format_number(functions.avg("prix_unitaire"), 2).alias("Prix Moyen"),
          functions.min("prix_unitaire").alias("Prix Min"),
          functions.max("prix_unitaire").alias("Prix Max")
        ).show()
      case 3 =>
        stocksDF.groupBy("categorie")
          .agg(functions.sum("montant").alias("Total Ventes"))
          .withColumn("Total Ventes", functions.format_number(functions.col("Total Ventes"), 2))
          .orderBy(functions.desc("Total Ventes"))
          .show()
      case 4 =>
        stocksDF.groupBy("produit")
          .agg(functions.sum("quantite").alias("Nombre de Ventes"))
          .withColumn("Nombre de Ventes", functions.format_number(functions.col("Nombre de Ventes"), 0))
          .orderBy(functions.desc("Nombre de Ventes"))
          .limit(5)
          .show()
      case 5 =>
        // Convertir la colonne date en format Date
        val stock_date = stocksDF.withColumn("date_achat", functions.to_date(functions.col("date_achat"), "yyyy-MM-dd"))

        val ventesParMois = stock_date
          .groupBy(functions.month(functions.col("date_achat")).alias("Mois"))
          .agg(functions.sum("quantite").alias("Nombre de Ventes"))
          .withColumn("Nombre de Ventes", functions.format_number(functions.col("Nombre de Ventes"), 0))
          .orderBy("Mois")

        ventesParMois.show()

        // Mois avec le plus de ventes
        ventesParMois.orderBy(functions.desc("Nombre de Ventes")).limit(1).show()
      case 6 =>
        // Convertir la colonne date en format Date
        val ca_date = stocksDF.withColumn("date_achat", functions.to_date(functions.col("date_achat"), "yyyy-MM-dd"))

        ca_date
          .groupBy(functions.month(functions.col("date_achat")).alias("Mois"))
          .agg(functions.sum("montant").alias("Chiffre d'Affaires"))
          .withColumn("Chiffre d'Affaires", functions.format_number(functions.col("Chiffre d'Affaires"), 0))
          .orderBy("Mois")
          .show()
      case 7 =>
        // Convertir la colonne date en format Date
        val ca_date2 = stocksDF.withColumn("date_achat", functions.to_date(functions.col("date_achat"), "yyyy-MM-dd"))

        ca_date2
          .groupBy(functions.month(functions.col("date_achat")).alias("Mois"))
          .agg(functions.sum("montant").alias("Chiffre d'Affaires"))
          .withColumn("Chiffre d'Affaires", functions.format_number(functions.col("Chiffre d'Affaires"), 0))
          .orderBy("Mois")
          .orderBy(functions.desc("Chiffre d'Affaires"))
          .limit(6)
          .show()
    }
  }