package tp2

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object TP2SparkFraud {

  private def showBasicInfo(df: DataFrame, name: String): Unit = {
    println(s"\n=== $name ===")
    df.printSchema()
    df.show(10, truncate = false)
    println(s"Colonnes: ${df.columns.length}")
  }

  private def missingSummary(df: DataFrame): DataFrame = {
    val total = df.count()

    val exprs = df.columns.map { c =>
      val missing = sum(when(col(c).isNull || trim(col(c).cast("string")) === "", 1).otherwise(0)).alias("missing_count")
      missing
    }

    val missingCountsRow = df.agg(exprs.head, exprs.tail: _*).collect()(0)

    val rows = df.columns.zipWithIndex.map { case (c, idx) =>
      val mc = missingCountsRow.getLong(idx)
      val pct = if (total == 0) 0.0 else (mc.toDouble / total.toDouble) * 100.0
      (c, mc, pct)
    }

    val spark = df.sparkSession
    import spark.implicits._
    spark.createDataset(rows).toDF("colonne", "missing_count", "missing_percent")
      .orderBy(desc("missing_count"))
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("TP2 - Analyse Fraude (Spark)")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val basePath = "data"

    // Chargement
    val transactionsDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(s"$basePath/transactions_data.csv")

    val usersDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(s"$basePath/users_data.csv")

    val cardsDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(s"$basePath/cards_data.csv")

    val mccDF = spark.read
      .option("multiline", "true")
      .json(s"$basePath/mcc_codes.json")

    val errorsDF = spark.read
      .option("multiline", "true")
      .json(s"$basePath/errors.json")

    // Partie 1 - Q1
    showBasicInfo(transactionsDF, "transactions_data")
    showBasicInfo(usersDF, "users_data")
    showBasicInfo(cardsDF, "cards_data")
    showBasicInfo(mccDF, "mcc_codes")
    showBasicInfo(errorsDF, "errors")

    // Partie 1 - Q2: volumétrie (adaptation selon colonnes disponibles)
    // ملاحظة: إذا أسماء الأعمدة عندك مختلفة، غيّرها هنا فقط.
    val txCount = transactionsDF.count()
    val clientsUniques =
      if (transactionsDF.columns.contains("user_id")) transactionsDF.select(col("user_id")).distinct().count()
      else if (transactionsDF.columns.contains("userId")) transactionsDF.select(col("userId")).distinct().count()
      else usersDF.select(usersDF.columns.head).distinct().count() // fallback

    val cartesUniques =
      if (transactionsDF.columns.contains("card_id")) transactionsDF.select(col("card_id")).distinct().count()
      else if (cardsDF.columns.contains("card_id")) cardsDF.select(col("card_id")).distinct().count()
      else 0L

    val merchantsUniques =
      if (transactionsDF.columns.contains("merchant_id")) transactionsDF.select(col("merchant_id")).distinct().count()
      else if (transactionsDF.columns.contains("merchant")) transactionsDF.select(col("merchant")).distinct().count()
      else 0L

    println("\n=== Partie 1 - Q2 : Volumétrie ===")
    println(s"Nombre total de transactions = $txCount")
    println(s"Nombre de clients uniques     = $clientsUniques")
    println(s"Nombre de cartes uniques      = $cartesUniques")
    println(s"Nombre de commerçants uniques = $merchantsUniques")

    // Partie 1 - Q3: qualité
    println("\n=== Partie 1 - Q3 : Qualité des données ===")

    // 1) Colonnes avec valeurs manquantes (tableau récapitulatif)
    val miss = missingSummary(transactionsDF)
    miss.show(200, truncate = false)

    // 2) Montant <= 0 (si la colonne amount existe)
    if (transactionsDF.columns.contains("amount")) {
      val badAmount = transactionsDF.filter(col("amount") <= 0).count()
      println(s"Transactions avec montant <= 0 : $badAmount")
    } else {
      println("Colonne 'amount' non trouvée pour vérifier montant <= 0.")
    }

    // 3) Sans MCC (si mcc existe)
    if (transactionsDF.columns.contains("mcc")) {
      val noMcc = transactionsDF.filter(col("mcc").isNull).count()
      println(s"Transactions sans MCC : $noMcc")
    } else {
      println("Colonne 'mcc' non trouvée pour vérifier MCC manquant.")
    }

    // 4) Transactions contenant des erreurs (selon colonne error / errors)
    val hasErrorCol =
      transactionsDF.columns.contains("error") || transactionsDF.columns.contains("errors") || transactionsDF.columns.contains("error_code")

    if (hasErrorCol) {
      val c =
        if (transactionsDF.columns.contains("error")) "error"
        else if (transactionsDF.columns.contains("errors")) "errors"
        else "error_code"

      val withError = transactionsDF.filter(col(c).isNotNull).count()
      println(s"Transactions contenant des erreurs (colonne '$c') : $withError")
    } else {
      println("Aucune colonne d'erreur trouvée (error/errors/error_code).")
    }

    spark.stop()
  }
}
