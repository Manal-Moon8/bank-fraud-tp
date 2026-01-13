 package tp2

import org.apache.spark.sql.{DataFrame, SparkSession, Column}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object TP2SparkFraud {

  // ==========================
  // Utils d'affichage
  // ==========================
  private def showBasicInfo(df: DataFrame, name: String): Unit = {
    println(s"\n==================== $name ====================")
    df.printSchema()
    df.show(10, truncate = false)
    println(s"Nombre de colonnes = ${df.columns.length}")
    println(s"Nombre de lignes   = ${df.count()}")
  }

  private def firstExistingCol(df: DataFrame, candidates: Seq[String]): Option[String] =
    candidates.find(df.columns.contains)

  // Tableau récapitulatif des valeurs manquantes
  private def missingSummary(df: DataFrame): DataFrame = {
    val total = df.count()

    val exprs = df.columns.map { c =>
      sum(
        when(col(c).isNull || trim(col(c).cast("string")) === "", 1).otherwise(0)
      ).alias(c)
    }

    val row = df.agg(exprs.head, exprs.tail: _*).collect()(0)

    val spark = df.sparkSession
    import spark.implicits._

    val rows = df.columns.zipWithIndex.map { case (c, idx) =>
      val missing = row.getLong(idx)
      val pct = if (total == 0) 0.0 else missing.toDouble * 100.0 / total.toDouble
      (c, missing, pct)
    }

    spark.createDataset(rows)
      .toDF("colonne", "missing_count", "missing_percent")
      .orderBy(desc("missing_count"))
  }

  // Parsing robuste timestamp
  private def toBestTimestamp(c: Column): Column = {
    coalesce(
      c.cast(TimestampType),
      to_timestamp(c, "yyyy-MM-dd HH:mm:ss"),
      to_timestamp(c, "yyyy-MM-dd'T'HH:mm:ss"),
      to_timestamp(c, "yyyy-MM-dd"),
      to_timestamp(c, "MM/dd/yyyy HH:mm:ss"),
      to_timestamp(c, "MM/dd/yyyy")
    )
  }

  // Nettoyage robuste du champ amount: "$175.73" -> 175.73 ; "$-77.00" -> -77.00
  private def parseAmountToDouble(amountCol: Column): Column = {
    val cleaned = regexp_replace(
      regexp_replace(trim(amountCol.cast("string")), "[$,]", ""), // supprime $ et virgules
      "\\s+",
      ""
    )
    when(cleaned.isNull || cleaned === "", lit(null).cast("double"))
      .otherwise(cleaned.cast("double"))
  }

  // Normalisation MCC quand le JSON est "wide": colonnes = codes MCC, 1 ligne = labels
  private def normalizeMcc(mccDF: DataFrame): DataFrame = {
    // Cas 1: fichier déjà en format long (mcc, label)
    val codeColOpt  = firstExistingCol(mccDF, Seq("mcc", "code", "mcc_code"))
    val labelColOpt = firstExistingCol(mccDF, Seq("category", "label", "description", "name"))

    (codeColOpt, labelColOpt) match {
      case (Some(cc), Some(ll)) =>
        mccDF.select(col(cc).cast("string").alias("mcc"),
          col(ll).cast("string").alias("merchant_category")).distinct()

      case _ =>
        // Cas 2: wide (colonnes = "1711", "3000", ...)
        val cols = mccDF.columns.toSeq
        val pairs = cols.map { c =>
          struct(lit(c).alias("mcc"), col(c).cast("string").alias("merchant_category"))
        }

        mccDF
          .select(explode(array(pairs: _*)).alias("kv"))
          .select(col("kv.mcc"), col("kv.merchant_category"))
          .filter(col("merchant_category").isNotNull && trim(col("merchant_category")) =!= "")
          .distinct()
    }
  }

  // Normalisation Errors (si fichier vide => DataFrame vide)
  private def normalizeErrors(errorsDF: DataFrame): DataFrame = {
    if (errorsDF.columns.isEmpty) {
      // DataFrame vide (0 colonnes)
      val spark = errorsDF.sparkSession
      import spark.implicits._
      return spark.emptyDataset[(String, String)]
        .toDF("error_code", "error_label")
    }

    val codeColOpt  = firstExistingCol(errorsDF, Seq("error_code", "code", "error"))
    val labelColOpt = firstExistingCol(errorsDF, Seq("label", "description", "message", "name"))

    (codeColOpt, labelColOpt) match {
      case (Some(cc), Some(ll)) =>
        errorsDF.select(col(cc).cast("string").alias("error_code"),
          col(ll).cast("string").alias("error_label")).distinct()

      case _ =>
        // Tentative Map JSON (si applicable)
        val first = errorsDF.columns.head
        errorsDF
          .selectExpr(s"explode(map_entries($first)) as kv")
          .select(col("kv.key").cast("string").alias("error_code"),
            col("kv.value").cast("string").alias("error_label"))
          .distinct()
    }
  }

  // Flag erreur robuste (chez toi: colonne "errors" très majoritairement vide)
  private def errorFlag(df: DataFrame): Column = {
    val errorColOpt = firstExistingCol(df, Seq("error", "errors", "error_code", "decline_reason"))
    errorColOpt match {
      case Some(c) => when(col(c).isNotNull && trim(col(c).cast("string")) =!= "", lit(1)).otherwise(lit(0))
      case None    => lit(0)
    }
  }

  def main(args: Array[String]): Unit = {

    // ========== SparkSession ==========
    val spark = SparkSession.builder()
      .appName("TP2 - Analyse Fraude (Spark)")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val basePath = "data"

    // ========== Chargement ==========
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

    val mccRawDF = spark.read
      .option("multiline", "true")
      .json(s"$basePath/mcc_codes.json")

    val errorsRawDF = spark.read
      .option("multiline", "true")
      .json(s"$basePath/errors.json")

    // ============================================================
    // PARTIE 1 – Q1 : Explorer / afficher
    // ============================================================
    println("\n==================== PARTIE 1 - Q1 ====================")
    showBasicInfo(transactionsDF, "transactions_data")
    showBasicInfo(usersDF, "users_data")
    showBasicInfo(cardsDF, "cards_data")
    showBasicInfo(mccRawDF, "mcc_codes (raw)")
    showBasicInfo(errorsRawDF, "errors (raw)")

    // ============================================================
    // PARTIE 1 – Q2 : Volumétrie
    // ============================================================
    println("\n==================== PARTIE 1 - Q2 (Volumétrie) ====================")

    val txCount = transactionsDF.count()

    val userIdCol = firstExistingCol(transactionsDF, Seq("user_id", "userId", "client_id"))
      .getOrElse(usersDF.columns.head)

    val cardIdColOpt = firstExistingCol(transactionsDF, Seq("card_id", "cardId"))
      .orElse(firstExistingCol(cardsDF, Seq("card_id", "cardId")))

    val merchantColOpt = firstExistingCol(transactionsDF, Seq("merchant_id", "merchant", "merchant_name"))

    val clientsUniques = transactionsDF.select(col(userIdCol)).distinct().count()
    val cartesUniques  = cardIdColOpt.map(c => transactionsDF.select(col(c)).distinct().count()).getOrElse(0L)
    val merchantsUniques = merchantColOpt.map(c => transactionsDF.select(col(c)).distinct().count()).getOrElse(0L)

    println(s"Nombre total de transactions = $txCount")
    println(s"Nombre de clients uniques     = $clientsUniques")
    println(s"Nombre de cartes uniques      = $cartesUniques")
    println(s"Nombre de commerçants uniques = $merchantsUniques")

    // ============================================================
    // PARTIE 1 – Q3 : Qualité des données
    // ============================================================
    println("\n==================== PARTIE 1 - Q3 (Qualité) ====================")

    println("\n--- Valeurs manquantes par colonne (transactions) ---")
    missingSummary(transactionsDF).show(200, truncate = false)

    // IMPORTANT: amount est string avec "$", donc on nettoie avant comparaison
    if (transactionsDF.columns.contains("amount")) {
      val badAmount = transactionsDF
        .withColumn("amount_num_tmp", parseAmountToDouble(col("amount")))
        .filter(col("amount_num_tmp").isNotNull && col("amount_num_tmp") <= 0)
        .count()
      println(s"Transactions avec montant <= 0 : $badAmount")
    } else {
      println("Colonne 'amount' non trouvée.")
    }

    val mccColOpt = firstExistingCol(transactionsDF, Seq("mcc", "mcc_code"))
    mccColOpt match {
      case Some(mccCol) =>
        val noMcc = transactionsDF.filter(col(mccCol).isNull || trim(col(mccCol).cast("string")) === "").count()
        println(s"Transactions sans MCC : $noMcc")
      case None =>
        println("Colonne MCC non trouvée (mcc / mcc_code).")
    }

    val withErrorCount = transactionsDF
      .withColumn("has_error", errorFlag(transactionsDF))
      .filter(col("has_error") === 1)
      .count()

    println(s"Transactions contenant des erreurs (colonnes error*) : $withErrorCount")

    // ============================================================
    // Préparation colonnes normalisées pour PARTIE 2→4
    // ============================================================
    val amountColOpt = firstExistingCol(transactionsDF, Seq("amount", "amt", "transaction_amount"))
    val dateColOpt   = firstExistingCol(transactionsDF, Seq("date", "datetime", "transaction_date", "timestamp", "trans_date"))
    val cityColOpt   = firstExistingCol(transactionsDF, Seq("city", "merchant_city", "billing_city", "user_city"))

    if (amountColOpt.isEmpty) println("[WARN] Aucun champ montant détecté => Q4 sera incomplet.")
    if (dateColOpt.isEmpty)   println("[WARN] Aucun champ date/heure détecté => Q5/Q8/Q9 seront incomplets.")

    val txBase = transactionsDF
      .withColumn("amount_num",
        amountColOpt.map(c => parseAmountToDouble(col(c))).getOrElse(lit(null).cast("double"))
      )
      .withColumn("ts",
        dateColOpt.map(c => toBestTimestamp(col(c).cast("string"))).getOrElse(lit(null).cast("timestamp"))
      )
      .withColumn("has_error", errorFlag(transactionsDF))
      .withColumn("user_id_norm", col(userIdCol).cast("string"))
      .withColumn("card_id_norm", cardIdColOpt.map(c => col(c).cast("string")).getOrElse(lit(null).cast("string")))
      .withColumn("mcc_norm", mccColOpt.map(c => col(c).cast("string")).getOrElse(lit(null).cast("string")))
      .withColumn("city_norm", cityColOpt.map(c => col(c).cast("string")).getOrElse(lit(null).cast("string")))

    txBase.cache()

    // ============================================================
    // PARTIE 2 – Q4 : Analyse des montants
    // ============================================================
    println("\n==================== PARTIE 2 - Q4 (Analyse des montants) ====================")

    val txWithAmount = txBase.filter(col("amount_num").isNotNull)

    txWithAmount.agg(
      round(avg("amount_num"), 2).alias("moyenne"),
      min("amount_num").alias("minimum"),
      max("amount_num").alias("maximum")
    ).show(truncate = false)

    val median = txWithAmount.stat.approxQuantile("amount_num", Array(0.5), 0.001).headOption.getOrElse(Double.NaN)
    println(s"Médiane (approx) = $median")

    val bucketed = txWithAmount.withColumn(
      "tranche",
      when(col("amount_num") < 10, "< 10")
        .when(col("amount_num").between(10, 50), "10-50")
        .when(col("amount_num").between(50, 200), "50-200")
        .otherwise("> 200")
    )

    bucketed.groupBy("tranche")
      .agg(
        count(lit(1)).alias("nb_transactions"),
        round(avg("amount_num"), 2).alias("moy_amount")
      )
      .orderBy(desc("nb_transactions"))
      .show(truncate = false)

    // ============================================================
    // PARTIE 2 – Q5 : Analyse temporelle
    // ============================================================
    println("\n==================== PARTIE 2 - Q5 (Analyse temporelle) ====================")

    val txWithTime = txBase.filter(col("ts").isNotNull)
      .withColumn("hour", hour(col("ts")))
      .withColumn("day_of_week", date_format(col("ts"), "E"))
      .withColumn("month", month(col("ts")))
      .withColumn("day", to_date(col("ts")))

    txWithTime.groupBy("hour").agg(count(lit(1)).alias("nb")).orderBy("hour")
      .show(50, truncate = false)

    txWithTime.groupBy("day_of_week").agg(count(lit(1)).alias("nb")).orderBy(desc("nb"))
      .show(20, truncate = false)

    // ============================================================
    // PARTIE 3 – Q6 : Jointure avec les MCC
    // ============================================================
    println("\n==================== PARTIE 3 - Q6 (Join MCC) ====================")

    val mccDF = normalizeMcc(mccRawDF)

    val txEnrichedMcc = txBase.join(mccDF, txBase("mcc_norm") === mccDF("mcc"), "left")
      .drop(mccDF("mcc"))

    txEnrichedMcc.groupBy("merchant_category")
      .agg(count(lit(1)).alias("volume"))
      .orderBy(desc("volume"))
      .limit(10)
      .show(truncate = false)

    txEnrichedMcc.filter(col("amount_num").isNotNull)
      .groupBy("merchant_category")
      .agg(
        count(lit(1)).alias("nb"),
        round(avg("amount_num"), 2).alias("avg_amount"),
        round(avg(col("has_error")), 3).alias("avg_error_flag")
      )
      .orderBy(desc("nb"))
      .show(30, truncate = false)

    // ============================================================
    // PARTIE 3 – Q7 : Analyse des erreurs
    // ============================================================
    println("\n==================== PARTIE 3 - Q7 (Analyse des erreurs) ====================")

    val errorsDF = normalizeErrors(errorsRawDF)
    val errorCodeColOpt = firstExistingCol(transactionsDF, Seq("error_code", "error", "errors", "decline_reason"))

    val txWithErrorLabel =
      errorCodeColOpt match {
        case Some(ec) if errorsDF.columns.nonEmpty =>
          txBase.withColumn("error_code_norm", col(ec).cast("string"))
            .join(errorsDF, col("error_code_norm") === errorsDF("error_code"), "left")
            .drop(errorsDF("error_code"))

        case _ =>
          txBase.withColumn("error_code_norm", lit(null).cast("string"))
            .withColumn("error_label", lit(null).cast("string"))
      }

    // Top erreurs (si on a des labels)
    if (txWithErrorLabel.columns.contains("error_label")) {
      txWithErrorLabel.filter(col("has_error") === 1)
        .groupBy("error_code_norm", "error_label")
        .agg(count(lit(1)).alias("nb"))
        .orderBy(desc("nb"))
        .limit(15)
        .show(truncate = false)
    } else {
      println("Fichier errors.json vide ou non exploitable : analyse détaillée des labels ignorée.")
    }

    // taux d'erreur par carte
    txWithErrorLabel.filter(col("card_id_norm").isNotNull)
      .groupBy("card_id_norm")
      .agg(
        count(lit(1)).alias("tx_total"),
        sum(col("has_error")).alias("tx_errors"),
        round(sum(col("has_error")) / count(lit(1)), 3).alias("error_ratio")
      )
      .orderBy(desc("error_ratio"), desc("tx_total"))
      .show(20, truncate = false)

    // taux d'erreur par client
    txWithErrorLabel.groupBy("user_id_norm")
      .agg(
        count(lit(1)).alias("tx_total"),
        sum(col("has_error")).alias("tx_errors"),
        round(sum(col("has_error")) / count(lit(1)), 3).alias("error_ratio")
      )
      .orderBy(desc("error_ratio"), desc("tx_total"))
      .show(20, truncate = false)

    // ============================================================
    // PARTIE 4 – Q8 : Indicateurs (par carte et par jour)
    // ============================================================
    println("\n==================== PARTIE 4 - Q8 (Indicateurs) ====================")

    val indicatorsByCardDay = txWithErrorLabel
      .filter(col("card_id_norm").isNotNull && col("ts").isNotNull)
      .withColumn("day", to_date(col("ts")))
      .groupBy("card_id_norm", "day")
      .agg(
        count(lit(1)).alias("tx_count"),
        round(sum(coalesce(col("amount_num"), lit(0.0))), 2).alias("total_amount"),
        countDistinct(col("city_norm")).alias("distinct_cities"),
        round(sum(col("has_error")) / count(lit(1)), 3).alias("error_ratio")
      )
      .orderBy(desc("tx_count"))

    indicatorsByCardDay.show(30, truncate = false)

    // ============================================================
    // PARTIE 4 – Q9 : Détection de comportements suspects
    // ============================================================
    println("\n==================== PARTIE 4 - Q9 (Suspicious cards) ====================")

    val X_TX_PER_DAY  = 20
    val MAX_CITIES    = 3
    val MAX_TOTAL     = 1000.0
    val MAX_ERR_RATIO = 0.30

    val suspicious_cards = indicatorsByCardDay.filter(
      col("tx_count") > X_TX_PER_DAY ||
      col("distinct_cities") > MAX_CITIES ||
      col("total_amount") > MAX_TOTAL ||
      col("error_ratio") > MAX_ERR_RATIO
    )

    suspicious_cards.show(50, truncate = false)

    // ============================================================
    // PARTIE 5 – Q10 : Synthèse (dans fichier REMI_TP.md)
    // ============================================================
    println("\n==================== PARTIE 5 - Q10 (Synthèse) ====================")
    println("La synthèse écrite est fournie dans le fichier REMI_TP.md (patterns, features, limites).")

    // Bonus : sauvegarde Parquet (optionnel)
    // suspicious_cards.write.mode("overwrite").parquet("output/suspicious_cards")

    spark.stop()
  }
}
