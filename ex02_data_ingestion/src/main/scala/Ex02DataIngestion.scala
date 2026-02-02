import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.time.LocalDate

object Ex02DataIngestion {

  final case class Params(
    year: String = "2023",
    month: String = "01",
    enableDw: Boolean = false,
    dwTable: String = "public.yellow_trips_staging"
  )

  private def parseArgs(args: Array[String]): Params = {
    def next(i: Int): String = if (i + 1 < args.length) args(i + 1) else ""
    var p = Params()
    var i = 0
    while (i < args.length) {
      args(i) match {
        case "--year"     => p = p.copy(year = next(i)); i += 2
        case "--month"    => p = p.copy(month = next(i)); i += 2
        case "--enableDw" => p = p.copy(enableDw = next(i).toLowerCase == "true"); i += 2
        case "--dwTable"  => p = p.copy(dwTable = next(i)); i += 2
        case other =>
          System.err.println(s"[WARN] Unknown arg: $other")
          i += 1
      }
    }
    p
  }

  private def monthWindow(year: String, month: String): (String, String) = {
    val y = year.toInt
    val m = month.toInt
    val start = LocalDate.of(y, m, 1)
    val end = start.plusMonths(1)
    // On construit des timestamps ISO
    val startStr = start.toString + " 00:00:00"
    val endStr   = end.toString + " 00:00:00"
    (startStr, endStr)
  }

  /** Nettoyage strictement générique (non métier) + filtrage temporel strict sur le mois */
  private def cleanYellow(df: DataFrame, year: String, month: String): DataFrame = {
    val (startStr, endStr) = monthWindow(year, month)

    // Cast explicite (générique)
    val casted = df
      .withColumn("VendorID", col("VendorID").cast(IntegerType))
      .withColumn("tpep_pickup_datetime", col("tpep_pickup_datetime").cast(TimestampType))
      .withColumn("tpep_dropoff_datetime", col("tpep_dropoff_datetime").cast(TimestampType))
      .withColumn("passenger_count", col("passenger_count").cast(IntegerType))
      .withColumn("trip_distance", col("trip_distance").cast(DoubleType))
      .withColumn("RatecodeID", col("RatecodeID").cast(IntegerType))
      .withColumn("store_and_fwd_flag", col("store_and_fwd_flag").cast(StringType))
      .withColumn("PULocationID", col("PULocationID").cast(IntegerType))
      .withColumn("DOLocationID", col("DOLocationID").cast(IntegerType))
      .withColumn("payment_type", col("payment_type").cast(IntegerType))
      .withColumn("fare_amount", col("fare_amount").cast(DoubleType))
      .withColumn("extra", col("extra").cast(DoubleType))
      .withColumn("mta_tax", col("mta_tax").cast(DoubleType))
      .withColumn("tip_amount", col("tip_amount").cast(DoubleType))
      .withColumn("tolls_amount", col("tolls_amount").cast(DoubleType))
      .withColumn("improvement_surcharge", col("improvement_surcharge").cast(DoubleType))
      .withColumn("total_amount", col("total_amount").cast(DoubleType))
      .withColumn("congestion_surcharge", col("congestion_surcharge").cast(DoubleType))
      .withColumn("airport_fee", col("airport_fee").cast(DoubleType))

    // Filtrage temporel strict (job mensuel)
    // On filtre sur pickup_datetime, car c’est la référence la plus courante.
    val startTs = to_timestamp(lit(startStr))
    val endTs   = to_timestamp(lit(endStr))

    val timeFiltered = casted
      .filter(col("tpep_pickup_datetime").isNotNull)
      .filter(col("tpep_pickup_datetime") >= startTs && col("tpep_pickup_datetime") < endTs)

    // Nulls critiques + cohérence générique (non métier)
    timeFiltered
      .filter(col("tpep_dropoff_datetime").isNotNull)
      .filter(col("PULocationID").isNotNull)
      .filter(col("DOLocationID").isNotNull)
      .filter(col("trip_distance").isNotNull && col("trip_distance") >= 0)
      .filter(col("total_amount").isNotNull && col("total_amount") >= 0)
      // passenger_count : générique (autorise null, mais pas négatif)
      .filter(col("passenger_count").isNull || col("passenger_count") >= 0)
  }

  private def requireEnv(name: String): String =
    sys.env.getOrElse(name, throw new RuntimeException(s"Missing env var: $name"))

  def main(args: Array[String]): Unit = {
    val params = parseArgs(args)

    val spark = SparkSession.builder()
      .appName(s"Ex02 - Data Ingestion yellow ${params.year}-${params.month}")
      .getOrCreate()

    try {
      val rawPath     = s"s3a://nyc-raw/yellow/${params.year}/${params.month}/"
      val interimPath = s"s3a://nyc-interim/yellow/${params.year}/${params.month}/"

      val (startStr, endStr) = monthWindow(params.year, params.month)
      println(s"[INFO] Month window: [$startStr, $endStr)")

      println(s"[INFO] Read raw: $rawPath")
      val rawDf = spark.read.parquet(rawPath)
      println(s"[INFO] Raw count (before clean): ${rawDf.count()}")

      val cleanDf = cleanYellow(rawDf, params.year, params.month).persist()
      println(s"[INFO] Clean count (after month filter + generic checks): ${cleanDf.count()}")

      // BRANCH 1 - Data Lake (ML-ready)
      println(s"[INFO] Write interim parquet: $interimPath")
      cleanDf.write
        .mode("overwrite")
        .parquet(interimPath)
      println("[INFO] Branch 1 done.")

      // BRANCH 2 - PostgreSQL (staging)
      if (params.enableDw) {
        val pgHost = sys.env.getOrElse("POSTGRES_HOST", "postgres")
        val pgPort = sys.env.getOrElse("POSTGRES_PORT", "5432")
        val pgDb   = requireEnv("POSTGRES_DB")
        val pgUser = requireEnv("POSTGRES_USER")
        val pgPwd  = requireEnv("POSTGRES_PASSWORD")

        val jdbcUrl = s"jdbc:postgresql://$pgHost:$pgPort/$pgDb"

        val dwDf = cleanDf.select(
          col("VendorID").as("vendorid"),
          col("tpep_pickup_datetime"),
          col("tpep_dropoff_datetime"),
          col("passenger_count"),
          col("trip_distance"),
          col("RatecodeID").as("ratecodeid"),
          col("store_and_fwd_flag"),
          col("PULocationID").as("pulocationid"),
          col("DOLocationID").as("dolocationid"),
          col("payment_type"),
          col("fare_amount"),
          col("extra"),
          col("mta_tax"),
          col("tip_amount"),
          col("tolls_amount"),
          col("improvement_surcharge"),
          col("total_amount"),
          col("congestion_surcharge"),
          col("airport_fee")
        )

        println(s"[INFO] Write JDBC to staging table: ${params.dwTable}")
        dwDf.write
          .format("jdbc")
          .option("url", jdbcUrl)
          .option("dbtable", params.dwTable)
          .option("user", pgUser)
          .option("password", pgPwd)
          .option("driver", "org.postgresql.Driver")
          // staging mono-mois = overwrite + truncate => OK
          .mode("overwrite")
          .option("truncate", "true")
          .save()

        println("[INFO] Branch 2 done.")
      } else {
        println("[INFO] Branch 2 skipped (enableDw=false).")
      }

      cleanDf.unpersist()
      println("[INFO] EX02 finished OK.")
    } finally {
      spark.stop()
    }
  }
}
