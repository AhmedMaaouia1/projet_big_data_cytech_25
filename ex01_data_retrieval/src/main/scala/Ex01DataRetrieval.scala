import java.io.File
import java.net.URL
import java.nio.file.{Files, Paths, StandardCopyOption}

import org.apache.spark.sql.SparkSession

object Ex01DataRetrieval {

  final case class Params(
    year: String = "2023",
    month: String = "01"
  )

  private def parseArgs(args: Array[String]): Params = {
    def next(i: Int): String = if (i + 1 < args.length) args(i + 1) else ""
    var p = Params()
    var i = 0
    while (i < args.length) {
      args(i) match {
        case "--year"  => p = p.copy(year = next(i)); i += 2
        case "--month" => p = p.copy(month = next(i)); i += 2
        case other =>
          System.err.println(s"[WARN] Unknown arg: $other")
          i += 1
      }
    }
    p
  }

  def main(args: Array[String]): Unit = {

    val params = parseArgs(args)

    // ------------------------------------------------------------------
    // 1. Spark Session (PAS de .master("local[*]"))
    // ------------------------------------------------------------------
    val spark = SparkSession.builder()
      .appName(s"Ex01 - NYC Taxi Data Retrieval ${params.year}-${params.month}")
      .getOrCreate()

    // ------------------------------------------------------------------
    // 2. Paramètres du dataset
    // ------------------------------------------------------------------
    val fileName = s"yellow_tripdata_${params.year}-${params.month}.parquet"

    val sourceUrl =
      s"https://d37ci6vzurychx.cloudfront.net/trip-data/$fileName"

    // Local Raw Zone
    val localDir =
      s"/opt/data/raw/yellow/${params.year}/${params.month}"

    val localFilePath =
      s"$localDir/$fileName"

    // MinIO (S3A)
    val s3TargetPath =
      s"s3a://nyc-raw/yellow/${params.year}/${params.month}/"

    // ------------------------------------------------------------------
    // 3. Téléchargement du fichier (si absent)
    // ------------------------------------------------------------------
    val localFile = new File(localFilePath)

    if (!localFile.exists()) {
      println(s"[INFO] Downloading NYC Taxi file from $sourceUrl")

      Files.createDirectories(Paths.get(localDir))

      val in = new URL(sourceUrl).openStream()
      Files.copy(in, Paths.get(localFilePath), StandardCopyOption.REPLACE_EXISTING)
      in.close()

      println(s"[INFO] File downloaded to $localFilePath")
    } else {
      println(s"[INFO] File already exists locally, skipping download")
    }

    // ------------------------------------------------------------------
    // 4. Lecture Spark du parquet local
    // ------------------------------------------------------------------
    val df = spark.read.parquet(localFilePath)
    println(s"[INFO] Number of records: ${df.count()}")

    // ------------------------------------------------------------------
    // 5. Upload vers MinIO (S3A)
    // Idempotence mensuelle : overwrite sur year/month
    // ------------------------------------------------------------------
    df.write
      .mode("overwrite")
      .parquet(s3TargetPath)

    println(s"[INFO] File uploaded to $s3TargetPath")

    spark.stop()
  }
}
