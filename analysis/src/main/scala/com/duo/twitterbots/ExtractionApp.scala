import java.io.File

import com.moandjiezana.toml.Toml
import org.apache.spark.sql.SparkSession

import com.duo.twitterbots.feature_extract.FeatureExtract


object ExtractionApp extends App {
  private def readOptions(optionsFile: String = System.getProperty("user.home") + "/twitter-bots.toml"): Toml = {
    new Toml().read(new File(optionsFile))
  }

  private def setupSpark(options: Toml): SparkSession = {
    val spark = SparkSession.builder()
      .appName("feature-extraction")

    val awsAccessKeyId = options.getString("aws_access_key_id", "")
    val awsSecretAccessKey = options.getString("aws_secret_access_key", "")

    if (awsAccessKeyId != "") {
      System.setProperty("aws.accessKeyId", awsAccessKeyId)
      spark.config("spark.hadoop.fs.s3a.access.key", awsAccessKeyId)
    }

    if (awsSecretAccessKey != "") {
      System.setProperty("aws.secretKey", awsSecretAccessKey)
      spark.config("spark.hadoop.fs.s3a.secret.key", awsSecretAccessKey)
    }

    if (options.getBoolean("local", false)) {
      spark.master("local[*]")
    }


    spark.getOrCreate()
  }

  val tomlOptions = readOptions()
  val spark = setupSpark(tomlOptions)
  FeatureExtract.run(spark, args.toSeq)
  spark.stop()
}

