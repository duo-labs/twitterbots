package com.duo.twitterbots.feature_extract

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.rogach.scallop.{ScallopConf, ScallopOption}

object FeatureExtract {

  class Arguments(args: Seq[String]) extends ScallopConf(args) {
    val accountData:ScallopOption[String] = opt[String](required = true, descr = "Twitter account data")
    val tweetData:ScallopOption[String] = opt[String](required = true, descr = "Tweet data")
    val destination:ScallopOption[String] = opt[String](required = true,
      descr = "Destination for extracted data")
    val extractionType:ScallopOption[String] = opt[String](required = true,
      descr = "Extracting Cresci data (training) or outputs from other scripts (unknown)")

    banner(
      """Performs feature extraction on twitter account and tweet data and saves data to local file system or S3.
        |
        |Usage: spark-submit --class ExtractionApp twitter-bots.jar --account-data <account_data_path> --tweet-data <tweet_data_path>
        |       --destination <output_path> --extraction-type [unknown|training]
      """.stripMargin
    )

    verify()
  }

  def run(spark: SparkSession, args: Seq[String]): Unit = {

    val arguments = new Arguments(args)

    val accountDataPath = arguments.accountData()
    val tweetDataPath = arguments.tweetData()
    val destination = arguments.destination()
    val extractionType = arguments.extractionType()


    val accountData = {
      if (extractionType == "training") {
          spark
            .read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(accountDataPath)
      } else {
        spark
          .read
          .json(accountDataPath)
          .withColumn("file_name", input_file_name())
          .repartition()
      }
    }

    val tweetData = {
      if (extractionType == "training") {
        val rawTweetData = spark
          .read
          .parquet(tweetDataPath)

        val formattedTweets = Ingest.formatTrainingDataTweets(rawTweetData)
        formattedTweets
      } else {
        val rawTweetData = spark
          .read
          .json(tweetDataPath)
          .withColumn("file_name", input_file_name())
          .repartition()

        val explodedTweets = Ingest.explodeTweets(rawTweetData)
        val ingestedTweets = Ingest.getColumns(explodedTweets)
        ingestedTweets
      }

    }


    val featureExtractor = new FeatureExtract(accountData, tweetData, extractionType, destination)
    featureExtractor.extract()
  }
}

/**
  * @param accountData Dataset of account data
  * @param tweetData DataFrame of tweet data
  * @param extractionType [training, unknown] training is the type for the Cresci dataset and unknown should be used
  *                       when trying to extract features from the outputs our tweet collection scripts
  * @param destination path where feature artifacts will be stored
  */
class FeatureExtract(accountData: Dataset[_],
                     tweetData: DataFrame,
                     extractionType: String,
                     destination: String) extends Serializable {

  /**
    * Function that encapsulates all of the feature extraction
    * and writes the data to the specified location
    */
  def extract():Unit = {

    val accountFeatures = if (extractionType == "training") AccountFeatures.trainFeatures else AccountFeatures.features
    val extractedAccountData = AccountFeatures
      .getAccountFeatures(accountData.toDF, accountFeatures)
      .drop(col("updated_at_ts"))
      .na
      .fill(0.0, Seq("favorite_rate", "tweet_rate", "ratio_followers_friends", "listed_rate"))

    val unknown = if (extractionType != "unknown") false else true
    val allData = TweetFeatures.getFeatures(tweetData, extractedAccountData, unknown)

    allData
      .write
      .mode("overwrite")
      .parquet(destination)

  }
}
