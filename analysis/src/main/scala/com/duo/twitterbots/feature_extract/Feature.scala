package com.duo.twitterbots.feature_extract

import com.javadocmd.simplelatlng.util.LengthUnit
import com.javadocmd.simplelatlng.{LatLng, LatLngTool}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Dataset}

import scala.util.control.Breaks._

object FeatureUtilities extends Serializable {
  def getPreviousTweetInfo(columnName: String): Column = {
    val defaultVal: Any = if (columnName == "tweet_ts_long") 0L else Double.NaN
    val windowSpec = Window.partitionBy("user_id").orderBy("tweet_ts_long")
    val previousInfo = lag(col(columnName), 1, defaultValue = defaultVal).over(windowSpec)
    val finalColumName = s"previous_tweet_$columnName"
    previousInfo.as(finalColumName)
  }

  def calculateDistance(startingLat: Double, startingLong: Double, endingLat: Double, endingLong: Double): Double = {
    if (startingLat.isNaN || startingLong.isNaN || endingLat.isNaN || endingLong.isNaN) {
      Double.NaN
    } else {
      val startingLatLng = new LatLng(startingLat, startingLong)
      val endingLatLng = new LatLng(endingLat, endingLong)
      val distance = LatLngTool.distance(startingLatLng, endingLatLng, LengthUnit.MILE)
      distance
    }
  }

  def calculateUsernameEntropy(username: String): Double = {
    var entropy: Double = 0.0
    val usernameLength: Integer = username.length()
    val occ: Map[Char, Int] = username.groupBy(c => c).mapValues(str => str.length)

    for ((k, v) <- occ) {
      val p = v.toDouble / usernameLength.toDouble
      entropy -= p * math.log(p) / math.log(2)
    }

    entropy
  }

  def calculateNumbersInScreenName(username: String): Integer = {
    username.filter(f => Character.isDigit(f)).length
  }

  def calculateNumbersAtBeginning(username: String): Integer = {
    var num = 0
    breakable {
      for (i <- 0 until username.length) {
        if (Character.isDigit(username.charAt(i))) {
          num += 1
        } else {
          break
        }
      }
    }
    num
  }

  val calculateDistanceUDF: UserDefinedFunction = udf(calculateDistance(_: Double, _: Double, _: Double, _: Double): Double)
  val calculateUsernameEntropyUDF: UserDefinedFunction = udf(calculateUsernameEntropy(_: String): Double)
  val calculateNumbersInScreenNameUDF: UserDefinedFunction = udf(calculateNumbersInScreenName(_: String): Integer)
  val calculateNumbersAtBeginningUDF: UserDefinedFunction = udf(calculateNumbersAtBeginning(_: String): Integer)
}

abstract class Feature(val outputColumn: String,
                       dependencies: Seq[String]) extends Serializable {

  def transformFunc(data: Dataset[_]): DataFrame

  def transform(data: Dataset[_]): DataFrame = {
    if (checkDependencies(data)) {
      transformFunc(data)
    } else {
      data.toDF
    }
  }

  /**
    * Checks to see if the dependencies for a particular feature
    * are present in the Dataset
    *
    * @param data input Dataset
    * @return boolean of whether or not the Dataset contains the necessary columns
    */
  def checkDependencies(data: Dataset[_]): Boolean = {
    var hasDependencies = true
    if (!dependencies.toSet.subsetOf(data.columns.toSet)) {
      println(s"com.duo.twitterbots.feature_extract.Feature: $outputColumn is missing column: ${dependencies.toSet.diff(data.columns.toSet)}")
      hasDependencies = false
    }
    hasDependencies
  }
}

/**
  *
  *
  * @param outputColumn name of the output column
  * @param columnExp    column operations to produce expected column
  * @param dependencies Sequence of columns necessary for column operations
  */
class ColumnFeature(outputColumn: String,
                    columnExp: Column,
                    dependencies: Seq[String])
  extends Feature(outputColumn, dependencies) {
  override def transformFunc(data: Dataset[_]): DataFrame = data.withColumn(outputColumn, columnExp)
}

object AccountFeatures {
  val dumpTs = new ColumnFeature(
    "dump_ts",
    to_timestamp(regexp_extract(col("file_name"), ".accounts-(\\d{4}-\\d{2}-\\d{2}).", 1), "yyyy-MM-dd"),
    Seq("file_name")
  )

  val userID = new ColumnFeature(
    "user_id",
    col("id"),
    Seq("id")
  )

  val createdAtTs = new ColumnFeature(
    "created_at_ts",
    to_timestamp(col("created_at"), "EEE MMMMM dd HH:mm:ss Z yyyyy"),
    Seq("created_at")
  )

  val accountAgeDays = new ColumnFeature(
    "account_age_days",
    abs(datediff(col("dump_ts"), col("created_at_ts"))),
    Seq("dump_ts", "created_at_ts")
  )

  val lang = new ColumnFeature(
    "lang",
    col("lang"),
    Seq("lang")
  )

  val isProtected = new ColumnFeature(
    "is_protected",
    when(col("protected").isNull, 0).when(col("protected") === 0, 0).otherwise(1),
    Seq("protected")
  )

  val screenName = new ColumnFeature(
    "screen_name",
    col("screen_name"),
    Seq("screen_name")
  )
  val defaultProfile = new ColumnFeature(
    "is_default_profile",
    when(col("default_profile").isNull, 0).when(col("default_profile") === 0, 0).otherwise(1),
    Seq("default_profile")
  )

  val verified = new ColumnFeature(
    "is_verified",
    when(col("verified").isNull, 0).when(col("verified") === 0, 0).otherwise(1),
    Seq("verified")
  )

  val geoEnabled = new ColumnFeature(
    "is_geo_enabled",
    when(col("geo_enabled").isNull, 0).when(col("geo_enabled") === 0, 0).otherwise(1),
    Seq("geo_enabled")
  )

  val listedRate = new ColumnFeature(
    "listed_rate",
    col("listed_count") / col("account_age_days"),
    Seq("listed_count", "account_age_days")
  )

  val numFollowers = new ColumnFeature(
    "num_followers",
    col("followers_count"),
    Seq("followers_count")
  )

  val numFriends = new ColumnFeature(
    "num_friends",
    col("friends_count"),
    Seq("friends_count")
  )

  val numFavorites = new ColumnFeature(
    "num_favorites",
    col("favourites_count"),
    Seq("favourites_count")
  )

  val ratioFollowersFriends = new ColumnFeature(
    "ratio_followers_friends",
    col("followers_count") / col("friends_count"),
    Seq("friends_count", "followers_count")
  )

  val screenNameEntropy = new ColumnFeature(
    "screen_name_entropy",
    FeatureUtilities.calculateUsernameEntropyUDF(col("screen_name")),
    Seq("screen_name")
  )

  val numbersAtBeginning = new ColumnFeature(
    "numbers_at_beginning_of_screen_name",
    FeatureUtilities.calculateNumbersAtBeginningUDF(col("screen_name")),
    Seq("screen_name")
  )

  val numbersAtEnd = new ColumnFeature(
    "numbers_at_end_of_screen_name",
    FeatureUtilities.calculateNumbersAtBeginningUDF(reverse(col("screen_name"))),
    Seq("screen_name")
  )

  val favoriteRate = new ColumnFeature(
    "favorite_rate",
    col("favourites_count") / col("account_age_days"),
    Seq("favourites_count", "account_age_days")
  )

  val numTweets = new ColumnFeature(
    "num_tweets",
    col("statuses_count"),
    Seq("statuses_count")
  )

  val tweetRate = new ColumnFeature(
    "tweet_rate",
    col("statuses_count") / col("account_age_days"),
    Seq("statuses_count", "account_age_days")
  )

  val updatedAtTs = new ColumnFeature(
    "updated_at_ts",
    to_timestamp(col("updated"), "yyyy-MM-dd HH:mm:ss"),
    Seq("updated")
  )

  val accountAgeDays2 = new ColumnFeature(
    "account_age_days",
    abs(datediff(col("updated_at_ts"), col("created_at_ts"))),
    Seq("updated_at_ts", "created_at_ts")
  )

  val createdAtTs2 = new ColumnFeature(
    "created_at_ts",
    to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"),
    Seq("timestamp")
  )

  def trainFeatures: Seq[Feature] = Seq(userID, createdAtTs2, updatedAtTs, accountAgeDays2, lang, screenName, defaultProfile,
    verified, geoEnabled, numFollowers, numFriends, numFavorites, ratioFollowersFriends,
    screenNameEntropy, numbersAtBeginning, numbersAtEnd, favoriteRate, numTweets,
    tweetRate, isProtected, listedRate)

  def features: Seq[Feature] = Seq(dumpTs, userID, createdAtTs, accountAgeDays, lang, screenName, defaultProfile,
    verified, geoEnabled, numFollowers, numFriends, numFavorites, ratioFollowersFriends,
    screenNameEntropy, numbersAtBeginning, numbersAtEnd, favoriteRate, numTweets,
    tweetRate, isProtected, listedRate)

  def getAccountFeatures(inputDF: DataFrame, features: Seq[Feature]): DataFrame = {
    var tempAccountDF = inputDF

    for (feature <- features) {
      tempAccountDF = tempAccountDF.transform(feature.transform)
    }

    val accountFeatureCols = features.map(f => col(f.outputColumn))

    val finalAccountFeatures = tempAccountDF.select(accountFeatureCols: _*)

    finalAccountFeatures.dropDuplicates("user_id")
  }


}

object TweetFeatures {

  val tweetTs = new ColumnFeature(
    "tweet_ts",
    to_timestamp(col("tweet_created_at"), "EEE MMMMM dd HH:mm:ss Z yyyyy"),
    Seq("tweet_created_at")
  )

  val tweetTsLong = new ColumnFeature(
    "tweet_ts_long",
    unix_timestamp(col("tweet_ts")),
    Seq("tweet_ts")
  )

  val retweetTs = new ColumnFeature(
    "retweet_ts",
    to_timestamp(col("retweeted_status_tweet_created_at"), "EEE MMMMM dd HH:mm:ss Z yyyyy"),
    Seq("retweeted_status_tweet_created_at")
  )

  val retweetTsLong = new ColumnFeature(
    "retweet_ts_long",
    unix_timestamp(col("retweet_ts")),
    Seq("retweet_ts")
  )

  val replyTs = new com.duo.twitterbots.feature_extract.ColumnFeature(
    "reply_ts",
    to_timestamp(col("replied_status_tweet_created_at"), "EEE MMMMM dd HH:mm:ss Z yyyyy"),
    Seq("replied_status_tweet_created_at")
  )

  val replyTsLong = new ColumnFeature(
    "reply_ts_long",
    unix_timestamp(col("reply_ts")),
    Seq("reply_ts")
  )

  val timeToRetweet = new ColumnFeature(
    "time_to_retweet",
    col("tweet_ts_long") - col("retweet_ts_long"),
    Seq("tweet_ts_long", "retweet_ts_long")
  )

  val timeToReply = new ColumnFeature(
    "time_to_reply",
    col("tweet_ts_long") - col("reply_ts_long"),
    Seq("tweet_ts_long", "reply_ts_long")
  )

  val retweetIDDistance = new ColumnFeature(
    "retweet_id_distance",
    abs(col("user_id") - col("retweeted_status_user_id")),
    Seq("user_id", "retweeted_status_user_id")
  )

  val replyIDDistance = new ColumnFeature(
    "reply_id_distance",
    abs(col("user_id") - col("in_reply_to_user_id")),
    Seq("user_id", "in_reply_to_user_id")
  )

  val tweetHour = new ColumnFeature(
    "tweet_hour",
    hour(col("tweet_ts")),
    Seq("tweet_ts")
  )

  val tweetDate = new ColumnFeature(
    "tweet_date",
    to_date(col("tweet_ts")),
    Seq("tweet_ts")
  )

  val longitude = new ColumnFeature(
    "longitude",
    col("coordinates").getItem(0),
    Seq("coordinates")
  )

  val latitude = new ColumnFeature(
    "latitude",
    col("coordinates").getItem(1),
    Seq("coordinates")
  )

  val numHashtags = new ColumnFeature(
    "num_hashtags",
    size(col("entities.hashtags")),
    Seq("entities")
  )

  val numUrls = new ColumnFeature(
    "num_urls",
    size(col("entities.urls")),
    Seq("entities")
  )

  val numUsersMentioned = new ColumnFeature(
    "num_users_mentioned",
    size(col("entities.user_mentions")),
    Seq("entities")
  )

  val previousTweetLatitude = new ColumnFeature(
    "previous_tweet_latitude",
    FeatureUtilities.getPreviousTweetInfo("latitude"),
    Seq("latitude")
  )

  val previousTweetLongitude = new ColumnFeature(
    "previous_tweet_longitude",
    FeatureUtilities.getPreviousTweetInfo("longitude"),
    Seq("longitude")
  )

  val previousTweetTsLong = new ColumnFeature(
    "previous_tweet_ts_long",
    FeatureUtilities.getPreviousTweetInfo("tweet_ts_long"),
    Seq("tweet_ts_long")
  )

  val distFromPreviousTweet = new ColumnFeature(
    "dist_from_previous_tweet",
    FeatureUtilities.calculateDistanceUDF(col("latitude"), col("longitude"),
      col("previous_tweet_latitude"), col("previous_tweet_longitude")),
    Seq("latitude", "longitude", "previous_tweet_latitude", "previous_tweet_longitude")
  )

  val tweetSource = new ColumnFeature(
    "tweet_source",
    col("tweet_source"),
    Seq("tweet_source")
  )

  val timeBetweenTweets = new ColumnFeature(
    "time_from_previous_tweet",
    col("tweet_ts_long") - col("previous_tweet_ts_long"),
    Seq("tweet_ts_long", "previous_tweet_ts_long")
  )

  val numRetweets = new ColumnFeature(
    "num_retweets",
    col("tweet_retweet_count"),
    Seq("tweet_retweet_count")
  )

  val numFavorites = new ColumnFeature(
    "num_favorites",
    col("tweet_favorite_count"),
    Seq("tweet_favorite_count")
  )

  val ratioRetweetFavorites = new ColumnFeature(
    "num_favorites",
    col("tweet_retweet_count").cast(DoubleType) / col("tweet_favorite_count"),
    Seq("tweet_retweet_count", "tweet_favorite_count")
  )

  def computeGetTotalTweets(inputTweetData: DataFrame): DataFrame = {
    inputTweetData
      .select(col("user_id"))
      .groupBy(col("user_id"))
      .agg(count("*").alias("total_tweets"))
  }

  def computeAverage(columnName: String)(inputTweetData: DataFrame): DataFrame = {
    inputTweetData
      .filter(col(columnName).isNotNull)
      .select(col("user_id"), col(columnName))
      .groupBy(col("user_id"))
      .agg(mean(col(columnName)).alias(s"avg_$columnName"))
  }

  def computePercentageRetweets(inputTweetData: DataFrame): DataFrame = {
    inputTweetData
      .withColumn("is_retweet", when(col("retweeted_status_id").isNotNull, 1).otherwise(0))
      .select(col("user_id"), col("is_retweet"))
      .groupBy("user_id")
      .agg(count("*").alias("total_tweets"),
        sum(col("is_retweet")).alias("num_retweets"),
        mean(col("is_retweet")).alias("perc_retweets")
      )
      .select(col("user_id"), col("num_retweets"), col("perc_retweets"))
  }

  def computeDistinctAccountsRetweeted(inputTweetData: DataFrame): DataFrame = {
    inputTweetData
      .filter(col("retweeted_status_id").isNotNull)
      .groupBy("user_id")
      .agg(approx_count_distinct(col("retweeted_status_id")).alias("unique_users_retweeted"))
      .select(col("user_id"), col("unique_users_retweeted"))
  }

  def computeNumLanguages(inputTweetData: DataFrame): DataFrame = {
    inputTweetData
      .select(col("user_id"), col("tweet_language"))
      .groupBy("user_id")
      .agg(approx_count_distinct(col("tweet_language")).alias("num_languages"))
  }

  def computeHoursTweeted(inputTweetData: DataFrame): DataFrame = {
    inputTweetData
      .select(col("user_id"), col("tweet_date"), col("tweet_hour"))
      .groupBy(col("user_id"), col("tweet_date"))
      .agg(approx_count_distinct(col("tweet_hour")).alias("distinct_hours_tweeted"))
      .groupBy(col("user_id"))
      .agg(mean("distinct_hours_tweeted").alias("avg_distinct_hours_tweeted"))
  }

  def computeEntityFeatures(entityName: String)(inputTweetData: DataFrame): DataFrame = {
    inputTweetData
      .withColumn(s"has_$entityName", when(col(s"num_$entityName") > 0, 1).otherwise(0))
      .select(col("user_id"), col(s"has_$entityName"), col(s"num_$entityName"))
      .groupBy(col("user_id"))
      .agg(sum(col(s"num_$entityName")).alias(s"num_tweets_$entityName"),
        mean(col(s"num_$entityName")).alias(s"avg_num_${entityName}_in_tweets"),
        mean(col(s"has_$entityName")).alias(s"perc_tweets_has_$entityName")
      )
  }

  def computeDistinctDevices(inputTweetData: DataFrame): DataFrame = {
    inputTweetData
      .select(col("user_id"), col("tweet_source"))
      .groupBy("user_id")
      .agg(approx_count_distinct(col("tweet_source")).alias("num_sources"))
      .select(col("user_id"), col("num_sources"))
  }

  def computeDuplicateText(inputTweetData: DataFrame): DataFrame = {
    inputTweetData
      .filter(col("retweeted_status_id").isNull && col("in_reply_to_user_id").isNull)
      .select(col("user_id"), col("tweet_date"), col("tweet_text"))
      .groupBy(col("user_id"), col("tweet_date"), col("tweet_text"))
      .agg(count(col("tweet_text")).alias("count"))
      .filter(col("count") > 1)
      .select(col("user_id"), col("tweet_date"))
      .groupBy(col("user_id"), col("tweet_date"))
      .agg(count(col("tweet_date")).alias("duplicates"))
      .select(col("user_id"), col("duplicates"))
  }

  def getOldestTweetTime(inputTweetData: DataFrame): DataFrame = {
    inputTweetData
      .select(col("user_id"), col("tweet_ts_long"))
      .groupBy(col("user_id"), col("tweet_ts_long"))
      .agg(min(col("tweet_ts_long")).alias("oldest_ts_long"))
      .select(col("user_id"), col("oldest_ts_long"))
  }


  def columnFeatures: Seq[Feature] = Seq(tweetTs, tweetTsLong,
    replyIDDistance, tweetHour, tweetDate, previousTweetTsLong,
    timeBetweenTweets, tweetSource, numFavorites, numRetweets
  )

  def columnFeaturesUnknown: Seq[Feature] = columnFeatures ++ Seq(
    retweetIDDistance, replyTs, replyTsLong, timeToReply,
    retweetTs, retweetTsLong, timeToRetweet, numHashtags, numUrls, numUsersMentioned,
    latitude, longitude, previousTweetLatitude, previousTweetLongitude, distFromPreviousTweet)

  def transformFeatures: Seq[DataFrame => DataFrame] = {
    Seq(computePercentageRetweets(_), computeHoursTweeted(_), computeAverage("time_from_previous_tweet"),
      computeEntityFeatures("urls"), computeEntityFeatures("hashtags"),
      computeEntityFeatures("users_mentioned"),
      computeAverage("reply_id_distance"),
      computeGetTotalTweets(_), computeDuplicateText(_),
      getOldestTweetTime(_), computeAverage("previous_tweet_ts_long"),
      computeDistinctAccountsRetweeted(_)
    )
  }

  def transformFeaturesUnknown: Seq[DataFrame => DataFrame] = {
    Seq(computePercentageRetweets(_), computeHoursTweeted(_), computeNumLanguages(_),
      computeEntityFeatures("urls"), computeEntityFeatures("hashtags"),
      computeEntityFeatures("users_mentioned"),
      computeAverage("reply_id_distance"), computeAverage("retweet_id_distance"),
      computeAverage("time_to_retweet"), computeAverage("dist_from_previous_tweet"),
      computeGetTotalTweets(_), computeDuplicateText(_),
      getOldestTweetTime(_), computeAverage("previous_tweet_ts_long"),
      computeDistinctAccountsRetweeted(_), computeAverage("retweet_ts_long"),
      computeAverage("time_to_reply"))
  }

  def transformColumns: Seq[Column] = Seq(col("num_retweets"), col("perc_retweets"),
    col("num_tweets_urls"), col("avg_num_urls_in_tweets"), col("perc_tweets_has_urls"),
    col("num_tweets_users_mentioned"), col("num_tweets_users_mentioned"),
    col("perc_tweets_has_users_mentioned"), col("num_tweets_hashtags"),
    col("avg_num_hashtags_in_tweets"), col("perc_tweets_has_hashtags"),
    col("avg_distinct_hours_tweeted"), col("avg_time_to_retweet"),
    col("avg_reply_id_distance"), col("num_sources"), col("total_tweets"),
    col("duplicates"), col("oldest_ts_long"), col("unique_users_retweeted"),
    col("average_retweet_ts_long")
  )

  def transformColumnsUnknown: Seq[Column] = transformColumns ++ Seq(col("num_languages"),
    col("avg_time_to_reply"))

  /**
    * Assembles the tweet data and combines it with the account data
    *
    * @param tweetsDf  DataFrame containing the raw tweet data
    * @param accountDF DataFrame containing the feature-extracted account data
    * @param unknown   Boolean of whether this is an unknown or training set
    * @return DataFrame containing both Account and Tweet-related Features
    */
  def getFeatures(tweetsDf: DataFrame, accountDF: DataFrame, unknown: Boolean = false): DataFrame = {
    var tempTweetsDF = tweetsDf

    val tweetColumnFeatures = if (unknown) TweetFeatures.columnFeaturesUnknown else TweetFeatures.columnFeatures
    for (feature <- tweetColumnFeatures) {
      tempTweetsDF = tempTweetsDF.transform(feature.transform)
    }

    tempTweetsDF.persist.count

    var tempJoinDF = accountDF

    val tweetTransformFeatures = if (unknown) TweetFeatures.transformFeaturesUnknown else TweetFeatures.transformFeatures
    for (feature <- tweetTransformFeatures) {
      val transformDF = tempTweetsDF.transform(feature)
      tempJoinDF = tempJoinDF.join(transformDF, Seq("user_id"), "left_outer")
    }

    val featuresDF = tempJoinDF.dropDuplicates(Seq("user_id"))
    featuresDF
  }

}