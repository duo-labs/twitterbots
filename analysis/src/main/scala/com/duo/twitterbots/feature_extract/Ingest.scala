package com.duo.twitterbots.feature_extract

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

object Ingest extends Serializable {
  /**
    * Outputs a DataFrame where each row is a new tweet
    *
    * @param df input DataFrame containing tweets as an array
    * @return DataFrame with each tweet as a row
    */
  def explodeTweets(df: DataFrame): DataFrame = {
    df
      .withColumn("tweet", explode(col("tweets")))
      .drop(col("tweets"))
      .select("tweet.*")
  }

  /**
    * Selects the appropriate columns and renames them
    *
    * @param df input DataFrame
    * @return a DataFrame with the proper columns
    */
  def getColumns(df: DataFrame): DataFrame = {
    /** Going to concede that there is probably a more idiomatic Spark way to do this. */
    df
      .select(
        col("created_at").alias("tweet_created_at"),
        col("id").alias("tweet_id"),
        col("text").alias("tweet_text"),
        col("source").alias("tweet_source"),
        col("truncated"),
        col("in_reply_to_status_id"),
        col("in_reply_to_user_id"),
        col("in_reply_to_screen_name"),
        col("contributors"),
        col("coordinates.coordinates"),
        col("user.id").alias("user_id"),
        col("user.name").alias("user_name"),
        col("user.screen_name").alias("user_screen_name"),
        col("user.followers_count").alias("user_follower_count"),
        col("user.url").alias("user_url"),
        col("user.location").alias("user_location"),
        col("user.description").alias("user_description"),
        col("user.verified").alias("user_verified"),
        col("user.followers_count").alias("user_followers_count"),
        col("user.favourites_count").alias("user_favorites_count"),
        col("user.friends_count").alias("user_friends_count"),
        col("user.listed_count").alias("user_listed_count"),
        col("user.statuses_count").alias("user_status_count"),
        col("user.created_at").alias("user_created_at"),
        col("user.utc_offset").alias("user_utc_offset"),
        col("user.geo_enabled").alias("user_geo_enabled"),
        col("user.lang").alias("user_language"),
        col("user.profile_image_url_https").alias("user_profile_image_url_https"),
        col("place"),
        col("is_quote_status"),
        col("quoted_status_id"),
        col("quoted_status.user.id").alias("quoted_status_user_id"),
        col("quoted_status.user.screen_name").alias("quoted_status_user_screen_name"),
        col("quoted_status.created_at").alias("quoted_status_tweet_created_at"),
        col("quoted_status.user.lang").alias("quoted_status_user_language"),
        col("retweeted_status.id").alias("retweeted_status_id"),
        col("retweeted_status.user.id").alias("retweeted_status_user_id"),
        col("retweeted_status.user.screen_name").alias("retweeted_status_user_screen_name"),
        col("retweeted_status.created_at").alias("retweeted_status_tweet_created_at"),
        col("retweeted_status.user.lang").alias("retweeted_status_user_language"),
        col("retweeted_status.entities").alias("retweeeted_status_entities"),
        col("in_reply_to_status.created_at").alias("replied_status_tweet_created_at"),
        col("retweet_count").alias("tweet_retweet_count"),
        col("favorite_count").alias("tweet_favorite_count"),
        col("entities"),
        col("possibly_sensitive"),
        col("lang").alias("tweet_language"),
        col("extended_entities")
      )
  }

  /**
    * Ensures that the training data is formatted properly
    *
    * @param df input DataFrame containing Cresci data
    * @return corrected DataFrame
    */
  def formatTrainingDataTweets(df: DataFrame): DataFrame = {
    val stringify: UserDefinedFunction = udf((payload: Array[Byte]) => new String(payload))
    df
      .filter(col("created_at").isNotNull && col("text").isNotNull && col("source").isNotNull)
      .withColumn("user_id", col("user_id").cast(LongType))
      .withColumn("tweet_created_at", stringify(col("created_at")))
      .withColumn("tweet_text", stringify(col("text")))
      .withColumn("tweet_source", stringify(col("source")))
      .withColumnRenamed("retweet_count", "tweet_retweet_count")
      .withColumnRenamed("favorite_count", "tweet_favorite_count")
      .withColumnRenamed("num_mentions", "num_users_mentioned")
  }
}