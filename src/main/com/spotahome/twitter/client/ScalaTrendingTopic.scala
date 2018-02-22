package com.spotahome.twitter.client

import org.joda.time.{DateTime, Seconds}
import twitter4j.conf.ConfigurationBuilder
import twitter4j._

import scala.collection.mutable
import scala.io.Source

object ScalaTrendingTopic {

  def main(args: Array[String]) {

    val Array(filename) = args

    val Array(accessTokenString, secretToken, consumerKey, consumerSecret) =
      Source.fromFile(filename).getLines.toArray.flatMap(_.split(","))

    val cb = new ConfigurationBuilder()
    cb.setDebugEnabled(true)
      .setOAuthConsumerKey(consumerKey)
      .setOAuthConsumerSecret(consumerSecret)
      .setOAuthAccessToken(accessTokenString)
      .setOAuthAccessTokenSecret(secretToken)

    val listener: StatusListener = new StatusListener() {
      val trending: mutable.Queue[(DateTime, String)] = mutable.Queue.empty[(DateTime, String)]
      val trendings: mutable.Queue[Seq[(String, Int)]] = mutable.Queue.empty[Seq[(String, Int)]]
      def onStatus(status: Status): Unit = {
        val initTime = if(trending.headOption.isDefined)trending.head._1 else DateTime.now()
        if(Seconds.secondsBetween(initTime, new DateTime(status.getCreatedAt)).getSeconds > 10){
          val tweetsToCalculate = trending.dequeueAll{case (date, _) =>
            date.isBefore(initTime.plusSeconds(10))}
          val oldTrending = trendings.dequeueFirst(p => true).getOrElse(Seq.empty[(String, Int)])
          import com.spotahome.twitter.trending.TrendingDsl._
          val trendingTopic = tweetsToCalculate.trendingTopic
          trendingTopic.printTrendingTopic(initTime,
            initTime.plusSeconds(10),
            oldTrending)
          trendings.enqueue(trendingTopic)
        } else trending.enqueue(status.getHashtagEntities.map(hash =>
          (new DateTime(status.getCreatedAt), hash.getText)) : _* )
      }
      override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice): Unit = {}

      override def onScrubGeo(userId: Long, upToStatusId: Long): Unit = {}

      override def onTrackLimitationNotice(numberOfLimitedStatuses: Int): Unit = {}

      override def onStallWarning(warning: StallWarning): Unit = {}

      override def onException(ex: Exception): Unit = {}
    }

    val twitterStream: TwitterStream = new TwitterStreamFactory(cb.build()).getInstance()

    val tweetFilterQuery = new FilterQuery

    tweetFilterQuery.track(Array[String]("justin bieber","star wars","real madrid"): _*)

    twitterStream.addListener(listener)

    twitterStream.filter(tweetFilterQuery)

    twitterStream.sample
  }
}
