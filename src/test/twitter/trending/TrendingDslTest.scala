// TODO Licenses go here
package twitter.trending

import org.joda.time.DateTime
import org.scalatest.{FlatSpec, Matchers}

class TrendingDslTest extends FlatSpec with Matchers {

  trait WithUnsortedTweets {
    val emtpySeq = Seq.empty[(String, Int)]
    val upperTweet: (String, Int) = ("tweetUpper", 4)
    val bottomTweet: (String, Int) = ("bottonUpper", 1)
    val almostBottomTweet: (String, Int) = ("bottonUpper", 2)
    val middleTweet: (String, Int) = ("tweetMiddle", 3)

    val unsort = Seq(middleTweet,
      bottomTweet,
      middleTweet,
      middleTweet,
      middleTweet,
      middleTweet,
      middleTweet,
      upperTweet,
      almostBottomTweet,
      middleTweet,
      middleTweet)
  }
  trait WithTweets {
    val dt1 = DateTime.now
    val dt2 = dt1.plusSeconds(1)
    val dt3 = dt1.plusSeconds(2)
    val hash1 = "Cristiano"
    val hash2 = "LukeSkyWalker"
    val hash3 = "bieber"
    val tweet1 = (dt1, hash1)
    val tweet2 = (dt1, hash2)
    val tweet3 = (dt1, hash2)
    val tweet4 = (dt2, hash1)
    val tweet5 = (dt3, hash3)
    val tweet6 = (dt2, hash1)
    val tweets = Seq(tweet1, tweet2, tweet3, tweet4, tweet5, tweet6)
    val expectedResult = Seq((hash1, 3), (hash2, 2), (hash3, 1))
  }


  "SortedFunction" should "sort the tweets list " in new WithUnsortedTweets {

    import com.spotahome.twitter.trending.TrendingDsl._
    val trendingTopic = unsort.takeTrending()
    trendingTopic.size should be (10)
    trendingTopic.head should be (upperTweet)
    trendingTopic.last should be (almostBottomTweet)
  }

  "CalculationFunction" should "calculate the trending topic " in new WithTweets  {

    import com.spotahome.twitter.trending.TrendingDsl._
    val trendingTopic = tweets.trendingTopic
    trendingTopic should be (expectedResult)
  }

}