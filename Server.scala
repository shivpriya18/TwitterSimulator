

import akka.actor.ActorSystem
import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit
import java.net.InetAddress
import com.typesafe.config.ConfigFactory
import akka.remote.RemoteScope
import scala.collection.mutable.HashSet

case class resetCounts()
case class outputStats()
case class requestHomeLine()
case class reqUserTimeline()
case class print()
case class terminate()

object Server {

  var noOfusers = 50000

  var monitor: ActorRef = null
  var client: ActorRef = null

  def main(args: Array[String]): Unit = {
    noOfusers = args(0).toInt

    val hostname = InetAddress.getLocalHost.getHostName
    val config = ConfigFactory.parseString(
      """ 
     akka{ 
    		actor{ 
    			provider = "akka.remote.RemoteActorRefProvider" 
    		} 
    		remote{ 
                enabled-transports = ["akka.remote.netty.tcp"] 
            netty.tcp{ 
    			hostname = "127.0.0.1"
    			port = 5150 
    		} 
      }      
    }""")

    val system = ActorSystem("TwitterServer", ConfigFactory.load(config))
    val server = system.actorOf(Props(new server), "server")
    server ! "init"
  }

  class server extends Actor {

    var tweetId = 0
    var noOfHandlers = noOfusers

    for (i <- 1 to noOfHandlers) {
      val handler = context.actorOf(Props(new Handle), i.toString)
    }

    def generateTweetId(): Int = {
      tweetId = tweetId + 1
      return (tweetId)
    }

    // send incoming traffic to monitor and to route it to handler
    def receive = {

      case "init" =>
        println("Initializing Server")
        // FOLLOWING STATISTICS

        // 50 % follow 2 or more 
        var mostUsersFollowingCount = (noOfusers * 0.5).toInt

        // 10 % follow 50 or more
        var avgUserFollowingCount = (noOfusers * 0.1).toInt

        // 1% follow more than 500 people 
        var avg2UserFollowingCount = (noOfusers * 0.01).toInt

        // 2 accounts follow more than 524,000
        var exceptionalUserFollowingCount = 2
       // var exceptionalFC = 524000

        var userSet = new HashSet[Int]()
        for (i <- 1 to noOfusers) {
          userSet.add(i)
        }

        // to update followed by list of users
        var count = 0
        for ((user) <- userSet) {

          count = count + 1
          var followedByIDList = new ArrayBuffer[Int]()

          // add largeFC users to followed by list
          if (count <= exceptionalUserFollowingCount) {
            var exceptionalFC = (0.03 * noOfusers).toInt
            for (i <- 1 to exceptionalFC) {
              var fUSer = util.Random.nextInt(noOfusers - 1) + 1
              while (followedByIDList.contains(fUSer) || fUSer == user) {
                fUSer = util.Random.nextInt(noOfusers - 1) + 1
              }
              followedByIDList.append(fUSer)
            }
          } else if (count < exceptionalUserFollowingCount + avg2UserFollowingCount) {
            var avg2FC = util.Random.nextInt(1000) + 500

            for (i <- 1 to avg2FC) {
              var fUSer = util.Random.nextInt(noOfusers - 1) + 1
              while (followedByIDList.contains(fUSer) || fUSer == user) {
                fUSer = util.Random.nextInt(noOfusers - 1) + 1
              }
              followedByIDList.append(fUSer)
            }
          } else if (count <= avgUserFollowingCount + exceptionalUserFollowingCount + avg2UserFollowingCount) {
            var avgFC = util.Random.nextInt(450) + 50
            for (i <- 1 to avgFC) {
              var fUSer = util.Random.nextInt(noOfusers - 1) + 1
              while (followedByIDList.contains(fUSer) || fUSer == user) {
                fUSer = util.Random.nextInt(noOfusers - 1) + 1
              }
              followedByIDList.append(fUSer)
            }
          } else if (count < avgUserFollowingCount + exceptionalUserFollowingCount + avg2UserFollowingCount + mostUsersFollowingCount) {
            var mostFC = util.Random.nextInt(48) + 2
            for (i <- 1 to mostFC) {
              var fUSer = util.Random.nextInt(noOfusers - 1) + 1
              while (followedByIDList.contains(fUSer) || fUSer == user) {
                fUSer = util.Random.nextInt(noOfusers - 1) + 1
              }
              followedByIDList.append(fUSer)
            }
          }

          var userinfo = new userinfo()
          userinfo.id = user
          userinfo.followedbyList = followedByIDList
          userinfo.homeTweets = new ArrayBuffer[Tweets]()
          userinfo.userTweets = new ArrayBuffer[Tweets]()

          context.actorSelection("/user/server/" + user) ! (userinfo)
          // println("Count " + count + " USer " + user + " followedBy " + userinfo.followedbyList.length)
        }
        println("Initialization Done")
        monitor = context.actorOf(Props(new Monitor), "monitorserver")

      /*
 *         
      
        var userSet = new HashSet[Int]()
        for (i <- 1 to noOfusers) {
          userSet.add(i)
        }
 * 
        // to update followed by list of users
        var count = 0
        for ((user) <- userSet) {

          count = count + 1
          var followedByIDList = new ArrayBuffer[Int]()
          // add largeFC users to followed by list
          if (count <= largeFollowingUserCount) {

            for (i <- 1 to largeFC) {
              var fUSer = util.Random.nextInt(noOfusers - 1) + 1
              while (followedByIDList.contains(fUSer) || fUSer == user) {
                fUSer = util.Random.nextInt(noOfusers - 1) + 1
              }
              followedByIDList.append(fUSer)
            }
          } else if (count < largeFollowingUserCount + mostUsersFollowingCount) {

            for (i <- 1 to mostFC) {
              var fUSer = util.Random.nextInt(noOfusers - 1) + 1
              while (followedByIDList.contains(fUSer) || fUSer == user) {
                fUSer = util.Random.nextInt(noOfusers - 1) + 1
              }
              followedByIDList.append(fUSer)
            }
          } else if (count < largeFollowingUserCount + mostUsersFollowingCount + avgUserFollowingCount) {

            for (i <- 1 to avgFC) {
              var fUSer = util.Random.nextInt(noOfusers - 1) + 1
              while (followedByIDList.contains(fUSer) || fUSer == user) {
                fUSer = util.Random.nextInt(noOfusers - 1) + 1
              }
              followedByIDList.append(fUSer)
            }
          } else if (count < largeFollowingUserCount + mostUsersFollowingCount + avgUserFollowingCount + avg2UserFollowingCount) {

            for (i <- 1 to avg2FC) {
              var fUSer = util.Random.nextInt(noOfusers - 1) + 1
              while (followedByIDList.contains(fUSer) || fUSer == user) {
                fUSer = util.Random.nextInt(noOfusers - 1) + 1
              }
              followedByIDList.append(fUSer)
            }
          } else if (count < largeFollowingUserCount + mostUsersFollowingCount + avgUserFollowingCount + avg2UserFollowingCount + exceptionalUserFollowingCount) {
            if (exceptionalFC > noOfusers) {
              exceptionalFC = noOfusers
            }

            for (i <- 1 to avg2FC) {
              var fUSer = util.Random.nextInt(noOfusers - 1) + 1
              while (followedByIDList.contains(fUSer) || fUSer == user) {
                fUSer = util.Random.nextInt(noOfusers - 1) + 1
              }
              followedByIDList.append(fUSer)
            }
          }*/

      case (userId: Int, action: String) =>
        client = sender

        //  println("Recieved req "+ " userID " + userId + " action " + action)
        if (action == "getUserTimeline") {
          context.actorSelection("/user/server/" + userId.toString) ! reqUserTimeline()
          monitor ! reqUserTimeline()
        } else {
          context.actorSelection("/user/server/" + userId.toString) ! requestHomeLine()
          monitor ! requestHomeLine()
        }

      case (tweet: Tweets) =>
        client = sender
        tweet.Id = generateTweetId()
        // println(" TweetId " + tweet.Id +  " Tweet User" + tweet.sourceUser  + " content " +  tweet.text + " time " + tweet.createdAt )
        context.actorSelection("/user/server/" + tweet.sourceUser.toString) ! (tweet)
        monitor ! tweet

      case (timeline: ArrayBuffer[Tweets], id: Int, value: String) =>
        client ! (timeline, id, value)
    }
  }

  class Handle extends Actor {

    var id = 0
    var uInfo = new userinfo()

    def receive = {

      case (userInfo: userinfo) =>
        this.uInfo = userInfo
        this.id = userInfo.id

      //  println("Init user " + uInfo.id + " followCount " + uInfo.followedbyList.length  + 
      //		" homeTw " + uInfo.homeTweets + " userTw " + uInfo.userTweets )

      case requestHomeLine() =>
        // sending home tweets 

        var recentTweets = ArrayBuffer[Tweets]()
        var size = uInfo.homeTweets.length
        var count = 0
        if (size > 20) {
          while (count < 20) {
            recentTweets.append(uInfo.homeTweets.apply(size - 1))
            size = size - 1
            count = count + 1
          }
        } else {
          recentTweets = uInfo.homeTweets
        }
        sender ! (recentTweets, id, "homeTimeline")
        monitor ! outputStats()

      case reqUserTimeline() =>
        // sending user tweets 
        var recentTweets = ArrayBuffer[Tweets]()
        var size = uInfo.userTweets.length
        var count = 0

        if (size > 20) {
          while (count < 20) {
            recentTweets.append(uInfo.userTweets.apply(size - 1))
            size = size - 1
            count = count + 1
          }
        } else {
          recentTweets = uInfo.userTweets
        }
        sender ! (recentTweets, id, "UserTimeline")
        monitor ! outputStats()

      case (tweet: Tweets) =>
        //  println("Adding tweet" + tweet.Id + " to timeline User " +  tweet.sourceUser + " followedbyList " + uInfo.followedbyList.length  )
        uInfo.homeTweets.append(tweet)
        uInfo.userTweets.append(tweet)

        // add this tweet to all the users following this user

        for (followedByUserId <- uInfo.followedbyList) {
          context.actorSelection("/user/server/" + followedByUserId.toString) ! (followedByUserId, tweet)
        }
        monitor ! outputStats()

      case (user: Int, tweet: Tweets) =>
        //this is tweeted by other,  add tweet to my home timeline
        //println("I am " + id + " tweeted by User " +  tweet.sourceUser + " TID " + tweet.Id )
        if (uInfo.homeTweets == null) {
          //   println("I am " + id + " tweeted by User " +  tweet.sourceUser + " TID " + tweet.Id )
        }
        uInfo.homeTweets.append(tweet)

      case ("print") =>
        var st = "User" + id + " homeline "
        for (tweet <- uInfo.homeTweets) {
          st = st + " " + tweet.Id + " "
        }
        st = st + " userline "
        for (tweet <- uInfo.userTweets) {
          st = st + " " + tweet.Id + " "
        }
        println(st)
    }
  }

  class Monitor extends Actor {
    var requestsHome = 0
    var requestsUser = 0
    var reqTweet = 0
    var outputstats = 0
    import context.dispatcher
    var alarm: akka.actor.Cancellable = context.system.scheduler.schedule(Duration.create(10000, TimeUnit.MILLISECONDS),
      Duration.create(10000, TimeUnit.MILLISECONDS), self, resetCounts())

    // var alarm4: akka.actor.Cancellable = context.system.scheduler.scheduleOnce(Duration.create(60000, TimeUnit.MILLISECONDS), self, terminate())
    def receive = {
      // Incoming traffic
      case requestHomeLine() =>
        requestsHome = requestsHome + 1

      case reqUserTimeline() =>
        requestsUser = requestsUser + 1

      case (tweet: Tweets) =>
        reqTweet = reqTweet + 1

      case resetCounts() =>
        println("outputstats " + outputstats + "requestsHome " + requestsHome + "\t" + " requestsUser " + requestsUser + "\t" + " reqTweet " + "\t" + reqTweet + "\t" + " input " + (requestsHome+requestsUser +reqTweet) )
        requestsHome = 0
        requestsUser = 0
        reqTweet = 0
        outputstats = 0

      case outputStats() =>
        outputstats = outputstats + 1

      case terminate() =>
        alarm.cancel
        // println("Terminate")
        for (i <- 1 to noOfusers) {
          context.actorSelection("/user/server/" + i.toString) ! "print"
        }

    }
  }

}


class Tweets extends Serializable {

  // 
	var Id:Int =  0
	// time
	var createdAt : Long= 0  
	var sourceUser = 0
    var text : String = "" 
  
  
}

class userinfo {
  
  var id = 0

   var followingList : ArrayBuffer[Int] = null
   
   var followedbyList : ArrayBuffer[Int] = null
  
   // for home timeline - all tweets
  var homeTweets : ArrayBuffer[Tweets] =  null
  
     // for mention timeline
   var mentionTweets : ArrayBuffer[Tweets] =  null
   
        // for user timeline - only my tweets
   var userTweets : ArrayBuffer[Tweets] =  null
    
   var reTweets : ArrayBuffer[Tweets] =  null
  
  var dmSent : ArrayBuffer[Tweets] =  null
  
   var dmRecieved : ArrayBuffer[Tweets] =  null 
  
}