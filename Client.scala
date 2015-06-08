import akka.actor.ActorSystem
import akka.actor.Props
import scala.collection.mutable.ArrayBuffer
import akka.actor.Actor
import akka.actor.ActorRef
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit
import java.net.InetAddress
import com.typesafe.config.ConfigFactory
import scala.collection.mutable.HashSet

case class init(userId: Int, type1: Boolean, type2: Boolean, type3: Boolean, type4: Boolean)
case class doSomething(action: String)
case class setScheduler()
case class requestHomeLine(userId: Int)
case class reqUserTimeline(userId: Int)
case class outTraffic()
case class resetcount()
case class terminate()

object Client {

  var noOfusers = 50000

  var countOfRequests = 0

  var monitor: ActorRef = null

  var server: ActorRef = null
  var serverIp = "127.0.0.1"

  def main(args: Array[String]): Unit = {
       noOfusers = args(0).toInt
       println("Users " + noOfusers)
       
      serverIp = args(1).toString()

    //val hostname = InetAddress.getLocalHost.getHostName
    val config = ConfigFactory.parseString(
      """akka{
		  		actor{
		  			provider = "akka.remote.RemoteActorRefProvider"
		  		}
		  		remote{
                   enabled-transports = ["akka.remote.netty.tcp"]
		  			netty.tcp{
						hostname = "127.0.0.1"
						port = 5000
					}
				}     
    	}""")

    // TRAFFIC STATISTICS
    // top 15 % of users generate most of the traffic
    val tweetsMostUser = (.15 * noOfusers).toInt

    val moderateTweetUSers = (.20 * noOfusers).toInt

    val lazyUsers = (.20 * noOfusers).toInt
    
    println("tweetsMostUser " + tweetsMostUser + " moderateTweetUSers " + moderateTweetUSers + " lazyUsers " + lazyUsers )

    val system = ActorSystem("TwitterClient", ConfigFactory.load(config))
    serverIp
    var addressstring = "akka.tcp://TwitterServer@" + serverIp + ":5150/user/server"
    println(addressstring)
    server = system.actorFor(addressstring)
    // server ! ("Hello")

    var response = system.actorOf(Props(new Response), "response")
    var userSet = new HashSet[Int]()

    // to set the traffic created by users
    for (i <- 1 to noOfusers) {
      val user = system.actorOf(Props(new User), i.toString)
      userSet.add(i)
    }
    // to update traffic statistics
    var count1 = 0
    for ((user) <- userSet) {
      count1 = count1 + 1
      if (count1 <= tweetsMostUser) {
        system.actorSelection("/user/" + user) ! init(user, true, false, false, false)
      } else if (count1 <= moderateTweetUSers + tweetsMostUser) {
        system.actorSelection("/user/" + user) ! init(user, false, true, false, false)
      } else if (count1 <= lazyUsers + moderateTweetUSers + tweetsMostUser) {
        system.actorSelection("/user/" + user) ! init(user, false, false, true, false)
      } else {
        system.actorSelection("/user/" + user) ! init(user, false, false, false, true)
      }

    }

    monitor = system.actorOf(Props(new Monitor), "monitor")
    for (uid <- 1 to noOfusers) {
      system.actorSelection("/user/" + uid) ! setScheduler()
    }
  }

  class Response extends Actor {

    def receive = {
      case (timeline: ArrayBuffer[Tweets], id: Int, value: String) =>

      /*  var st = "Recieved Response " + value + " User" + id 
          for(tweets <- timeline){
            st = st + tweets.Id
          }
          println(st)*/
    }
  }

  class User extends Actor {

    import context.dispatcher
    var alarm: akka.actor.Cancellable = null
    var alarm1: akka.actor.Cancellable = null
    var alarm2: akka.actor.Cancellable = null

    var userId = 0

    // generates most of the traffic
    var type1User = false

    // moderately tweets
    var type2User = false

    // no tweets, only sees hometimelne
    var type3User = false

    // no activity 
    var type4User = false

    // generate twitter content  
    def generateContent(): String = {

      var size = largeBodyOfText.length()

      var startIndex = util.Random.nextInt(size) + 1
      var endIndex = util.Random.nextInt(130) + startIndex + 10
      if (endIndex > size) {
        endIndex = size
      }
      //  println("start " + startIndex + " end " + endIndex)
      var text = largeBodyOfText.substring(startIndex, endIndex)
      return text
    }

    def receive = {

      case init(id: Int, type1: Boolean, type2: Boolean, type3: Boolean, type4: Boolean) =>
        userId = id
        type1User = type1
        type2User = type2
        type3User = type3
        type4User = type4

      case setScheduler() =>
        // set scheduler for this user   
        // println("setting")
        if (type1User) {
          //    println("type1")
          var action: String = ""

          alarm = context.system.scheduler.schedule(Duration.create(10000, TimeUnit.MILLISECONDS),
            Duration.create(30000, TimeUnit.MILLISECONDS), self, doSomething("tweetMessage"))
            alarm1 = context.system.scheduler.schedule(Duration.create(10000, TimeUnit.MILLISECONDS),
            Duration.create(100000, TimeUnit.MILLISECONDS), self, doSomething("getHomeTimeline"))
             alarm2 = context.system.scheduler.schedule(Duration.create(10000, TimeUnit.MILLISECONDS),
            Duration.create(100000, TimeUnit.MILLISECONDS), self, doSomething("getUserTimeline"))

        } else if (type2User) {
          //  println("type2")
          alarm = context.system.scheduler.schedule(Duration.create(30000, TimeUnit.MILLISECONDS),
            Duration.create(60000, TimeUnit.MILLISECONDS), self, doSomething("tweetMessage"))
        } else if (type3User) {
          //  println("type3")
          alarm = context.system.scheduler.schedule(Duration.create(40000, TimeUnit.MILLISECONDS),
            Duration.create(60000, TimeUnit.MILLISECONDS), self, doSomething("getHomeTimeline"))
        } 

      case doSomething(action: String) =>
        //println(" userID " + userId + " action " + action)
        action match {
          case "tweetMessage" =>
            val tweet = new Tweets()
            tweet.text = generateContent()
            tweet.sourceUser = userId
            tweet.createdAt = System.currentTimeMillis
            // send tweet to server
            monitor ! outTraffic()
            server ! (tweet)

          //  println("Tweet User" + userId  + " content " +  tweet.text + " time " + tweet.createdAt )

          case "getHomeTimeline" =>
            monitor ! outTraffic()
            server ! (userId, "getHomeTimeline")
          // println("userid " + userId + " getHomeTimeline")
          case "getUserTimeline" =>
            monitor ! outTraffic()
            server ! (userId, "getUserTimeline")
        }

    }
  }

  var largeBodyOfText = "microblogging service as part of a recruiting talk at UC Berkeley. One interesting stat involves the number of API calls that the sit" +
    "e is seeing: Krikorian says that Twitter is seeing 6 billion API calls per day, or 70,000 per second. Thats more than double theamount (3 billion API calls  " +
    " that Twitter was seeing in April, according to the Programmable Web.Some of the stats, including the fact that Twitteris seeing 90 million Tweets per day " +
    "we also heard at Twitters news event this past week. Krikorian delved into the amount of data storage each Tweet " +
    "includes in terms of text (200 bytes for 140 characters Twitter is seeing 800 Tweets per second, or 12GBs of Twitter text. And Twitter generates 8 " +
    "terabytes of data per day (or 80 MB per second), whereas the New York Stock Exchange creates 1 terabyte of data per day" +
    "In his slides, Krikorian says Twitters 150 million users generate 1000 Tweets per second, and that Twitter eventually wants to be able to support " +
    "half the worlds population and all of their devices (5 billion people and 6 billion devices, according to Krikorian). Challenges for the network, says" +
    "Krikorian, include indexing, search, analytics, storage, scalability and efficiency.Twitter had a considerable amount of downtime over the summer due " +
    "to record usage because of the World Cup, and was forced to cut API limits on Tweet read requests. Krikorian presented a nifty slide in his presentation " +
    "that shows the u "

  class Monitor extends Actor {

    var outgoingTraffic = 0
    import context.dispatcher
    var alarm: akka.actor.Cancellable = context.system.scheduler.schedule(Duration.create(10000, TimeUnit.MILLISECONDS),
      Duration.create(10000, TimeUnit.MILLISECONDS), self, resetcount())

    var alarm2: akka.actor.Cancellable = context.system.scheduler.scheduleOnce(Duration.create(180000, TimeUnit.MILLISECONDS), self, terminate())

    def receive = {
      // Incoming traffic
      case outTraffic() =>
        outgoingTraffic = outgoingTraffic + 1

      case resetcount() =>
        println("outgoingTraffic " + outgoingTraffic)
        outgoingTraffic = 0
      case terminate() =>
        alarm.cancel
        exit(0)

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