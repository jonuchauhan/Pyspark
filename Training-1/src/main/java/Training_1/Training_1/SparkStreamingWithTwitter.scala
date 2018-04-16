package Training_1.Training_1

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import twitter4j.Logger

object SparkStreamingWithTwitter {
  
    def setupLogging() = {
    import org.apache.log4j.{Level, Logger}   
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)   
  }
  
  
  def twittersetup()=
  {
  import scala.io.Source
  for(line <- Source.fromFile("../twitter.txt").getLines())
  {
    val fields = line.split(" ")
     System.setProperty("twitter4j.oauth." + fields(0) , fields(1))
     println(  System.setProperty("twitter4j.oauth." + fields(0) , fields(1)))
    
  }
  
  }
  
  def main(args : Array[String]){
    setupLogging()
    twittersetup()
    
    val conf = new SparkConf().setAppName("SparkStreamingWithTwitter").setMaster("local[*]")
    
    val ssc = new StreamingContext("local[*]", "PopularHashtags",Seconds(1))
    
    val tweets = TwitterUtils.createStream(ssc,None)
    val status = tweets.map(tweets=>tweets.getText)
    val words =status.flatMap(x=>(x.split(" ")))
    val hastags = words.filter(x=>(x.startsWith("#"))).map(x=>(x,1))
    val hastagscount = hastags.reduceByKeyAndWindow((x,y)=> x+y, (x,y)=>x-y, Seconds(300), Seconds(1))
    val count = hastagscount.transform(x=> x.sortBy(x=>x._2,false))
    
    count.saveAsTextFiles("../popular_hastags.txt")
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
    }
  
 }


  
  
 
  
    
    
    
    
