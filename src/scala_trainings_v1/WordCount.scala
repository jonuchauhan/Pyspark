package scala_trainings_v1

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.SparkConf


object WordCount {
  
   /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("WordCount")
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext(conf)   
    
    // Read each line of my book into an RDD
    val input = sc.textFile("../jonuchauhan1/input/book.txt")
    
    // Split into words separated by a space character
    val words = input.flatMap(x => x.split(" "))
    
    // Count up the occurrences of each word
    val wordCounts = words.countByValue()
    
    // Print the results.
    wordCounts.foreach(println)
  }
  
  
  
}