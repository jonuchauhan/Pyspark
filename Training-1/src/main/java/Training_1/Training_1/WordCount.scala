package Training_1.Training_1
import org.apache.spark.rdd.JdbcRDD._
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.SparkConf

import org.apache.spark.sql._

object MysqlToHdfsIntegration {
  
  def main (args :Array[String])
  {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val url ="jdbc:mysql://ms.itversity.com:3306/retail_db"
    val username = "retail_user"
    val password = "itversity"
    Class.forName("com.mysql.jdbc.Driver").newInstance()
 val conf = new SparkConf().setAppName("MysqlToHdfsIntegration").setMaster("local[*]").set("useSSL", "true")
 
    val sc = new SparkContext(conf)
    val sql = new SQLContext(sc)
    val order_item = sql.read.format("org.apache.spark.sql.execution.datasources.jdbc.DefaultSource").option("url", url).
    option("driver", "com.mysql.jdbc.Driver").option("dbtable", "order_items").
    option("user", username).option("password", password).load()
    println("connection succeeded")
    
    
    order_item.rdd.saveAsTextFile("../jonuchauhan1/output/hello_jonuchauhan.txt")
    
    }
  
  
  
}