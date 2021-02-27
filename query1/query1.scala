import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.api.java.{JavaDStream, JavaPairDStream}
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.mongodb
import org.mongodb.scala.MongoClient
import org.mongodb.scala.bson.collection.immutable.Document

object query1 extends App {

  def parseLines(x:Array[String]):((String,String),(String,Long))={

    val date = x(1).split("T")(0)
    val region = x(4)
    val province = x(6)
    val totalCases = x(10).toLong

    ((date,region),(province,totalCases))

  }

  def takeProvince(t1:(String,Long),t2:(String,Long)):(String,Long)={

    if(t1._2 >= t2._2) t1 else t2

  }

  def finalMapper(x:((String,String),(String,Long))) : List[String] = {

    val t1 = x._1
    val t2 = x._2

    List(t1._1,t1._2,t2._1,t2._2.toString)

  }

  def saveToMongo(x:RDD[List[String]]) : Unit = {

    val lists = x.collect().toList
    val doc = Document("content" -> lists)
    collection.insertOne(doc).subscribe(x => println("An RDD has been stored"))

  }

  val BATCH_INTERVAL = new Duration(5000)

  val path = "/home/fran22/Scaricati/covid19_italy_province.csv"

  val config = new SparkConf().setMaster("local[*]").setAppName("query1")

  val context = new StreamingContext(config, BATCH_INTERVAL)

  val header = context.sparkContext.textFile(path).first().split(",")

  val receiver = context.socketTextStream("10.0.2.15",9999,StorageLevel.MEMORY_ONLY)

  val tuples = receiver.map(_.split(",")).filter(!_.equals(header)).map(parseLines)

  val kvStream = new JavaPairDStream[(String,String),(String,Long)](tuples)

  val res = kvStream.reduceByKey(takeProvince(_,_)).map(finalMapper(_))

  val client = MongoClient() //mongoDB client connected to a server on localhost on standard port 27017

  val database = client.getDatabase("query")

  val collection = database.getCollection("col")

  res.foreachRDD(saveToMongo(_))

  //res.print()

  context.start()

  context.awaitTermination()

}