import org.apache.spark._
import org.apache.spark.api.java.JavaPairRDD
import org.mongodb
import org.mongodb.scala.{MongoClient}
import org.mongodb.scala.bson.collection.immutable.Document

object Query2 extends App {

  def parseLines(x:Array[String]): (Int, Int, Int, String, String, String) = {

    val date = x(1).split("/")

    val month = date(0).toInt
    val day = date(1).toInt
    val year = date(2).toInt
    val province = x(2)
    val country = x(3)
    val deaths = x(6)

    (month,day,year,province,country,deaths)

  }

  def filterTuples(x:(Int, Int, Int, String, String, String)) : Boolean = {

    x._3.equals(2020) && x._5.equals("Mainland China") && x._2.equals(durations(x._1))

  }

  def parseFilteredLines(x:(Int, Int, Int, String, String, String)) : (Int,String,Float) = {

    val month = x._1
    val province = x._4
    val deaths = x._6.toFloat

    (month,province,deaths)

  }

  def parseIncreases(x:((Int,Int,String),Float)) : (Int,(String,Float)) = {

    val month = x._1._2
    val province = x._1._3
    val deathIncrease = x._2

    (month,(province,deathIncrease))

  }

  def parser(x:(Int, String,Float)) = {

    val currentMonth = x._1
    val province = x._2
    val deaths = x._3

    Array(((currentMonth,currentMonth+1,province),deaths),((currentMonth-1,currentMonth,province),deaths))

  }

  def takeAbs(x:Float,y:Float) : Float = {

    Math.abs(x-y)

  }

  def takeProvince(x:(String,Float),y:(String,Float)) : (String,Float) = {

    if(x._2 >= y._2) x else y

  }

  def remove13(x:(Int,(String,Float))) : Boolean = {

    !x._1.equals(13)

  }

  def parseToTuple1(x:(Int,(String,Float))) : (Int,String,Float) = {

    val couple = x._2

    (x._1,couple._1,couple._2)

  }

  val header = "SNo,ObservationDate,Province/State,Country/Region,Last Update,Confirmed,Deaths,Recovered"

  val durations = Array(0,31,29,31,30,31,30,31,31,30,31,30,31)

  val path = "/home/fran22/Scaricati/covid_19_data.csv"

  val context = new SparkContext("local[*]","query2")

  val startingRDD = context.textFile(path)

  val filtered = startingRDD.filter(!_.equals(header)).map(_.split(",")).map(parseLines).filter(filterTuples)//.filter(filterDays)

  val kvRDD = filtered.map(parseFilteredLines)

  val tmp = kvRDD.flatMap(parser)

  val javaKV = new JavaPairRDD[(Int,Int,String),Float](tmp)

  val increases = javaKV.reduceByKey(takeAbs(_,_))

  val tuples = increases.map(parseIncreases(_)).filter(remove13(_))

  val res = new JavaPairRDD[Int,(String,Float)](tuples).reduceByKey(takeProvince(_,_)).map(parseToTuple1(_))

  val client = MongoClient() //mongoDB client connected to a server on localhost on standard port 27017

  val database = client.getDatabase("query")

  val collection = database.getCollection("months")

  for(m <- res){

    val doc = Document("month" -> m._1,"province" -> m._2, "increase" -> m._3.toInt)
    collection.insertOne(doc).subscribe(x => {println(x);println("inserted document")})
  }


}