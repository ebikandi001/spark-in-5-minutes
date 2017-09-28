
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by oskar on 23/05/17.
  */
object Main extends App {

  final case class battle(data: Array[String]) {
    var name = data(0)
    var year = data(1)
    var battle_number = data(2)
    var attacker_king = data(3)
    var defender_king = data(4)
    var attacker_1 = data(5)
    var attacker_2 = data(6)
    var attacker_3 = data(7)
    var attacker_4 = data(8)
    var defender_1 = data(9)
    var defender_2 = data(10)
    var defender_3 = data(11)
    var defender_4 = data(12)
    var attacker_outcome = data(13)
    var battle_type = data(14)
    var major_death = data(15)
    var major_capture = data(16)
    var attacker_size = data(17)
    var defender_size = data(18)
    var attacker_commander = data(19)
    var defender_commander = data(20)
    var summer = data(21)
    var location = data(22)
    var region = data(23)
    //var note = data(24)
  }

  val sparkConf = new SparkConf().setAppName("Spark-HBase").setMaster("local[*]")
  sparkConf.set("spark.executor.cores", "5")
  val sc = new SparkContext(sparkConf)
  val csv = sc.textFile("/home/oskar/Downloads/battles.csv")
  val data = csv.map(line => line.split(","))
  val attacker_list = data.map(Arrays => battle(Arrays)).map(battle => (battle.attacker_1, battle.defender_1))

  attacker_list.map((tuple: (String, String)) => ((tuple._1, tuple._2), 1))
    .reduceByKey { case (accumulatedValue, currentValue) => accumulatedValue + currentValue }
    .foreach { case (key, numberOfTimes) => println(key + ":" + numberOfTimes) }

  val sqlContext = new SQLContext(sc)

  val df = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true") // Use first line of all files as header
    .option("inferSchema", "true") // Automatically infer data types
    .load("/home/oskar/Downloads/character-deaths.csv")

  df.registerTempTable("deaths")
  sqlContext.sql("SELECT * FROM deaths WHERE Allegiances='Stark'").show()


}
