/**
  * Created by jinyuzhang on 9/27/17.
  */
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object SQLDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SQLDemo")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    System.setProperty("user.name", args(0))

    val personRdd = sc.textFile(args(1)).map(line => {
      val fields = line.split(",")
      Person(fields(0).toLong, fields(1), fields(2).toInt)
    })

    import sqlContext.implicits._
    val personDf = personRdd.toDF

    personDf.registerTempTable("person")

    sqlContext.sql("select * from person where age >= 20 order by age desc limit 2").show()

    sc.stop()
  }
}
case class Person(id: Long, name: String, age: Int)
