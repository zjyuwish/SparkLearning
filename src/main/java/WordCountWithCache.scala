/**
  * Created by jinyuzhang on 9/25/17.
  */
import org.apache.spark.{SparkConf, SparkContext}
object WordCountWithCache {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WC_withCache")
    val sc = new SparkContext()
    sc.textFile(args(0)).cache().flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).saveAsTextFile(args(1))
    sc.stop()
  }
}
