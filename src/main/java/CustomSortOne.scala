/**
  * Created by jinyuzhang on 9/26/17.
  */
import org.apache.spark.{SparkConf, SparkContext}
object CustomSortOne {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CustomSortOne")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(List(("Yufeihong", 90, 28, 1), ("angelababy", 90, 27, 2)))
  }
}
