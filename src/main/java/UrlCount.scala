/**
  * Created by jinyuzhang on 9/25/17.
  */
import org.apache.spark.{SparkConf, SparkContext}
import java.net.URL


object UrlCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UrlCount")
    val sc = new SparkContext(conf)
    val rdd1 = sc.textFile(args(0)).map(line => {
      val f = line.split("\t")
      (f(1), 1)
    })
    val rdd2 = rdd1.reduceByKey(_+_)
    val rdd3 = rdd2.map(line => {
      val url = line._1
      val host = new URL(url).getHost
      (host, url, line._2)
    })
    val rdd4 = rdd3.groupBy(_._1).mapValues(it => {
      it.toList.sortBy(_._3).reverse.take(3)
      //java中排序，放在一台机器上 如果数据量太大 内存会溢出
    })
    println("---------------------------")
    println(rdd4.collect().toBuffer)
    sc.stop()
  }
}
