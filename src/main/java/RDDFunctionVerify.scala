import org.apache.spark.{SparkConf, SparkContext}
//import scala.collection.mutable.ArrayBuffer
/**
  * Created by jinyuzhang on 9/28/17.
  */
object RDDFunctionVerify {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDDFunctionVerify").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val c = sc.parallelize(List("cat", "dog", "tiger", "lion", "gnu", "crocodile", "ant", "whale", "dolphin", "spider"), 3)
//    val l = ArrayBuffer[String]()
//    c.foreach(x => (l ++ x))
//    println(l)
//    println("Hello World")
    c.foreach(x => println(x + "s are yummy"))



  }
}
