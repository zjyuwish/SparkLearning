/**
  * Created by jinyuzhang on 9/26/17.
  */
import org.apache.spark.{SparkConf, SparkContext}
//object OrderContext {
//  implicit val girlOrdering  = new Ordering[Girl] {
//    override def compare(x: Girl, y: Girl): Int = {
//      if(x.faceValue > y.faceValue) 1
//      else if (x.faceValue == y.faceValue) {
//        if(x.age > y.age) -1 else 1
//      } else -1
//    }
//  }
//}
//object CustomSortTwo {
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setAppName("CustomSortTwo")
//    val sc = new SparkContext(conf)
//    val rdd1 = sc.parallelize(List(("Yufeihong", 90, 28, 1), ("angelababy", 90, 27, 2), ("JuJingYi", 95, 22, 3)))
//    import OrderContext._
//    val rdd2 = rdd1.sortBy(x => Girl(x._2, x._3), false)
//    println(rdd2.collect().toBuffer)
//    sc.stop()
//  }
//
//
//}
//case class Girl(val faceValue: Int, val age: Int) extends serializable
