/**
  * Created by jinyuzhang on 9/26/17.
  */
import org.apache.spark.{SparkConf, SparkContext}
object CustomSortOne {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CustomSortOne")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(List(("Yufeihong", 90, 28, 1), ("angelababy", 90, 27, 2), ("JuJingYi", 95, 22, 3)))
    val rdd2 = rdd1.sortBy(x => Girl(x._2, x._3), false)
    println(rdd2.collect().toBuffer)
    sc.stop()
  }


}
case class Girl(val faceValue: Int, val age: Int) extends Ordered[Girl] with Serializable {
  override def compare(that: Girl): Int = {
    if(this.faceValue == that.faceValue) {
      this.age - that.age
    }
    else {
      this.faceValue - that.faceValue
    }
  }
}
