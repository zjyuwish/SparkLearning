/**
  * Created by jinyuzhang on 9/25/17.
  */
import org.apache.spark.{SparkConf, SparkContext}

object UserLocation {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("UserLocation")
    val sc = new SparkContext(conf)
    val mbt = sc.textFile(args(0)).map(line => {
      val fields = line.split(",")
      val eventType = fields(3)
      val time = fields(1)
      val timeLong = if(eventType == "1") -time.toLong else time.toLong
      (fields(0) + "_" + fields(2), timeLong)

    })
    val rdd1 = mbt.groupBy(_._1).mapValues(_.foldLeft(0L)(_ + _._2))
    val rdd2 = rdd1.map( t => {
      val mobile_bs = t._1
      val mobile = mobile_bs.split("_")(0)
      val lac = mobile_bs.split("_")(1)
      val time = t._2
      (mobile, lac, time)
    })
    val rdd3 = rdd2.groupBy(_._1)
    //ArrayBuffer((18688888888,List((18688888888,16030401EAFB68F1E3CDF819735E1C66,87600), (18688888888,9F36407EAD0629FC166F14DDE7970F68,51200))), (18611132889,List((18611132889,16030401EAFB68F1E3CDF819735E1C66,97500), (18611132889,9F36407EAD0629FC166F14DDE7970F68,54000))))
    val rdd4 = rdd3.mapValues(it => {
      it.toList.sortBy(_._3).reverse.take(2)
    })
    println(rdd4.collect().toBuffer)
    sc.stop()
  }
}
