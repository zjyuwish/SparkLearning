package ScalaStudy

/**
  * Created by jinyuzhang on 11/22/17.
  */
object Job {
  private var hello: HelloWord = new HelloWord
  def getInstance: HelloWord = {
      if(hello == null)
      hello = new HelloWord
      hello
  }
}


class Job{
    private var port: Int = 2090
}

class HelloWord  {
  var abc = 345

}
