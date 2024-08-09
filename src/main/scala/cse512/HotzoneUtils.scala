package cse512

object HotzoneUtils {

  //my code
  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    var rangeRectangle:Array[String]= queryRectangle.split(",")
    var checkRectangle:Array[String]=pointString.split(",")
    if(rangeRectangle(0).toDouble<=checkRectangle(0).toDouble && rangeRectangle(1).toDouble<=checkRectangle(1).toDouble){
      if(rangeRectangle(2).toDouble>=checkRectangle(0).toDouble && rangeRectangle(3).toDouble>=checkRectangle(1).toDouble){
        return true
      }
    }
    return false
  }

}
