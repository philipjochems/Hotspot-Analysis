package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  
  //my code
  spark.udf.register("getisOrd",(numCells: Int,sqr: Double,sum: Double,count: Int,sum2: Int)=>((
    HotcellUtils.getisOrd(numCells,sqr,sum,count,sum2)
  )))
  pickupInfo.createOrReplaceTempView("myTemp")
  val validPoints = spark.sql("SELECT x, y, z, count(*) AS count FROM myTemp WHERE x>="+minX+" AND y>="+minY + " AND z>="+minZ+" AND x<="+maxX+" AND y<="+maxY+" AND z<=" +maxZ+" group by x,y,z")
  validPoints.createOrReplaceTempView("validPoints")  
  val validPoints2 = spark.sql("SELECT x, y, z, count(*) AS count FROM myTemp WHERE x>="+minX+" AND y>="+minY + " AND z>="+minZ+" AND x<="+maxX+" AND y<="+maxY+" AND z<=" +maxZ+" group by x,y,z")
  validPoints2.createOrReplaceTempView("validPoints2") 
  val findN = spark.sql("SELECT validPoints.x AS x, validPoints.y AS y, validPoints.z AS z, SUM(validPoints2.count) AS sum2, count(*) AS count FROM validPoints LEFT JOIN validPoints2  WHERE ABS(validPoints.z-validPoints2.z) <= 1 AND ABS(validPoints.x-validPoints2.x) <= 1 AND ABS(validPoints.y-validPoints2.y) <= 1 GROUP BY validPoints.x, validPoints.y, validPoints.z")
  findN.createOrReplaceTempView("findN")
  val sumSql2 = spark.sql("SELECT sum(count*count) AS sqr FROM validPoints")
  val sumSql = spark.sql("SELECT sum(count) AS sum FROM validPoints")
  val getisScore =  spark.sql("SELECT x,y,z,getisOrd("+ numCells + ","+ sumSql2.first().getLong(0).toDouble+ ","+ sumSql.first().getLong(0).toDouble +",count, sum2" + ") AS gScore FROM findN")
  getisScore.createOrReplaceTempView("getisScore")
  val returnDf = spark.sql("SELECT x,y,z FROM getisScore ORDER BY gScore DESC")
  return returnDf
}
}
