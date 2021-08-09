/**
  * Created by lynnjiang on 2017/4/20.
  */
package tianyu.algorithm

import org.apache.spark.sql.SparkSession
import tianyu.algorithm.recommendation.AssociationRule
import tianyu.algorithm.util.hdfs

object ARLocalTest extends App{


  val spark = SparkSession.builder().appName("AssociationRule").master("local[*]")
    .config("spark.some.config.option", "some-value")
    .enableHiveSupport()
    .getOrCreate()


  /**
    * Create a Recommendation Model based on association rule
    * @param spark: SparkSession type. SparkSession created by main method.
    * @param userPath: String type. Path of data file which defines the users' information
    * @param prodPath: String type. Path of data file which defines the products' information
    * @param minSupport: Double type. The minimum of support degree, for filtering out frequent items.
    * @param minConfidence: Double type. The minimum of confidence degree, for filtering out rules.
    * @param topN: Integer type. How many products or resources to recommend.
    * @param userID: String type. The column name of userid in the rawDF.
    * @param itemID: String type. The column name of itemid in the rawDF.
    * @param numPartitions: Integer type. The number of partitions used to distribute the work.
    */

//  val testArray:Array[String] = Array("2017-03-25 00:00:00","2017-01-12 00:00:00","2017-05-17 00:00:00", "2016-11-16 00:00:00")
//
//  testArray.foreach{time=>

//    val testTime = java.sql.Timestamp.valueOf(time).getTime

    val timeprocess = true

    val timeFormat = """(\d{4})(\d{2})(\d{2})""".r
    val endTime = java.sql.Timestamp.valueOf(
      timeFormat.replaceAllIn("20170320","$1-$2-$3 00:00:00")
    ).getTime

//  val endTime = new java.util.Date().getTime

    val ar = new AssociationRule(
      spark = spark,
      //sc=sc,
      //      userfile = userfile,
      //      itemfile = itemfile,
      //      dirname= params.dirname,
      //      recentPeriod = params.recent,
      outDir="/Users/lynnjiang/Desktop/",
      timeProcess=timeprocess,
      endTime=endTime,
      maxItems = 1000,
      numPastMonths=1,
      minSupport=0.03,
      minConfidence=0.6,
      topN=10,
      numPartitions=100)

     timeprocess match {
       case true =>ar.getRecoMultibyDate(hdfs.subpath,hdfs.scorepath,hdfs.downpath,hdfs.collectpath)
       case false =>ar.getRecoMulti(hdfs.subpath,hdfs.scorepath,hdfs.downpath,hdfs.collectpath)

     }



//  val ar = new AssociationRule(spark = spark,
//    //sc = sc,
//    //userfile = "t_user_daily_full.avro",
//    //itemfile= "t_product_hourly_inc.avro",
//    //dirname = "",
//    numPartitions =10,
//    //recentPeriod = 360,
//    minSupport=0.1,
//    minConfidence=0.6)
//
//
//
//  /**
//    * Get Recommendation
//    * @param filePath: String type. The file path for loading data.
//
//    **/
//  val filename = "t_download_log_hourly_inc.avro"
//  //ar.getRecommendFromPath(filename)
//  ar.getRecoMultibyDate()
//  //ar.getComparison(userid="shixun_594844")
//  //ar.getComparisonFromPath(filename = "down_test.csv")

}

