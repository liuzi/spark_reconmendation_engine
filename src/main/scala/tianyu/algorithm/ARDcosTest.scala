package tianyu.algorithm

import org.apache.spark.sql.SparkSession
import scopt.OptionParser
import tianyu.algorithm.recommendation.AssociationRule
import tianyu.algorithm.util.hdfs

/**
  * Created by lynnjiang on 2017/4/20.
  */
object ARDcosTest {

  case class Params(outDir:String = "" ,
                    timeProcess:Boolean = false,
                    endTime:String = "",
                    numPastMonths: Int = 1,
                    maxItems: Int = 100,
                    minSupport: Double = 0.2,
                    minConfidence: Double = 0.6,
                    topN: Int = 10,
                    numPartitions: Int = 10
                    ) extends AbstractParams[Params]

  //FIXME GET THE NEWEST DEFINITION

  def main(args: Array[String]) {

    val defaultParams = Params()

    val parser = new OptionParser[Params]("ARDcosTest") {
      head("ARDcosTest: an example app.")
      opt[String]("outDir").required()
        .text("root directory for saving the anaysis results")
        .action((x, c) => c.copy(outDir = x))
      opt[Boolean]("timeProcess").required()
        .text("whether process the time series of data")
        .action((x,c)=> c.copy(timeProcess = x))
      opt[String]("endTime").required()
        .text("specified current time/ end time")
        .action((x,c) => c.copy(endTime = x))
      opt[Int]("numPastMonths").required()
        .text("Recent histpry")
        .action((x,c)=>c.copy(numPastMonths = x))
      opt[Int]("maxItems").required()
        .text("minimun number of items to determine a abnormal transaction")
        .action((x,c)=>c.copy(maxItems = x))
      opt[Double]("minSupport").required()
        .text(s"minimum of support")
        .action((x, c) => c.copy(minSupport = x))
      opt[Double]("minConfidence").required()
        .text(s"minimum of confidence")
        .action((x, c) => c.copy(minConfidence = x))
      opt[Int]("topN").required()
        .text(s"recommend topM items")
        .action((x, c) => c.copy(topN = x))
      opt[Int]("numPartitions").required()
        .text(s"numbers of partitions for RDD")
        .action((x, c) => c.copy(numPartitions = x))

    }

    parser.parse(args, defaultParams) match {
      case Some(params) => run(params)
      case _ => sys.exit(1)
    }
  }

  def run(params: Params): Unit = {

    val spark = SparkSession
      .builder
      .appName("ARDcosTest")
      .getOrCreate()

    val sc = spark.sparkContext
    //val filePath = "/Users/lynnjiang/Desktop/TianYu/testdata/resource_system/T_DOWNLOAD_LOG.csv"
//    val userfile: String = "t_user_daily_full"
//    val itemfile: String = "t_product_daily_full"

    //val userPath = "hdfs://node3.tianyuyun.cn:8020/tianyu/rdbms/t_user/2017/03/28/*"
    //val prodPath = "hdfs://node3.tianyuyun.cn:8020/tianyu/rdbms/t_product/2017/03/28/*"


    val endTime = if(params.endTime=="now"){
      new java.util.Date().getTime
    }else{
      val timeFormat = """(\d{4})(\d{2})(\d{2})""".r
      java.sql.Timestamp.valueOf(
        timeFormat.replaceAllIn(params.endTime,"$1-$2-$3 00:00:00")
      ).getTime
    }

    /**
      * Create a Recommendation Model based on association rule
      * @param spark: SparkSession type. SparkSession created by main method.
      * @param charset: String tyep. Encoding of the data storing in files.
      * @param savePath: String type. The path to save the recommendation results.
      * @param userPath: String type. Path of data file which defines the users' information
      * @param prodPath: String type. Path of data file which defines the products' information
      */

    val ar = new AssociationRule(
      spark = spark,
      outDir=params.outDir,
      timeProcess=params.timeProcess,
      maxItems=params.maxItems,
      endTime = endTime,
      //sc=sc,
      //      userfile = userfile,
      //      itemfile = itemfile,
      //      dirname= params.dirname,
      //      recentPeriod = params.recent,
      numPastMonths=params.numPastMonths,
      minSupport=params.minSupport,
      minConfidence=params.minConfidence,
      topN=params.topN,
      numPartitions=params.numPartitions)


    params.timeProcess match {
      case true =>ar.getRecoMultibyDate(hdfs.subpath,hdfs.scorepath,hdfs.downpath,hdfs.collectpath)
      case false =>ar.getRecoMulti(hdfs.subpath,hdfs.scorepath,hdfs.downpath,hdfs.collectpath)
    }


    /**
      * Get Recommendation
      * @param filePath: String type. The file path for loading data.
      * @param minSupport: Double type. The minimum of support degree, for filtering out frequent items.
      * @param minConfidence: Double type. The minimum of confidence degree, for filtering out rules.
      * @param topN: Integer type. How many products or resources to recommend.
      * @param userID: String type. The column name of userid in the rawDF.
      * @param itemID: String type. The column name of itemid in the rawDF.
      * @param numPartitions: Integer type. The number of partitions used to distribute the work.
      **/

//    ar.getComparison(userid="shixun_594844")
    spark.stop()
  }
}