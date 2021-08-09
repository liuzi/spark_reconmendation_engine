package tianyu.algorithm

import org.apache.spark.sql.{DataFrame, SparkSession}
import scopt.OptionParser
import tianyu.algorithm.ClusterDcosTest.Params
import tianyu.algorithm.dataprocess.DataCleansing
import tianyu.algorithm.recommendation.Cluster
import tianyu.algorithm.util.hdfs
import org.apache.spark.sql.functions.{col, when}

/**
  * Created by lynnjiang on 2017/5/24.
  */
object Comparison {

  case class Params(rootDir: String = null,
                    user: String = null,
                    alg: String = null,
                    subType: String = null,
                    DateTime: String = null) extends AbstractParams[Params]

  private val hisname = "User-history-logs"
  private val reconame = "User-recommendations"
  private val user_id_field = "PERSON_ID"
  private val log_user_id_field = "USER_ID"
  private val account_id_field = "ACCOUNT"
  private val user_name_field = "NAME"
  private val item_id_field = "PRODUCT_CODE"
  private val item_name_field = "PRODUCT_NAME"
  private val score_field = "SCORE"

  def main(args: Array[String]) {

    val defaultParams = Params()

    val parser = new OptionParser[Params]("Comparison") {
      head("ClusterDcosTest: an example app.")
      opt[String]("rootDir").required()
        .text("Root directory of results file")
        .action((x, c) => c.copy(rootDir = x))
      opt[String]("user").required()
        .text("Name, ID or account of the user to look up")
        .action((x, c) => c.copy(user= x))
      opt[String]("alg").required()
        .text("Name of recommendation model")
        .action((x, c) => c.copy(alg = x))
      opt[String]("subType").required()
        .text("Subsidiary type of the specific recommendation model")
        .action((x, c) => c.copy(subType = x))
      opt[String]("DateTime").required()
        .text("Time when the analysis are executed")
        .action((x, c) => c.copy(DateTime = x))

    }

    parser.parse(args, defaultParams) match {
      case Some(params) => run(params)
      case _ => sys.exit(1)
    }
  }


  private def getNamedDF(df:DataFrame,userDefineDF:DataFrame,itemDefineDF:DataFrame):DataFrame={
    //import df.sqlContext.implicits._
    df.join(userDefineDF,log_user_id_field)
      .join(itemDefineDF,item_id_field)
      .drop(item_id_field)
  }

  private def getSelectDF(df:DataFrame,cols:String *)=df.select(cols.map(col):_*)
//  private def getARHistory(spark:SparkSession,
//                           resultPath:String,
//                           hisname:String="User-history-logs"):DataFrame={
//    import spark.implicits._
//    getNamedDF(spark.sparkContext.textFile(resultPath+"/"+hisname)
//    .map(_.split("#")).map(s=>(s(0),s(1)))
//    .toDF(log_user_id_field,item_id_field),
//    searched_userDF,itemDefineDF)}
//  def getClusterHistory():DataFrame={}
//  def searchUser(user:String)


  def run(params: Params): Unit ={

    val spark = SparkSession
      .builder
      .appName("Comparison")
      .getOrCreate()

    val sc = spark.sparkContext
    import spark.implicits._

    val resultPath = hdfs.getPath(
      params.rootDir,
      params.alg,
      params.subType,
      params.DateTime)
    val savePath = hdfs.getPath(
      params.rootDir,
      "Comparison",
      params.user,
      Seq(params.alg, params.subType, params.DateTime).mkString("_")
    )

    val dc = new DataCleansing(spark)

    val userDefineDF = dc.dataFrame(hdfs.accountpath).select(user_id_field,account_id_field)
      .join(dc.dataFrame(hdfs.userpath).select(user_id_field,user_name_field),user_id_field)
      .withColumnRenamed(user_id_field,log_user_id_field)
      .distinct()
//      .persist()

    val searched_userDF = userDefineDF.where(
      col(log_user_id_field)===params.user ||
        col(user_name_field)===params.user ||
        col(account_id_field)===params.user).persist()

    val itemDefineDF = dc.dataFrame(hdfs.productpath)
      .select(item_id_field,item_name_field)
      .distinct()
      .persist()

    val hisIDDF = params.alg match {
      case "Association" => sc.textFile(resultPath+"/"+hisname)
        .map(_.split("#")).map(s=>(s(0),s(1)))
        .toDF(log_user_id_field,item_id_field)
      case "Cluster" => Seq(hdfs.scorepath,hdfs.downpath,hdfs.collectpath)
          .map(dc.dataFrame)
          .map(getSelectDF(_,log_user_id_field,item_id_field))
          .reduce((pre,after)=>pre.union(after))
      case "ALS" => getSelectDF(dc.dataFrame(hdfs.scorepath),
        log_user_id_field,item_id_field,score_field)
      case "CosSim" => getSelectDF(dc.dataFrame(hdfs.scorepath),
        log_user_id_field,item_id_field,score_field)
      case _ => throw new IllegalArgumentException("Illegal value for alg argument " + params.alg)
    }


    val historyDF = getNamedDF(hisIDDF,searched_userDF,itemDefineDF)

    val recoDF = getNamedDF(sc.textFile(resultPath+"/"+reconame)
      .map(_.split("#")).map(s=>(s(0),s(1)))
      .toDF(log_user_id_field,item_id_field),
      searched_userDF,itemDefineDF)

    historyDF.write
      .format("com.databricks.spark.csv")
      .option("delimiter","#")
      .mode("overwrite")
      .save(savePath+"/User-historys")
    recoDF.write
      .format("com.databricks.spark.csv")
      .option("delimiter","#")
      .mode("overwrite")
      .save(savePath+"/User-recommendations")

    searched_userDF.unpersist()
    itemDefineDF.unpersist()

  }

}

