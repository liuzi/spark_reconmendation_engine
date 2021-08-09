package tianyu.algorithm.util

import java.text.SimpleDateFormat
import java.util.Date

import org.joda.time.DateTime

/**
  * Created by lynnjiang on 2017/4/12.
  */
object hdfs extends Serializable{

  //FIXME following annonations are for running on docs
  //  val url = "hdfs://node3.tianyuyun.cn:8020"
  //  val analysis_dir = "tianyu/data_analysis"
  //  val data_dir = "tianyu/rdbms"


  val url = "/Users/lynnjiang/Desktop"
  val analysis_dir = "TianYu/testdata/data_analysis"
  val data_dir = "TianYu/testdata/resource_system"


  val in_dir:String = Array(url,data_dir).mkString("/")
  val out_dir:String = Array(url,analysis_dir).mkString("/")
  private  val start = """^/+""".r
  private val end = """/+$""".r

  val nowTime = new Date()//.getTime
  val nowYear = new DateTime(nowTime).getYear
  val yesTime = new Date(nowTime.getTime - 1000l*60*60*24)
  //FIXME: prevent the situation that the file hasn't been created
  val lastHourTime = new Date(nowTime.getTime - 2*1000l*60*60)
  val simFormat = new SimpleDateFormat("yyyy/MM/dd")
  val yesDate = simFormat.format(yesTime)
  val simFormat2 = new SimpleDateFormat("yyyy/MM/dd/HH")
  val lastHourDate = simFormat2.format(lastHourTime)

  private val DateFileSuffix = yesDate+"/part*"
  private val DateHourFileSuffix = lastHourDate+"/part*"
  // /year/month/day/hour/part*
  private val HourlyFileSuffix = "/*/*/*/*/part*"

  /**remove "/" at the start and end of file name**/
  def format(file:String):String=end.replaceFirstIn(start.replaceFirstIn(file, ""),"")


  /**add prepath and datetime of reading files**/
  //final def readDatePath(file:String):String=Array(in_dir,format(file)).mkString("/")
  /**add pre path and hourly split of reading files**/
  //final def readHourlyPath(file:String):String=Array(in_dir,format(file)).mkString("/")
  //FIXME following annonations are for running on docs
  //  /**add prepath and datetime of reading files**/
  final def readDatePath(file:String):String=Array(in_dir,format(file),DateFileSuffix).mkString("/")
  final def readDateHourPath(file:String):String=Array(in_dir,format(file),DateHourFileSuffix).mkString("/")
  //  /**add pre path and hourly split of reading files**/
  final def readHourlyPath(file:String):String=Array(in_dir,format(file),HourlyFileSuffix).mkString("/")
  final def readPath(file:String):String=Array(in_dir,format(file)).mkString("/")
//  final def resultPath(rootDir:String,paths: String *) = "/"+rootDir+"/"+paths.map(format).mkString("/")

  /**add pre path of writing files**/
  /**"/" prefix for local**/
  final def writePath(file:String):String = "/"+Array(out_dir,format(file)).mkString("/")
  /**combine dir with file name**/
//  final def getPath(dir:String,file:String):String= "/"+Array(format(dir),format(file)).mkString("/")
  final def getPath(dirs:String *):String= "/"+dirs.map(format).mkString("/")

  final val scorepath = readHourlyPath("t_score")
  final val downpath = readHourlyPath("t_down")
  final val collectpath = readHourlyPath("t_collect")
  //local test
  final val subpath = readPath("t_prod_res*")
//  final val subpath = hdfs.readDateHourPath("t_prod_res*")
  final val userpath = readDatePath("t_user_daily_full")
  final val productpath = readDatePath("t_product_daily_full")
  final val accountpath = readDatePath("t_account_daily_full")


  final def tempPath(file:String):String = getPath(
    writePath("dataParsed"),
    file
  )

  final def arPath(rootdir:String, subdir:String):String ={
    getPath(format(rootdir),"Association",subdir)
  }

  final def clusterPath(rootdir:String,
                        //                    algType:String,
                        subdir:String="Basic"):String={
    getPath(format(rootdir),"Cluster",subdir)
  }

  final def mfPath(rootdir:String,
                   subdir:String="Basic"):String={
    getPath(format(rootdir),"ALS",subdir)
  }

  final def csPath(rootdir:String,
                   subdir:String="Basic"):String={
    getPath(format(rootdir),"CosSim",subdir)
  }
//  final def arPath(rootdir:String,
//                   subdir:String="Basic"):String =getPath(
//    format(rootdir)+"/"+"Association",subdir)
//
//  final def clusterPath(rootdir:String,
////                    algType:String,
//                        subdir:String="Basic"):String=getPath(
//    format(rootdir)+"/"+"Cluster",subdir)
//
//  final def mfPath(rootdir:String,
//                   subdir:String="Basic"):String=getPath(
//    format(rootdir)+"/"+"ALS", subdir)
//
//  final def csPath(rootdir:String,
//                   subdir:String="Basic"):String=getPath(
//    format(rootdir)+"/"+"CosSim",subdir)

}
