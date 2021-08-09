package tianyu.algorithm.recommendation

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.DataFrame

/**
  * Created by lynnjiang on 2017/5/8.
  */
trait CommonParams {

  protected val log_user_id_field:String = "USER_ID"
  protected val user_id_field:String = "PERSON_ID"
  protected val item_id_field:String = "PRODUCT_CODE"
  protected val res_id_field:String = "RES_ID"
  protected val user_name_field:String = "USER_NAME"
  protected val item_name_field:String = "PRODUCT_NAME"
  protected val date_field:String = "CREATE_TIME"
  protected val antecedent_field:String = "ANTECEDENT"
  protected val consequent_field:String = "CONSEQUENT"
  protected val confidence_field:String = "CONFIDENCE"
  protected val rank_field:String = "RANK"
  protected val score_field:String = "SCORE"
  protected val score_time_field:String = "SCORE_TIME"

  protected val op_time_field:String = "op_time"

  private val nowTime = new Date()//.getTime
  private val simFormat = new SimpleDateFormat("yyyyMMdd00")
  private val simFormatPath = new SimpleDateFormat("yyyy-MM-dd-00")
  protected var op_time = simFormat.format(nowTime)
  protected val op_time_path = simFormatPath.format(nowTime)

  
  final def toMinusDigits(d:Double):String = f"$d%1.16f"
  final def toDigits(d:Double):String = f"$d%10.8f"


  final def linkArgs(args:Any *):String =args.mkString("_")

  //    hdfs.arPath(outDir,
  //      minSupport.toString,
  //      minConfidence.toString,
  //      topN.toString,
  //      numPastMonths.toString,
  //      maxItems.toString,
  //      timeprocess

  final def writeTXT(file:DataFrame,path:String): Unit ={
    //TODO:remember to delete path transformation
    //val path = "/"+hdfs.getPath(writepath,filename)

    file.write
      .mode("overwrite")
      .text(path)//hdfs.getPath(writepath,filename)
  }

  final def writeCSV(file:DataFrame,path:String,split:String="#"): Unit ={
    //TODO:remember to delete path transformation
    //val path = "/"+hdfs.getPath(writepath,filename)

    file.write
      .format("com.databricks.spark.csv")
      .option("delimiter",split)
      .mode("overwrite")
      .save(path)//hdfs.getPath(writepath,filename)
  }

  final def writeAVRO(file:DataFrame,path:String): Unit ={
    //TODO:remember to delete path transformation
    //val path = "/"+hdfs.getPath(writepath,filename)

    file.write
      .format("com.databricks.spark.avro")
      .mode("overwrite")
      .save(path)//hdfs.getPath(writepath,filename)
  }

  final def writeLibsvm(file:DataFrame,path:String): Unit ={
    //TODO:remember to delete path transformation
    //val path = "/"+hdfs.getPath(writepath,filename)

    file.write
      .format("libsvm")
      .mode("overwrite")
      .save(path)//hdfs.getPath(writepath,filename)
  }
}
