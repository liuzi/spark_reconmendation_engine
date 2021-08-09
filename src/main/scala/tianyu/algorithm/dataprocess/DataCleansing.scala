package tianyu.algorithm.dataprocess


/**
  * Created by lynnjiang on 2017/3/28.
  */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import tianyu.algorithm.util.hdfs

class DataCleansing(spark:SparkSession
                    //sc:SparkContext,
                    // charset:String = "UTF-8"
                   ) extends Serializable{

  import spark.sqlContext.implicits._
  import org.apache.spark.sql.functions.{desc,count,when,col}

  private val item_id_field:String = "PRODUCT_CODE"
  private val res_id_field:String = "RES_ID"

  /**Data File to data frame**/
  def dataFrame(file:String
                //, customSchema:Array[String] = DataCleansing.t_download_log
               ):DataFrame = {
    spark.sqlContext.read
      .format("com.databricks.spark.avro")
      .load(file)
  }

  def fieldTransform(baseDF:DataFrame,
                     subDF:DataFrame,
                     baseColumn:String=item_id_field,
                     linkColumn:String=res_id_field,
                     subColumn:String = "FIELD_COPY"
                    )={

    //.withColumn("PRODUCT_CODE", when(col("PRODUCT_CODE").isNull ,col("SUB_PRODUCT_CODE")).otherwise(col("PRODUCT_CODE"))))
    baseDF.join(subDF.select(col(linkColumn),col(baseColumn).alias(subColumn)),Seq(linkColumn),"left")
      .withColumn(baseColumn, when(col(baseColumn).isNull, col(subColumn)).otherwise(col(baseColumn)))
      .drop(linkColumn,subColumn)
      .filter(col(baseColumn).isNotNull)
  }

  /**
    * Cacluate some metrics to represent the data's situation.
    * Such as the percentile of missing value, the dispersion of the value.
    * @param file: String type. The path of data file
    **/

  def dataSituation(file:String
                    //, customSchema:Array[String] = DataCleansing.t_download_log
                   ) = {

    /**load raw datafile into dataframe**/
    val rawDF = dataFrame(file)
    /**The total length of data records**/
    val wholeCount = rawDF.count

    /**compute metrics of data situation**/
    val dataMetrics = rawDF.columns.map{cname =>
      val column = rawDF.select(cname)
      val notNoneColumn = column.filter(!(column(cname) <=> null) && !(column(cname) <=> ""))
      val notNoneCount = notNoneColumn.count
      val noneCount = wholeCount - notNoneCount

      val (dispersion, modePercent) = if(notNoneCount == 0){(0,0d)}
      else{
        val disperSum = notNoneColumn.groupBy(cname)
          .agg(count(cname).alias("count"))
          .orderBy(desc("count"))
          .map(r=>r.getLong(1))
          .collect

        val disperCount = disperSum.length

        (disperCount, disperSum.head.toDouble/notNoneCount)
      }

      /**Frequency of the value occurs the most**/
      (cname, noneCount.toDouble/wholeCount, dispersion, modePercent)

    }.toSeq.toDF("cname","nonePercent","dispersion","modePercent")


    /**raw dataframe and final result data frame of data situation**/
    (rawDF, dataMetrics.sort("nonePercent"))

  }
}

