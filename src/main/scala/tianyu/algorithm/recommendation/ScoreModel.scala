package tianyu.algorithm.recommendation

import org.apache.spark.sql.functions.{col,lit,avg,when}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import tianyu.algorithm.dataprocess.DataCleansing
import tianyu.algorithm.util.hdfs

import scala.collection.mutable.Queue

/**
  * Created by lynnjiang on 2017/5/2.
  */
abstract class ScoreModel() extends Serializable with CommonParams{
  
  //import spark.implicits._

  //protected val dc = new DataCleansing(spark)
//  private var score_tune:Float = 0f


  protected val user_index_field:String = "u"
  protected val item_index_field:String = "i"
  protected val feature_field:String = "feature"

  protected val label_field:String = "label"
  protected val prediction_field:String = "prediction"
  protected val add_score_field:String = "ADD_SOCRE"
  private val avg_score_field = "AVG_SCORE"

  protected val userCol:String = user_index_field
  protected val itemCol:String = item_index_field
  protected val ratingCol:String = score_field

  protected var itemIndexDF:DataFrame = _
  protected var userIndexDF:DataFrame = _

  //recommendations' metrics
  protected var num_ratings = 0l
  protected var num_raw_users = 0l
  protected var num_tran_users = 0
  protected var num_total_items = 0
  protected var num_reco_users = 0
  protected var num_reco_items = 0


  /**
    *Change the data type of columns in a DataFrame
    * @param df: DataFrame type
    * @param columnQueue: Queue of columns needed to be changed their types.
    * @param datatype: String type. What type are the columns going to be transformed to.
    **/
  protected def changeColType(df:DataFrame,
                    columnQueue:Queue[String],
                    datatype:String="String"):DataFrame={

    if(columnQueue.isEmpty) df
    else{
      val column: _root_.scala.Predef.String = columnQueue.dequeue()
      changeColType(df.withColumn(column,df.apply(column).cast(datatype)),
        columnQueue,
        datatype)
    }
  }


  /**
    *Root mean square error between ratings and predictions
    * @param df: DataFrame type. Dataframe contains rating and prediction column.
    **/
  protected def getRMSE(df:DataFrame):Double = {

    import df.sqlContext.implicits._

    val caldf = df.select(ratingCol,prediction_field)

    val MSE = changeColType(caldf,Queue(caldf.columns:_*),"Double")
      .map{
        case Row(rating:Double,prediction:Double)=> (rating,prediction)
//        case Row(rating:Float,prediction:Float)=> (rating.toDouble,prediction.toDouble)
      }.rdd
      .map{case(rating,prediction)=>
        math.pow(prediction-rating,2)
      }.reduce(_+_)/df.count
    math.sqrt(MSE)
  }

  /**
    *Add indexes to ids
    * @param rawDF: DataFrame type, DataFrame of ids.
    * @param rawfield: String type, field name of id.
    * @param indexfield: String type, field name of index.
    * @return DataFrame of both of ids and indexes and average scores.
    **/
  def getIndexDF(rawDF:DataFrame,
                 rawfield:String,
                 indexfield:String):DataFrame =  {

    import rawDF.sparkSession.implicits._

//    changeColType(rawDF.select(rawfield),Queue(rawfield),"String")
//      .distinct()
//      .map(_.getString(0))
//      .rdd.zipWithIndex
//      .toDF(rawfield,indexfield)

    rawDF.select(rawfield)
      .distinct()
      .map(_.getString(0))
      .rdd.zipWithIndex
      .toDF(rawfield,indexfield)
  }

  //case class Rating(user:Int, item:Int, Rating:Float)

  /**
    *Index the user and item
    * Average the rating
    * @param ratingRawDF: DataFrame type, raw rating DataFrame
    **/
  private def getAvgRatingIndexDF(ratingRawDF: DataFrame) ={

    import ratingRawDF.sparkSession.implicits._

    /**Indexed user_id and res_id dataframe**/
    //TODO:Transform res_id into product_code
    this.userIndexDF = getIndexDF(ratingRawDF,
      log_user_id_field,
      user_index_field)

    this.itemIndexDF = getIndexDF(ratingRawDF,
      item_id_field,
      item_index_field)

    /**Replace ids with indexes**/
    val ratingIndexDF = ratingRawDF
      .join(userIndexDF,log_user_id_field)
      .join(itemIndexDF,item_id_field)


    val cols:Array[String] = Array(user_index_field,log_user_id_field,item_index_field,item_id_field,score_field)
    /**Average rating of each item rated by the same user**/
    val avgRatingIndexDF = changeColType(
      changeColType(ratingIndexDF,Queue(score_field),"Float"),
      Queue(user_index_field,item_index_field),"Int")
      .select(cols.map(col(_)):_*)
      /**Filter out null value**/
      .filter(!(col(score_field) <=> null) && !(col(score_field) <=> ""))
      .map{ case Row(u:Int,user:String,i:Int,item:String,rating:Float)=>
        ((u,user,i,item),(rating,1))}
      .rdd.reduceByKey{(pre,after)=>
        (pre._1+after._1,pre._2+after._2)}
      .map{case((u,user,i,item),(ratingsum,ratingcount))=>
        (u,user,i,item,ratingsum.toFloat/ratingcount)
      }.toDF(cols:_*)

    //FIXME PARSED DATA
//    writeCSV(avgRatingIndexDF,
//      hdfs.tempPath("Rating-parsed"),",")

    avgRatingIndexDF

  }


  /**replace all of the resids with productcodes**/
  protected def getParsedRating(dc:DataCleansing,
                                filepath:String,
                                subpath:String = "t_prod_res*") ={
    //TODO replace all of the resids with productcodes
    /**Import raw rating data from data file**/
    val baseDF = dc.dataFrame(filepath)
      .select(log_user_id_field,item_id_field,res_id_field,score_field)
//      .persist()
//    num_raw_users = baseDF.select(log_user_id_field).distinct().count().toInt
    //TODO wait for being deleted. Compared to parsed rating data.

    val subDF = dc.dataFrame(subpath)
      .select(res_id_field,item_id_field)

    val ratingRawDF = dc.fieldTransform(baseDF,subDF,item_id_field,res_id_field)
      .select(log_user_id_field,item_id_field,score_field)

//    baseDF.unpersist()

    getAvgRatingIndexDF(ratingRawDF)

  }

//  protected def set_score_tune(tune:Float):this.type = {
//    this.score_tune = tune
//    this
//  }


  //FIXME: get index dataframe of MultiScore

  /**Score data from t_score,t_collection,t_download**/
  protected def getMultiParsedScore(dc:DataCleansing,
                                    subpath:String,
                                    scorepath:String,
                                    otherpaths_scores:(String,Float) *):DataFrame = {

    //FIXME replace all of the resids with productcodes
    /**Import raw rating data from data file**/

    val scoreCols = Array(log_user_id_field,res_id_field,item_id_field,score_field,score_time_field).map(col)
    val otherCols = Array(log_user_id_field,res_id_field,item_id_field,date_field).map(col)

    val scorebaseDF = dc.dataFrame(scorepath).select(scoreCols:_*).withColumnRenamed(score_time_field,date_field)
    import scorebaseDF.sparkSession.implicits._
    //FIXME : number of users in raw logs    persist user list
   //num_raw_users = num_raw_users + scorebaseDF.select(log_user_id_field).na.drop(log_user_id_field).distinct().count()

    val subDF = dc.dataFrame(subpath).select(res_id_field,item_id_field).persist()

    //Ensoure score_field is the float type
    val scoreRawDF = dc.fieldTransform(scorebaseDF,subDF)
      .filter(col(log_user_id_field).isNotNull)
      .withColumn(score_field,col(score_field).cast("Float"))

    val scoreDF = scoreRawDF.drop(date_field)
      .map{case Row(uid:String,iid:String,score:Float)=>
      ((uid,iid),(score,1))
    }.rdd.reduceByKey{(pre,later)=>
      (pre._1+later._1,pre._2+later._2)
    }.map{case((uid,iid),(scoresum,count))=>
      (uid,iid,scoresum/count)
    }.toDF(log_user_id_field,item_id_field,score_field)
    .persist()

    val otherDF = otherpaths_scores.map{case(path,score)=>
      //FIXME scorefield may need to be changed
      dc.fieldTransform(dc.dataFrame(path).select(otherCols:_*),subDF)
        .filter(col(log_user_id_field).isNotNull)
        .drop(date_field)
        .withColumn(add_score_field,lit(score))
    }
//      .map(df=>
//      changeColType(df,Queue(log_user_id_field,item_id_field))
//        .withColumn(add_score_field,col(add_score_field).cast("Float"))
//    )
      .reduce((pre,later)=>pre.union(later))
      .map{case Row(uid:String,iid:String,addscore:Float)=>
        ((uid,iid),addscore)
    }//sum the add score from other log files
      .rdd.reduceByKey((pre,later)=>pre+later)
      .map{case((u,i),score)=>(u,i,score)}
      .toDF(log_user_id_field,item_id_field,add_score_field)

    subDF.unpersist()


    //Average scores for each user from t_score data
    val userAvgScoreDF = scoreDF.groupBy(log_user_id_field).agg(avg(col(score_field)).alias(avg_score_field))
    //score: socre - avgscore
    val multiScoreDF = scoreDF.join(userAvgScoreDF,log_user_id_field)
      .join(otherDF, Seq(log_user_id_field,item_id_field), "outer")
      .withColumn(score_field, when(col(score_field).isNotNull ,
        col(score_field) - col(avg_score_field)).otherwise(col(add_score_field)))
      .drop(avg_score_field,add_score_field)

    scoreDF.unpersist()
    multiScoreDF

    //multiScoreDF.map{case Row(uid,iid)}

  }

  //protected def oneValidation(train:DataFrame,test:DataFrame)
  //def crossValidation(df:)
  //protected def RecoFrmPath()

}
