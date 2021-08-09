package tianyu.algorithm.recommendation

import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import tianyu.algorithm.dataprocess.DataCleansing
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{avg, col, lit, desc, pow, rank}
import org.apache.spark.sql.expressions.Window
import tianyu.algorithm.util.{WriteFile, hdfs}


/**
  * Created by lynnjiang on 2017/3/31.
  */
class CosineSimularity(spark:SparkSession,
                       outDir:String,
                       minSim:Double=0.7,
                       topSim:Int=20,
                       minCommons:Int = 5,
                       minRating:Int = 2,
                       topN:Int = 10) extends ScoreModel{

  import spark.implicits._
  private val dc = new DataCleansing(spark)

  private def writePath(filename:String) = hdfs.getPath(
    hdfs.csPath(outDir), op_time_path,filename)


  private val j_index_field:String = "j"
  private val i_feature_field:String = "i_feature"
  private val j_feature_field:String = "j_feature"
  private val i_rating_field:String = "i_rating"
  private val j_rating_field:String = "j_rating"
  private val cross_diff_product_field:String = "cross_diff_product"
  private val i_diff_square_field:String= "i_diff_square"
  private val j_diff_square_field:String = "j_diff_square"
  private val u_avg_field:String = "u_avg"
  private val simularity_field:String = "simularity"
  private val jitem_id_field:String = "j_PRODUCT_CODE"


  /**
    *For each item in product catalog,i1
    *  For each user u who rated i1, u1
    *   For each item i2 rated by user u1
    *     Record (u1,i1,i2) ---  record every two items rated by the same user
    **/
  private def getAdjustedCosSim(ratingsDF:DataFrame) = {

    val avgRatingByUserDF = ratingsDF
      .groupBy(user_index_field)
      .agg(avg(col(i_rating_field)).alias(u_avg_field))

    //find (u,i1,i2) pairs
    //return Row(u,i,i_rating,j,j_rating,u_avg_rating)
    val ijPairsDF = ratingsDF.join(ratingsDF.toDF(user_index_field,j_index_field,j_rating_field),user_index_field)
      //i!=j
      .where(col(item_index_field) =!= col(j_index_field))
      .join(avgRatingByUserDF,user_index_field)

    /**Caculate intermediate result of adjusted cosine simularity
      * dev(ui)=Rui-mean(Ru),  dev(uj)=Ruj-mean(Ru) */
    val tempVariableSDF = ijPairsDF.withColumn(i_rating_field,col(i_rating_field)-col(u_avg_field))
      .withColumn(j_rating_field,col(j_rating_field)-col(u_avg_field))
      .drop(u_avg_field)
      .withColumn(cross_diff_product_field,col(i_rating_field)*col(j_rating_field))
      .withColumn(i_diff_square_field,pow(i_rating_field,2))
      .withColumn(j_diff_square_field,pow(j_rating_field,2))
      .drop(i_rating_field,j_rating_field)

    //sum(dev(ui)*dev(uj) / ( sqrt(sum(dev(ui)^2)) * sqrt(sum(dev(uj)^2))
    val ij_SimDF = tempVariableSDF.map{
      case Row(u:Int,i:Int,j:Int,cross:Double,isquare:Double,jsquare:Double)=>
        ((i,j),(cross,isquare,jsquare,1))
    }.rdd
      .reduceByKey((pre,later) =>
        (pre._1+later._1,
          pre._2+later._2,
          pre._3+later._3,
          pre._4+later._4))
      //adjust simularity as two items with high simularity may have far Euclidean distance
      .filter(_._2._4>minCommons-1)
      .map{
        case((i,j),(cross,isquare,jsquare,count))=>
          if(isquare*jsquare==0d) (i,j,0d)
          else (i,j,cross/(math.sqrt(isquare)*math.sqrt(jsquare)))
      }.toDF(item_index_field,j_index_field,simularity_field)

    //ij_SimDF(topN)
    val window = Window.partitionBy(col(item_index_field)).orderBy(desc(simularity_field),col(j_index_field))
    ij_SimDF.filter(col(simularity_field) >= minSim)
      .withColumn(rank_field,rank.over(window))
      .where(col(rank_field) <= topSim)
      .drop(rank_field)
  }


  /**
    *Get predicted rating for items unrated by each user
    * For each user in rating records
    *  For each item rated by user u, i1
    *   For each item which is simular to i1, i2
    *     if(i2 is unrated by u) record (u,i1,rate(u,i1),i2,sim(i1,i2))
  **/
  private def predictRating(ij_topSimDF:DataFrame,ratingsDF:DataFrame,unrated:Boolean=true) = {

    val predictBaseDF = unrated match {
      case true => ratingsDF.filter(col(i_rating_field)>= minRating)
                            .join(ij_topSimDF,item_index_field)
                            //remove item j rated by user u
                            .join(ratingsDF.withColumnRenamed(item_index_field,j_index_field),
                              Seq(user_index_field,j_index_field),"leftanti")
      case false => ratingsDF.filter(col(i_rating_field)>= minRating)
                             .join(ij_topSimDF,item_index_field)
                            //select rated records for predicting existing ratings
                             .join(ratingsDF.withColumnRenamed(item_index_field,j_index_field),
                               Seq(user_index_field,j_index_field),"leftsemi")
    }



    predictBaseDF.map{
        case Row(u:Int,j:Int,i:Int,i_rating:Float,sim:Double)=>
          ((u,j),(i_rating*sim,math.abs(sim)))
      }.rdd
      .reduceByKey{(pre,later)=>
        (pre._1+later._1,pre._2+later._2)
      }.map{
      case((u,j),(cross,abs))=>
        if(abs==0) (u,j,-1d)
        else (u,j,cross/abs)
    }.toDF(user_index_field,item_index_field,prediction_field)

  }

  def getRecoFromPath(filepath:String,
                      subpath:String)={

    /**Get parsed rating dataframe (user:int,item:int,score:float)**/
    val ratingsBaseDF = getParsedRating(dc,filepath,subpath)
      .select(user_index_field,item_index_field,score_field)
    val ratingsDF = ratingsBaseDF.withColumnRenamed(score_field,i_rating_field).persist()
    num_ratings=ratingsDF.count()
    val topCosSimDF = getAdjustedCosSim(ratingsDF).persist()
    this.itemIndexDF.persist()
    num_total_items=itemIndexDF.count().toInt

    /**writeFile: Item-cossimularities**/
    val item_cossimularities = itemIndexDF.join(
      itemIndexDF.toDF(jitem_id_field,j_index_field)
        .join(topCosSimDF,j_index_field)
        .drop(j_index_field),
      item_index_field)
      .drop(item_index_field)
    writeCSV(
      item_cossimularities.withColumn(op_time_field,lit(op_time)),
      writePath("Item-cossimularities")
    )

    /**Predict ratings for items urated by users**/
    val userPreRatingsDF = predictRating(topCosSimDF,ratingsDF)
    this.userIndexDF.persist()
    val RMSE = getRMSE(predictRating(topCosSimDF,ratingsDF,false)
      .join(ratingsDF,Seq(user_index_field,item_index_field))
      .withColumnRenamed(i_rating_field,ratingCol))

    num_tran_users=userIndexDF.count().toInt
    val userPreRatings = userIndexDF.join(
      itemIndexDF.join(userPreRatingsDF,item_index_field)
        .drop(item_index_field),user_index_field)
      .drop(user_index_field).persist()
    topCosSimDF.unpersist()
    this.itemIndexDF.unpersist()
    this.userIndexDF.unpersist()
    ratingsDF.unpersist()

    //userPreRatings.persist()
    //writeFile: Rating-predictions
    writeCSV(userPreRatings.withColumn(op_time_field,lit(op_time)),
      writePath("Rating-predictions"))

    /**Make recommendations for users**/
    val window = Window.partitionBy(col(log_user_id_field)).orderBy(desc(prediction_field),col(item_id_field))
    val userRecommendations = userPreRatings.filter(col(prediction_field)>=minRating)
      .withColumn(rank_field,rank.over(window))
      .drop(prediction_field)
      .where(col(rank_field) <= topN).persist()
    //User-recommendations
    userPreRatings.unpersist()
    num_reco_users = userRecommendations.select(log_user_id_field).distinct.count.toInt
    num_reco_items = userRecommendations.select(item_id_field).distinct.count.toInt
    writeCSV(userRecommendations.withColumn(op_time_field,lit(op_time)),
      writePath("User-recommendations")
    )
    userRecommendations.unpersist()

    val recoMeasurements = Array(
      ("OP_TIME",op_time),
      ("ARGUMENTS",linkArgs(minSim, topSim, minCommons, minRating, topN)),
      ("NUM_PROCESSED_LOG", num_ratings),
      ("NUM_ALL_USERS",num_tran_users),
      ("NUM_POSSIBLE_USERS", num_reco_users),
      ("NUM_RECOMMEND_ITEMS", num_reco_items),
      ("COVERAGE", toMinusDigits(num_reco_items.toDouble/num_total_items)),
      ("RMSE",RMSE)
    ).map(measure=>(measure._1,measure._2.toString))

    writeCSV(spark.sparkContext.parallelize(
      recoMeasurements).toDF.coalesce(1), writePath("Recommendation-metrics"))

  }

}
