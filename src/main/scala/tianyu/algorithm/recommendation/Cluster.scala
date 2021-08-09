package tianyu.algorithm.recommendation

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{desc,rank,avg}
import tianyu.algorithm.dataprocess.DataCleansing
import tianyu.algorithm.util.hdfs
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Created by lynnjiang on 2017/4/19.
  */

/**
  * Recommendation Model based on association rule
  * @param spark: SparkSession type. SparkSession created by main method.
  * @param outDir: String type. Root directory of analysis results.
  * @param k: Integer tyep. Number of clusters computed by KMeans.
  * @param maxIterations: Integer type. Maximum number of iterations to run.
  * @param topN: Integer type. How many products or resources to recommend.
  */
class Cluster(spark:SparkSession,
              //endTime:Long,
              outDir:String,
              var k:Int,
              var maxIterations: Int = 10,
              //var initializationMode: Boolean,
              //var epsilon: Double,
//              tune:Float = 3f,
              var topN:Int = 10
              ) extends ScoreModel {

  import spark.implicits._
  import org.apache.spark.sql.functions.{col,lit}
  import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
  import org.apache.spark.mllib.util.MLUtils
  import org.apache.spark.ml.linalg.SparseVector
  import org.apache.spark.ml.clustering.KMeans
  import org.apache.spark.rdd.RDD


  private val dc: DataCleansing = new DataCleansing(spark)
  private val score_sum_field:String = "score_sum"
  private var WSSSE:Double=0d

  private def writePath(file:String) = {
    hdfs.getPath(hdfs.clusterPath(outDir),"/"+op_time_path+"/"+file)
  }

  /**
    * Transform indexed rating data to rating vectors
    * @param avgRatingIndexRDD: processed rating data
    **/
  def getRatingVec(avgRatingIndexRDD: RDD[(Long,Long,Double)]) : DataFrame = {


    val useritemMatrix:RDD[MatrixEntry] = avgRatingIndexRDD.map{
      case(u, i, rating)=> MatrixEntry(u, i, rating)
    }

    val useritemLabelVec = new CoordinateMatrix(useritemMatrix)
      .toIndexedRowMatrix()
      .rows
      .toDF(label_field,feature_field)

    val useritemVecDF = MLUtils.convertVectorColumnsToML(useritemLabelVec)

    useritemVecDF
  }

  /**Cluster on users according to their ratings
    * @param useritemVecDF: User rating vector. (label, features)
    **/
  def getCluster(useritemVecDF:DataFrame): DataFrame = {


    // Trains a k-means model.
    val kmeans = new KMeans().setK(k).setFeaturesCol(feature_field)
      .setMaxIter(maxIterations)
    val kmeansModel = kmeans.fit(useritemVecDF)


    //TODO: Validation for cluster computing and recommenddation
    // Evaluate clustering by computing Within Set Sum of Squared Errors.
    this.WSSSE = kmeansModel.computeCost(useritemVecDF)
    //println(s"Within Set Sum of Squared Errors = $WSSSE")

    // FIXME: output cluster centers
    //println("Cluster Centers: ")

    this.userIndexDF.persist()
    num_tran_users = userIndexDF.count().toInt
    val userClusterDF = userIndexDF.join(
      kmeansModel.summary.predictions
      .select(label_field, feature_field,prediction_field)
      .withColumnRenamed(label_field,user_index_field)
      ,user_index_field)
      .drop(user_index_field)
    this.userIndexDF.unpersist()

    writeCSV(userClusterDF
      .withColumn(op_time_field,lit(op_time))
      .drop(feature_field),
      writePath("User-clusters"))

    /**
    val clusterCenters = kmeansModel.clusterCenters.map(_.toSparse)
    WriteFile.writeTXT(spark.sparkContext.parallelize(clusterCenters.map(_.toString)).toDF(),
      hdfs.getPath(hdfs.clusterPath(k),
      "ClusterCentersDF"))
      **/
    userClusterDF

  }

  /**Summing ratings on every items for each cluster
    * @param userClusterDF: Cluster information of users
    **/
  private def calRatingSumRDD(userClusterDF:DataFrame) = {

    val ratingSumRDD = userClusterDF.select(prediction_field,feature_field).map {
      case Row(prediction: Int, features: SparseVector) =>
        (prediction, (features.indices, features.values))
    }.rdd.reduceByKey { (pre, af) =>
      val preindices = pre._1.toSet
      val afindices = af._1.toSet
      val preMap = pre._1.zip(pre._2).map { case (k, v) => k -> v }.toMap
      val afMap = af._1.zip(af._2).map { case (k, v) => k -> v }.toMap
      val newindices = (preindices ++ afindices).toArray.sorted
      val newvalues = newindices.map { indice =>
        preMap.getOrElse(indice, 0d) + afMap.getOrElse(indice, 0d)
      }
      (newindices, newvalues)
    }

    ratingSumRDD

  }


  /**TonN recommended items for each user in Names
    *@param ratingDF: raw rating data.
    **/
  private def getRecommendations(ratingDF:DataFrame)= {

    /**Process the raw rating data**/
    val avgRatingIndexRDD = ratingDF.map{
      case Row(u:Long,i:Long,score:Float)=>
        (u,i,score.toDouble)
      case Row(u:Long,i:Long,score:Double)=>
        (u,i,score)
    }.rdd
    /**Transform raw rating data to users' rating vector**/
    val useritemVecDF = getRatingVec(avgRatingIndexRDD)
    /**Cluster information of users**/ //TODO:need parameter kmeansModel?
    /**Get predicted cluster No for each user id**/
    val userClusterDF = getCluster(useritemVecDF)

    /**rating sum for each item in each cluster**/
    val ratingSumRDD = calRatingSumRDD(userClusterDF)

    this.itemIndexDF.persist()
    num_total_items = itemIndexDF.count.toInt
    val clusterRatingsDF = ratingSumRDD.flatMap{case(prediction,(indicices,values))=>
      val length = indicices.length
      Array.fill(length)(prediction).zip(indicices).zip(values)
    }.map{case((prediction,item),scoresum)=>
      (prediction,item,scoresum,op_time)
    }.toDF(prediction_field,item_index_field,score_sum_field,op_time_field)
      .join(itemIndexDF, item_index_field)
      .drop(item_index_field)
      .select(prediction_field,item_id_field,score_sum_field,op_time_field)
    this.itemIndexDF.unpersist()
    writeCSV(clusterRatingsDF,writePath("Cluster-ratings"))

    /**Select topN items for each cluster which socre the topN highest**/
    val window = Window.partitionBy(col(prediction_field)).orderBy(desc(score_sum_field),col(item_id_field))
    val clusterTopRatingsDF = clusterRatingsDF
      //items who can be recommended should have sum of scores gt 0
      .filter(col(score_sum_field) > 0)
      .withColumn(rank_field, rank.over(window))
      .where(col(rank_field) <= topN)
      .select(prediction_field,item_id_field,score_sum_field,rank_field,op_time_field)
    writeCSV(clusterTopRatingsDF,writePath("Cluster-recommendations"))

    val userRecommendDF = userClusterDF.drop(feature_field)
      .join(clusterTopRatingsDF, prediction_field)
      .drop(prediction_field)
      .select(log_user_id_field,item_id_field,rank_field,op_time_field).persist()
    //FIXME: Dleste if this number is equal to num_tran_users
    num_reco_users = userRecommendDF.select(log_user_id_field).distinct().count.toInt
    num_reco_items = userRecommendDF.select(item_id_field).distinct.count.toInt
    writeCSV(userRecommendDF,writePath("User-recommendations"))
    userRecommendDF.unpersist()



  }


  /**Get recommended names from path
    *@param filename: File name for loading data file.
    **/
  def getRecoFromPath(filename:String,
                      subfilename:String = "t_prod_res*") = {

    //TODO replace all of the resids with productcodes
    /**Import raw rating data from data file**/
//    set_score_tune(tune)
    val parsedRatingDF = getParsedRating(dc,filename,subfilename)

    this.getRecommendations(parsedRatingDF.select(
      user_index_field,
      item_index_field,
      score_field))

    val raw_num_user = "Number of Users in the Raw Data"
    val tran_num_user = "Number of Users in the processed Data"
    val total_num_item = "Number of all of the items"
    val reco_num_user = "Number of Users Who have been recommended"
    val reco_num_item = "Number of items Recommended for All Users"
    val reco_metrics = Array(
      raw_num_user + " : " + num_raw_users,
      tran_num_user+" : "+num_tran_users,
      total_num_item+" : "+num_total_items,
      reco_num_user+" : "+num_reco_users,
      reco_num_item+" : "+num_reco_items
    )

    writeCSV(spark.sparkContext.parallelize(
      reco_metrics).toDF.coalesce(1), writePath("Recommendation-metrics"))

  }

  /**Get recommended names from multiple path, such as t_score, t_collection, t_download, etc
    *@param subpath: subordinate data used for transfroming resid to productcode
    *@param scorepath: File name of score data.
    *@param otherpaths_scores: file name and add score of other data without scores
    **/
  def getRecoFrmoMultiPath(subpath:String,
                           scorepath:String,
                           otherpaths_scores:(String,Float) *) {

    val parsedRatingDF = getMultiParsedScore(dc,subpath,scorepath,otherpaths_scores:_*).persist()
    //val window = Window.orderBy(log_user_id_field)

    num_ratings = parsedRatingDF.count()

    this.userIndexDF = getIndexDF(parsedRatingDF,log_user_id_field,user_index_field)
    this.itemIndexDF = getIndexDF(parsedRatingDF,item_id_field,item_index_field)

    //Add index of userid and productcode
    val indexedRatingDF = parsedRatingDF.join(itemIndexDF,item_id_field)
      .join(userIndexDF,log_user_id_field)

    parsedRatingDF.unpersist()


//    var lastuid=""
//    var index:Int=0
//    val indexedRatingDF = parsedRatingDF.sort(log_user_id_field)
//      .withColumn(score_field,col(score_field).cast("Float"))
//      .filter(col(log_user_id_field).isNotNull)
//      .map{
//        case Row(uid:String,iid:String,score:Float)=>
//          if(uid==lastuid) (uid,index,iid,score)
//          else {lastuid=uid
//            (uid,index+1,iid,score)}
//      }.toDF()

    this.getRecommendations(indexedRatingDF.select(
      user_index_field,
      item_index_field,
      score_field))



    //Write recommendation situations into file
//    val transform_ratings = "Number of processed ratings from multiple data sources"
//    val tran_num_user = "Number of Users in the processed Data"
//    val total_num_item = "Number of all of the items"
//    val reco_num_user = "Number of Users Who have been recommended"
//    val reco_num_item = "Number of items Recommended for All Users"

    val recoMeasurements = Array(
      ("OP_TIME",op_time),
      ("ARGUMENTS",linkArgs(k, maxIterations, topN)),
      ("NUM_PROCESSED_LOG", num_ratings),
      ("NUM_ALL_USERS",num_tran_users),
      ("NUM_POSSIBLE_USERS", num_reco_users),
      ("NUM_RECOMMEND_ITEMS", num_reco_items),
      ("COVERAGE", toMinusDigits(num_reco_items.toDouble/num_total_items)),
      ("WSSSE", toDigits(WSSSE))
    ).map(measure=>(measure._1,measure._2.toString))

//    val reco_metrics = Array(
//      transform_ratings + " : " + num_ratings,
//      tran_num_user+" : "+num_tran_users,
//      total_num_item+" : "+num_total_items,
//      reco_num_user+" : "+num_reco_users,
//      reco_num_item+" : "+num_reco_items,
//      "WSSSE"+" : "+WSSSE
//    )

    writeCSV(spark.sparkContext.parallelize(
      recoMeasurements).toDF.coalesce(1), writePath("Recommendation-metrics"))


  }



//End of class
}

