package tianyu.algorithm.recommendation

import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, desc, lit, rank}
import tianyu.algorithm.dataprocess.DataCleansing
import tianyu.algorithm.util.{WriteFile, hdfs}


/**
  * Created by lynnjiang on 2017/5/2.
  */


/**
  * Recommendation Model based on association rule
  * @param spark: SparkSession type. SparkSession created by main method.
  * @param outDir: String type. Root directory of analysis results.
  * @param minScore: Double tyep. Minimum of score for predictions used for making recommendations.
  * @param matrix_rank: Double type. Rank of factors matrix for both users and items.
  * @param reg:Double type.Regularization parameter in ALS
  * @param maxIter: Maximum iterations for running als algorithm.
  * @param numBlocks: Int type. Number of blocks the users and items will be partitioned into in order to parallelize computation.
  * @param topN: Integer type. How many products or resources to recommend.
  */
class MatrixFactorization(spark:SparkSession,
                          outDir:String,
                          minScore:Double = 3,
                          matrix_rank:Int=10,
                          reg:Double = 1.0d,
                          maxIter:Int = 10,
                          topN:Int=10,
                          numBlocks:Int = 10) extends ScoreModel{


  import spark.implicits._
  import scala.collection.mutable.WrappedArray

  private val dc = new DataCleansing(spark)
  //FIXME FILE SAVING PATH
  private def writePath(filename:String) = hdfs.getPath(
    hdfs.mfPath(outDir), "/"+op_time_path+"/"+filename)


  private def createALSModel(trainData:DataFrame) = {

    val als = new ALS()
      .setRank(matrix_rank)
      .setRegParam(reg)
      .setNumBlocks(numBlocks)
      .setMaxIter(maxIter)
      .setUserCol(userCol)
      .setItemCol(itemCol)
      .setRatingCol(ratingCol)


    try{
      val alsModel = als.fit(trainData)
    } catch {case ex: ArrayIndexOutOfBoundsException =>
        println("Failed to train the ALS model, please try again")
    }


    val alsModel = als.fit(trainData)

    this.userIndexDF.persist()
    num_tran_users = userIndexDF.count().toInt

    val userFactorsDF = userIndexDF.join(
      alsModel.userFactors.withColumnRenamed("id",user_index_field),user_index_field)
      .drop(user_index_field)

    writeCSV(userFactorsDF
      .map(r=> (r.getString(0),
        r.getAs[WrappedArray[Float]](1).mkString(","))).toDF()
      .withColumn(op_time_field,lit(op_time)),
      writePath("User-factors"))
    this.userIndexDF.unpersist()

    this.itemIndexDF.persist()
    num_total_items = itemIndexDF.count().toInt
    val itemFactorsDF = itemIndexDF.join(
      alsModel.itemFactors.withColumnRenamed("id",item_index_field),item_index_field)
      .drop(item_index_field)
    writeCSV(itemFactorsDF
      .map(r=> (r.getString(0),
        r.getAs[WrappedArray[Float]](1).mkString(","))).toDF()
      .withColumn(op_time_field,lit(op_time)),
      writePath("Product-factors"))
    this.itemIndexDF.unpersist()

    //Write userFactors and itemFactors into files
    alsModel

  }


  private def oneValidation(training:DataFrame,
                            test:DataFrame)={


    val alsModel = createALSModel(training)

    val trainFrame = alsModel.transform(training)
//    WriteFile.writeCSV(trainFrame.coalesce(1),
//      hdfs.tempPath("trainDF"))

    val testFrame = alsModel.transform(test)
      .filter(col(prediction_field) =!= Float.NaN)
//    WriteFile.writeCSV(testFrame.coalesce(1),
//      hdfs.tempPath("testDF"))

    val trainErr = getRMSE(trainFrame)

    val testErr = getRMSE(testFrame)

    (trainErr,testErr)

  }

  private def crossValidation(ratingsDF:DataFrame,
                              numFold:Int = 5) = {

    import ratingsDF.sparkSession.implicits._

    val dataPartions = ratingsDF.randomSplit(Array.fill(numFold)(1f/numFold))
    val schema = ratingsDF.schema

    val dataMap = dataPartions.zipWithIndex.map{case(ds,index)=>
      index -> ds.persist()
    }.toMap

    val valiResult = (0 until numFold).map{index=>
      val test = dataMap(index)
      val training = dataMap//.filter(_._1!=index)
        //.map(_._2)
        .foldLeft(-1,
          spark.sqlContext.createDataFrame(spark.sparkContext.emptyRDD[Row],schema)){
        (pre,later)=>
          if(later._1 != index) (later._1, later._2.union(pre._2))
          else (later._1,pre._2)//pre.union(later)
      }
      oneValidation(test, training._2)
    }

    dataMap.map{
      case(index, ds)=>
        ds.unpersist()
    }

    valiResult
  }

  def ValidFromPath(filename:String,
                    subfilename:String = "t_prod_res*",
                    numFolds:Int=5,
                    trainPer:Float=0.8f
                  ) ={

    //Get parsed rating dataframe (user:int,item:int,score:float)
    val ratingsDF = getParsedRating(dc,filename,subfilename)
      .select(user_index_field,item_index_field,score_field)

    //val Array(training, test) = ratingsDF.randomSplit(Array(trainPer,1-trainPer))
    val cvResult = crossValidation(ratingsDF,numFolds)

//    val cvResult = (0 until 1).map{i=>
//      oneValidation(training,test)}

    WriteFile.writeCSV(cvResult.toDF().coalesce(1),
      hdfs.tempPath("cvResult"))
  }

  /**Use als model to predict missing ratings**/
  private def predictRating(ratingsDF:DataFrame)={

    val alsModel = createALSModel(ratingsDF)
    val RMSE = getRMSE(alsModel.transform(ratingsDF).filter(col(prediction_field)=!=Double.NaN))

    //all of the items rated by all users
    val itemSet = itemIndexDF.select(item_index_field)
      .map{
        case Row(u:Int) => u
        case Row(u:Long) => u.toInt
      }.collect()
      .toSet

    //items unrated by every user
    val newItemsDF = ratingsDF.map{
      case Row(user:Int,item:Int,score)=>
        (user,Array(item))
    }.rdd
      .reduceByKey((pre,later)=>(later.toBuffer++pre).toArray)
      .flatMap{
        case(user,items)=> val newitems = (itemSet -- items.toSet).toArray
          Array.fill(newitems.length)(user).zip(newitems)
        case _ => None
      }.toDF(user_index_field,item_index_field)

    val ratingPredictionDF = userIndexDF
      .join(alsModel.transform(newItemsDF),user_index_field)
      .drop(user_index_field)
      .filter(col(prediction_field)=!=Double.NaN && col(prediction_field)>minScore)
      .join(itemIndexDF,item_index_field)
      .drop(item_index_field)
      .select(log_user_id_field,item_id_field,prediction_field)
//    ratingPredictionDF.persist

    (ratingPredictionDF,RMSE)

  }


  //top rating
  private def getTopRating(ratingPredictionDF:DataFrame)= {

    val window = Window.partitionBy(col(log_user_id_field)).orderBy(desc(prediction_field),col(item_id_field))

    val userRecommendations = ratingPredictionDF
      .filter(col(prediction_field)>3)
      .withColumn(rank_field,rank.over(window))
      .where(col(rank_field) <= topN)

//    ratingPredictionDF.unpersist()
    userRecommendations


  }

  def RecoFromPath(filepath:String,
                   subpath:String )={

    /**Get parsed rating dataframe (user:int,item:int,score:float)**/
    val ratingsDF = getParsedRating(dc,filepath,subpath)
      .select(user_index_field,item_index_field,score_field).persist()
    num_ratings = ratingsDF.count()

    /**run alsmodel and predict new ratings**/
    val (ratingPredictionDF,preRMSE)=predictRating(ratingsDF)
    ratingsDF.unpersist()
    ratingPredictionDF.persist()
    writeCSV(ratingPredictionDF.withColumn(op_time_field,lit(op_time)),writePath("Rating-predictions"))

    /**get recommendations**/
    val userRecommendations = getTopRating(ratingPredictionDF).persist()
    //finish using ratingsDF
    ratingPredictionDF.unpersist()
    num_reco_users=userRecommendations.select(log_user_id_field).distinct().count.toInt
    num_reco_items=userRecommendations.select(item_id_field).distinct().count.toInt
    writeCSV(userRecommendations
      .drop(prediction_field)
      .withColumn(op_time_field,lit(op_time)),writePath("User-recommendations"))
    userRecommendations.unpersist()

//    val raw_num_user = "Number of Users in the Raw Data"
//    val tran_num_user = "Number of Users in the Transactions"
//    val total_num_item = "Number of all of the items"
//    val reco_num_user = "Number of Users Who have been recommended"
//    val reco_num_item = "Number of items Recommended for All Users"
//    val reco_metrics = Array(
//      raw_num_user + " : " + num_raw_users,
//      tran_num_user+" : "+num_tran_users,
//      total_num_item+" : "+num_total_items,
//      reco_num_user+" : "+num_reco_users,
//      reco_num_item+" : "+num_reco_items
//    )


    //FIXME modify RMSE
//    val RMSE = 1d

    val recoMeasurements = Array(
      ("OP_TIME",op_time),
      ("ARGUMENTS",linkArgs(matrix_rank, reg, maxIter, topN, numBlocks)),
      ("NUM_PROCESSED_LOG", num_ratings),
      ("NUM_ALL_USERS",num_tran_users),
      ("NUM_POSSIBLE_USERS", num_reco_users),
      ("NUM_RECOMMEND_ITEMS", num_reco_items),
      ("COVERAGE", toMinusDigits(num_reco_items.toDouble/num_total_items)),
      ("RMSE",preRMSE)
    ).map(measure=>(measure._1,measure._2.toString))

    writeCSV(spark.sparkContext.parallelize(
      recoMeasurements).toDF.coalesce(1), writePath("Recommendation-metrics"))

  }

}


