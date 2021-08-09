package tianyu.algorithm.recommendation

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat

import org.apache.spark.sql.{Column, Row, SparkSession}
import org.joda.time.DateTime
import tianyu.algorithm.dataprocess.DataCleansing
import tianyu.algorithm.util.hdfs
import tianyu.algorithm.util.WriteFile


/**
  * Created by lynnjiang on 2017/3/16.
  */

/**
  * Recommendation Model based on association rule
  * @param spark: SparkSession type. SparkSession created by main method.
  * @param outDir: String type, root path for saving the ar analysis results.
  * @param timeProcess: whther use time-processing on data
  * @param numPastMonths: Int type, the length of the same period for every year. Used for cut up data on time line.
  * @param tranLength: Int type, length of days to divide the transaction
  * @param maxItems: Int type, max items of normal transactions
  * @param minSupport: Double type. The minimum of support degree, for filtering out frequent items.
  * @param minConfidence: Double type. The minimum of confidence degree, for filtering out rules.
  * @param topN: Integer type. How many products or resources to recommend.
  * @param numPartitions: Integer type. The number of partitions used to distribute the work.
  */
class AssociationRule(spark:SparkSession,
                      //var charset:String = "UTF-8",
                      //userfile:String = "T_USER/2017/04/12/part*",
                      //0itemfile:String = "T_PRODUCT/2017/04/12/part*",
                      //var dirname:String = "download",
                      //var recentPeriod:Integer = 180,
                     //TODO delete testTime wchich is only for test
//                      testTime:Long,
                      outDir:String = "",
                      timeProcess:Boolean,
                      endTime:Long,
                      numPastMonths:Int = 1,
                      tranLength:Int = 4,
                      var maxItems:Int = 100,
                      var minSupport:Double = 0.2,
                      var minConfidence:Double = 0.8,
                      var topN:Integer = 10,
                      var numPartitions:Integer = 10
                     ) extends Serializable with CommonParams{

  import org.apache.spark.sql.expressions.Window
  import org.joda.time.format.DateTimeFormat
  import org.apache.spark.sql.functions.{col,rank, desc,lit,avg,udf,dayofyear,year}
  import org.apache.spark.rdd.RDD
  import org.apache.spark.sql.DataFrame
  import org.apache.spark.mllib.fpm.FPGrowth
  import java.sql.Timestamp
  import spark.implicits._

  private val dc = new DataCleansing(spark)
//  private val nowTime = nowTime
  private val nowTime = new java.util.Date(endTime)
  private val nowTimeString = DateTimeFormat.forPattern("YYYY-MM-dd-00").print(endTime)
  op_time = DateTimeFormat.forPattern("yyyyMMdd00").print(endTime)
  private val nowYear = new DateTime(nowTime).getYear
  //preTime = current - pastTimes/2
  private val preMonthTime = new DateTime(new Date(nowTime.getTime-500l*60*60*24*30*numPastMonths))
  private val start = preMonthTime.getDayOfYear



  private val avg_field = "avg"
  private val date_string_field = "DATE"

  //recommendations' metric field
//  private val raw_num_user = "Number of Users in the Raw Data"
//  private val tran_num_user = "Number of Users in the Transactions"
//  private val reco_num_user = "Number of Users Who have been recommended"
//  private val reco_num_item = "Number of items Recommended for All Users"

  /**Variables store the computing results**/
  //var hisTranNameDF:DataFrame = _
  //var singleFrequentDF:DataFrame = _
  var recommendNameDF:DataFrame = _
  private var num_all_items = 0l

  private val subdir = timeProcess match {
    case true => "Time_Window"
    case false => "Full_History"
  }
  private def writePath(file:String) = {
    hdfs.getPath(hdfs.arPath(outDir,subdir),nowTimeString+"/"+file)
  }


  private def longToDateUDF = udf((time:Long)=>
    DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss").print(time)
  )


  /**Every month**/
  private def getDateParsedDF(df:DataFrame,
                              timeField:String = date_field) = {

    val day_field = "Day"
    //current + pastTimes/2
    val nowTimeEnd = new DateTime(endTime + 500l*60*60*24*30*numPastMonths)
    val wholeEnd = nowTimeEnd.getDayOfYear
    val nowTime = new DateTime(endTime)
    val end = nowTimeEnd.getDayOfYear

    val parsedDF = df.withColumn(date_string_field,longToDateUDF(col(timeField)))
      .drop(date_field)
      .withColumn(day_field,dayofyear(col(date_string_field)))
      //if start < end
      //FIXME YEAR<CURRENT YEAR
      .filter(
      year(col(date_string_field)) <= nowYear &&
        (if(start<wholeEnd)  col(day_field).between(start,wholeEnd)
        else col(day_field)> start||col(day_field)<wholeEnd))
      //now year, day must be before current day
      .filter(year(col(date_string_field))=!=nowYear || col(day_field) < end)
      .drop(day_field)


    parsedDF
  }


//  private def getAllBehaviors(baseCols:Seq[Column],
//                              subCols:Seq[Column],
//                              finalCols:Seq[Column]):DataFrame = {
//
//    var rawDFCount = 0l
//
//
//
//  }

  private def getMultiTransaction(scorepath:String ,
                                  otherpaths:Seq[String],
                                  subpath:String )={


    var rawDFCount = 0l


    val baseCols = Seq(log_user_id_field,item_id_field,res_id_field).map(col)
    val subCols = Seq(res_id_field,item_id_field).map(col)
    val finalCols = Seq(log_user_id_field,res_id_field,item_id_field).map(col)

    val scoreRawDF = dc.dataFrame(scorepath).persist()
    rawDFCount = rawDFCount + scoreRawDF.count()
    val scoreBaseDF = scoreRawDF
      .select(finalCols ++ Seq(col(score_field)):_*)
    scoreRawDF.unpersist()

    //TODO subDF read the newest t_pro_res data
    val subDF = dc.dataFrame(subpath).select(subCols:_*).persist()
    val scoreDF = dc.fieldTransform(scoreBaseDF,subDF).persist()
    val avgDF = scoreDF.select(log_user_id_field,item_id_field,score_field)
      .groupBy(log_user_id_field)
      .agg(avg(col(score_field)).alias(avg_field))
    val scorePartDF = scoreDF.join(avgDF,log_user_id_field)
      .withColumn(score_field,col(score_field)-col(avg_field))
      .drop(avg_field)
      .filter(col(score_field)>0)
      .drop(score_field)
      .filter(col(log_user_id_field).isNotNull && col(item_id_field).isNotNull)
    scoreDF.unpersist()

    var allBehaviorDF:DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row],scorePartDF.schema)

    //FIXME  use reduce to union dataframe instead
    otherpaths.foreach{path=>

      val rawDF = dc.dataFrame(path).persist()
      rawDFCount = rawDFCount + rawDF.count
      val baseDF = rawDF.select(finalCols:_*)
      rawDF.unpersist()

      val partDF = dc.fieldTransform(baseDF,subDF)
        .filter(col(log_user_id_field).isNotNull && col(item_id_field).isNotNull)

      allBehaviorDF = allBehaviorDF.union(partDF)

    }
    subDF.unpersist()

    allBehaviorDF = allBehaviorDF.distinct.persist()
    this.num_all_items = allBehaviorDF.select(item_id_field).distinct().count()


    val muiltiTranRDD = allBehaviorDF
      .distinct()
      .map{case Row(u:String, i:String)=>
        (u,Array(i))
      }.rdd.reduceByKey{(pre,later)=>
      (pre.toBuffer ++ later).toArray
    }.map(_._2).filter(t=>t.length>1 && t.length<maxItems)


    allBehaviorDF.unpersist()

    //    writeCSV(muiltiTranRDD.map(_.mkString(",")).toDF,writePath("User_day_transactions"))


    (allBehaviorDF.distinct(),muiltiTranRDD,rawDFCount)

  }


  private def getMultiTransactionbyDate(scorepath:String ,
                          otherpaths:Seq[String],
                          subpath:String )={


    var rawDFCount = 0l


    val baseCols = Seq(log_user_id_field,item_id_field,res_id_field,date_field).map(col)
    val subCols = Seq(res_id_field,item_id_field).map(col)
    val finalCols = Seq(log_user_id_field,res_id_field,item_id_field,date_string_field).map(col)

    val scoreRawDF = dc.dataFrame(scorepath).persist()
    rawDFCount = rawDFCount + scoreRawDF.count()
    val scoreBaseDF = getDateParsedDF(scoreRawDF,score_time_field)
      .withColumnRenamed(score_time_field,date_field)
      .select(finalCols ++ Seq(col(score_field)):_*)
    scoreRawDF.unpersist()

    //TODO subDF read the newest t_pro_res data
    val subDF = dc.dataFrame(subpath).select(subCols:_*).persist()
    val scoreDF = dc.fieldTransform(scoreBaseDF,subDF).persist()
    val avgDF = scoreDF.select(log_user_id_field,item_id_field,score_field)
      .groupBy(log_user_id_field)
      .agg(avg(col(score_field)).alias(avg_field))
    val scorePartDF = scoreDF.join(avgDF,log_user_id_field)
      .withColumn(score_field,col(score_field)-col(avg_field))
      .drop(avg_field)
      .filter(col(score_field)>0)
      .drop(score_field)
      .filter(col(log_user_id_field).isNotNull && col(item_id_field).isNotNull)
    scoreDF.unpersist()
      //.select(finalCols:_*)

    var allBehaviorDF:DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row],scorePartDF.schema)


    //FIXME  use reduce to union dataframe instead
    otherpaths.foreach{path=>

      val rawDF = dc.dataFrame(path).persist()
      rawDFCount = rawDFCount + rawDF.count
      val baseDF = getDateParsedDF(rawDF).select(finalCols:_*)
      rawDF.unpersist()

      val partDF = dc.fieldTransform(baseDF,subDF)
        .filter(col(log_user_id_field).isNotNull && col(item_id_field).isNotNull)

      allBehaviorDF = allBehaviorDF.union(partDF)

    }
    subDF.unpersist()

    allBehaviorDF = allBehaviorDF.distinct.persist()
    this.num_all_items = allBehaviorDF.select(item_id_field).distinct().count()
    val numProcessDF = allBehaviorDF.count
    numProcessDF match {
      case 0l => throw new IndexOutOfBoundsException("Not enough data till the date you specify for getting " +
        "transactions" + nowTimeString)
      case _ => None
    }

    val days_long = 1000l*60*60*24*tranLength

    val muiltiTranRDD = allBehaviorDF
      .distinct()
      .map{case Row(u:String, i:String, date:String)=>
        ((u,Timestamp.valueOf(date).getTime/days_long),(Array(i),1))
      }.rdd
      .reduceByKey{(pre,later)=>
        ((pre._1.toBuffer ++ later._1).toArray,
          pre._2+later._2)
      }
      //Remove abnormal transactions
      .filter(t=>t._2._2<maxItems && t._2._2>1)
      .map(_._2._1.distinct)

    val end = new DateTime(nowTime).getDayOfYear
    val nowTimeEnd = new DateTime(endTime + 500l*60*60*24*30*numPastMonths)
    val wholeEnd = nowTimeEnd.getDayOfYear

    val historyDF = allBehaviorDF.filter(
      if(start< end) year(col(date_string_field))===nowYear && dayofyear(col(date_string_field)).between(start,end)
      else (year(col(date_string_field))===nowYear-1 && dayofyear(col(date_string_field))>start) ||
        (year(col(date_string_field))===nowYear && dayofyear(col(date_string_field))<end)
    )
    val futureDF = allBehaviorDF.filter(
      if(end< wholeEnd) year(col(date_string_field))===nowYear && dayofyear(col(date_string_field)).between(end,wholeEnd)
      else (year(col(date_string_field))===nowYear-1 && dayofyear(col(date_string_field))>end) ||
        (year(col(date_string_field))===nowYear && dayofyear(col(date_string_field))<wholeEnd)
    )

    allBehaviorDF.unpersist()

//    writeCSV(muiltiTranRDD.map(_.mkString(",")).toDF,writePath("User_day_transactions"))


    (futureDF,historyDF,muiltiTranRDD,rawDFCount,numProcessDF)

  }


  /**
    * Get list of users and all of the transactions from users' behavior logs
    * @param rawDF: DataFrame type. The file path for loading data.
    * @param write: Whether to write the transaction results into files.
    **/
  private def getTransaction(rawDF:DataFrame,
                             write:Boolean = true) : RDD[(String,Array[String])] = {

    /**Extract columns of users and items from raw datafrme**/
    val useritemRDD = rawDF.filter(col(item_id_field) =!= "")
      .sort(log_user_id_field).select(log_user_id_field,item_id_field)
      .map(row=>(row.getString(0),Array(row.getString(1))))
      .rdd

    /**Get transactions of all of the users**/
    val userTransactionRDD = useritemRDD.reduceByKey{(pre,after)=>
      val preBuffer = pre.toBuffer
      after.map(preBuffer+=_)
      preBuffer.toArray
    }.map{case(user,transaction)=>
      (user, transaction.distinct)
    }

    /**Write Transactions into csv files**/
//    if(write){
//      writeTXT(userTransactionRDD
//        .map{case(user,transaction)=>
//          user+" : "+transaction.mkString(",")
//        }.toDF(),"/FullTransactions")
//    }

    /**Assign to users and transactions of this class**/
    //this.userTransactions=userTransactionRDD
    userTransactionRDD

  }

  /**
    * Get association rules
    * @param transactions: user transactions for computing frequents and association rules
    * @return : RDD of frequencies and association rules
    **/
  private def getAssociationRule(transactions:RDD[Array[String]]) = {

    val fpg = new FPGrowth()
      .setMinSupport(minSupport)
      .setNumPartitions(numPartitions)

    val fpgmodel = fpg.run(transactions)

    /**Write frequent items into files**/
    val frequentsRDD = fpgmodel.freqItemsets.map { itemset =>
      (itemset.items.sorted, itemset.freq)
    }

    //Write frequents whose length are less than 10 into files
//    writeCSV(
//      frequentsRDD.filter(_._1.length<10)
//      .map{case(items,freq)=>
//        (items.mkString(","),items.length,freq,op_time)
//    }.toDF(),writePath("Frequents"))


    /**Get collections of Association Rules **/
    val ruleClass = fpgmodel.generateAssociationRules(minConfidence)
    val rulesRDD = ruleClass.map { rule =>
      (rule.antecedent.sorted, rule.consequent.sorted, rule.confidence)
    }
    //this.rules = rulesRDD

    /**Write association rules into files**/
//    writeCSV(rulesRDD.map{case(ante,conse,conf)=>
//      (ante.mkString(","),
//        conse.mkString(","),
//        conf,
//        op_time)
//    }.toDF(),writePath("Rules"))

    (rulesRDD,frequentsRDD)
  }

  /**
    * Get recommendation from rules
    * @param rulesRDD: association rules
    * @param historyDF: user behavior history
    * @param frequentsRDD: Frequents items
    * @return : Recommendation Result with userID and productID
    **/
  //TODO:Modified get recommendID from association rules
  private def recommendFromRules(rulesRDD:RDD[(Array[String],Array[String],Double)],
                                 historyDF:DataFrame,
                                 frequentsRDD:RDD[Array[String]],
                                 min_collect_confidence:Double = 0.92):DataFrame= {


    //FIXME add rules collect recommendation
//    val rulesArray = rulesRDD.filter(r=>r._1.length>3 && r._3>min_collect_confidence)
//      .collect()
//      .map{r=>(r._1.toSet,r._2(0),r._3)}
//    val rulesArrayRecommendRDD =  historyDF.select(log_user_id_field,item_id_field)
//      .map{case Row(user:String,item:String)=>
//        (user,Array(item))
//      }.rdd.reduceByKey{(pre,later)=>
//      (pre.toBuffer ++ later).toArray
//    }.flatMap{case(user,items)=>
//        val recoItems = rulesArray.map{r=>
//          if(r._1.subsetOf(items.toSet)) (r._2,r._3)
//          else ("",0d)
//        }.filter(_._2>0)
//        Array.fill(recoItems.length)(user).zip(recoItems)
//    }.map{case(user,(item,confidence))=>
//      ((user,item),confidence)
//    }
      //.distinct.toDF(log_user_id_field,item_id_field,confidence_field)



    //Filter out rules whose antecedent's length is less than 4
    val rulesFilterDF = rulesRDD.filter(_._1.length<4).map{
      case(antecedent,consequent,confidence)=>
        (antecedent.mkString(","),consequent.mkString(","),confidence)
    }.toDF(antecedent_field,consequent_field,confidence_field)

    val frequentsFilterDF = frequentsRDD.filter(_.length < 4)
                                        .map(_.mkString(","))
                                        .toDF(antecedent_field)

    //1-length subsets
    val oneSubsetFilterDF = historyDF.join(frequentsFilterDF.toDF(item_id_field),item_id_field)
      .select(log_user_id_field,item_id_field)

    //2-length subsets
    val twoTransactions = getTransaction(oneSubsetFilterDF,false)
    val twoSubsetDF = twoTransactions.flatMap{case(user,items)=>
        val twoSubsets = items.toSet.subsets(2)
            .map(_.toArray.sorted.mkString(","))
            .toArray
        Array.fill(twoSubsets.length)(user).zip(twoSubsets)
    }.toDF(log_user_id_field,antecedent_field)
    val twoSubsetFilterDF = twoSubsetDF.join(frequentsFilterDF,antecedent_field)
      .select(log_user_id_field,antecedent_field)

    //3-length subsets
    val threeTransactions = getTransaction(
      twoSubsetFilterDF.flatMap{
        case Row(user:String,antecedent:String)=> {
          val items = antecedent.split(",")
          Array.fill(2)(user).zip(items)
        }
        case _ => None
      }.toDF(log_user_id_field,item_id_field),false)

    val threeSubsetDF = threeTransactions.flatMap{case(user,items)=>
      val threeSubsets = items.toSet.subsets(3)
        .map(_.toArray.sorted.mkString(","))
        .toArray
      Array.fill(threeSubsets.length)(user).zip(threeSubsets)
    }.toDF(log_user_id_field,antecedent_field)
    val threeSubsetFilterDF = threeSubsetDF.join(frequentsFilterDF,antecedent_field)
      .select(log_user_id_field,antecedent_field)

    //union 1-lenth, 2-length, 3-length subset of users' transactions
    val subsetFilterDF = threeSubsetFilterDF
      .union(twoSubsetFilterDF)
      .union(oneSubsetFilterDF)

    writeCSV(subsetFilterDF
      .withColumn(op_time_field
      ,lit(op_time)),
      writePath("User-frequent-priors"))

    //Primary recommend DF(user,antecedent,consequent,confidence)
    val allRecommendDF= subsetFilterDF.join(rulesFilterDF,antecedent_field)
      .select(log_user_id_field,consequent_field,confidence_field)
      .flatMap{
      case Row(user:String, consequent:String, confidence:Double)=>{
        val recos = consequent.split(",")
        val length = recos.length
        Array.fill(length)(user).zip(recos).zip(Array.fill(length)(confidence))
      }
      //case _ => None
    }.rdd
//      .union(rulesArrayRecommendRDD)
//      .distinct()
      .reduceByKey{(pre,after)=>
      if (pre>after) pre
      else after
    }.map{case((user, recos),confidence)=>(user,recos,confidence)}
     .toDF(log_user_id_field,item_id_field,confidence_field)

      //Remove items in the recommended sets if user has been interacting with
    //FIXME remove recent historical items from recommendations
    .join(historyDF,Seq(log_user_id_field,item_id_field),"leftanti")
    //TODO GO ON MODIFYING


    val window = Window.partitionBy(col(log_user_id_field)).orderBy(desc(confidence_field),col(item_id_field))
    val recommendIDDF = allRecommendDF.withColumn(rank_field, rank.over(window)).where(col(rank_field) <= topN)
      .select(log_user_id_field, item_id_field, rank_field)

    /**Write recommendations with ID into files**/
    writeCSV(recommendIDDF.withColumn(op_time_field,lit(op_time)), writePath("User-recommendations"))

    recommendIDDF

  }


//TODO: delete or modify a month later from 0605
//  /**Change recommended ids to names.
//    * Here invoke all of the relevant functions above.
//    * @param rawDF: raw user-behavior data
//    **/
//  def getRecommendations(rawDF:DataFrame) {
//
//
//    val historyDF = rawDF.select(log_user_id_field,item_id_field)
//
//    //How many users in the raw user-behavior logs
//    val rawUserNum = historyDF.select(log_user_id_field).distinct().count()
//
//    /**Invoke previous functions to make recommendations with ID**/
//    val transactions = this.getTransaction(historyDF)
//      .map{case(user,transaction)=>transaction}
//    //How may users in transactions transformed from raw user-behavoir logs
//    val filterUserNum = transactions.count()
//
//    /**Get association rules**/
//    val (rulesRDD,frequentsRDD) = this.getAssociationRule(transactions)
//
//    /**Get the past recent-period days data of logs**/
//    //time relevant processing
//
//    val nowTime = new Timestamp(this.nowTime.getTime)
//    /**history user-behavior datarame and save the dataframe as a variable**/
//
//
//    /**Get recommendation according to rules and historical transactions**/
//    val recommendIDDF = this.recommendFromRules(rulesRDD,historyDF,frequentsRDD)
//    //How many users are exactly recommended with some items
//    val userRecoNum = recommendIDDF.select(user_id_field).distinct().count()
//    val itemRecoNum = recommendIDDF.count()


//    /**The time when the recommendations are created**/
//
//
//    //Measurements of recommendations using association rules this time
//    val recoMeasurements = Array(
//      raw_num_user+" : "+rawUserNum,
//      tran_num_user+" : "+filterUserNum,
//      reco_num_user+" : "+userRecoNum,
//      reco_num_item+" : "+itemRecoNum
//    )
//
//    writeCSV(spark.sparkContext.parallelize(
//      recoMeasurements).toDF.coalesce(1), writePath("RecommendMetrics"))
//
//  }


  def getRecoMultibyDate(subpath:String,
                         scorepath:String,
                         otherpaths:String *) = {

    val (futureDF, historyDF,multiTranRDD,num_rawlogs,num_prologs) =
      getMultiTransactionbyDate(scorepath,otherpaths,subpath)

    //TODO remove this test code later
    val aa =futureDF.count
    assert(aa!=0, "future wrong" + aa)

    historyDF.persist()
    historyDF.count() match {
      case 0l => throw new IndexOutOfBoundsException("Not enough data till the date you specify" + nowTimeString)
      case _ => None
    }
    //write historyfiles
    writeCSV(historyDF,writePath("User-history-logs"))

    multiTranRDD.persist()
    val num_multi_tran = multiTranRDD.count()
    val (rulesRDD,frequentsRDD) = this.getAssociationRule(multiTranRDD)
    multiTranRDD.unpersist()


    frequentsRDD.persist()
    writeCSV(
      frequentsRDD.filter(_._1.length<10)
        .map{case(items,freq)=>
          (items.mkString(","),items.length,freq,op_time)
        }.toDF(),writePath("Frequents"))

    rulesRDD.persist()
    writeCSV(rulesRDD.map{case(ante,conse,conf)=>
      (ante.mkString(","),
        conse.mkString(","),
        conf,
        op_time)
    }.toDF(),writePath("Rules"))


    val recoDF = this.recommendFromRules(
      rulesRDD,
      historyDF.drop(date_string_field),
      frequentsRDD.map(_._1))
    //TODO delete the part which is only for test
    val num_his_users= historyDF.select(log_user_id_field).distinct().count
    historyDF.unpersist()
    rulesRDD.unpersist()
    frequentsRDD.unpersist()

    recoDF.persist()
    val num_reco_users = recoDF.select(log_user_id_field).distinct.count()
    val num_reco_distinct_items = recoDF.select(item_id_field).distinct().count()
    val num_reco_items = recoDF.count()
    val actuleLikeDF = futureDF.select(log_user_id_field,item_id_field).distinct().persist()
    val Ntp = recoDF.join(actuleLikeDF,Seq(log_user_id_field,item_id_field),"leftsemi").count
    val precision = Ntp.toDouble/num_reco_items
    val recall = Ntp.toDouble/actuleLikeDF.count()
    val coverage = num_reco_distinct_items.toDouble/this.num_all_items
    actuleLikeDF.unpersist
    recoDF.unpersist()



    val recoMeasurements = Array(
      ("OP_TIME",op_time),
      ("ARGUMENTS",linkArgs(minSupport,minConfidence,numPastMonths,maxItems,topN,numPartitions)),
      ("NUM_RAW_LOGS", num_rawlogs),
      ("NUM_PROCESSED_LOG", num_prologs),
      ("NUM_ACTION_TRANSACTIONS", num_multi_tran),
      ("NUM_ALL_USERS",num_his_users),
      ("NUM_POSSIBLE_USERS", num_reco_users),
      ("NUM_RECOMMEND_ITEMS", num_reco_distinct_items),
      ("PRECISION" , precision),
      ("RECALL" ,recall),
      ("COVERAGE" , toMinusDigits(coverage))
    ).map(measure=>(measure._1,measure._2.toString))

    writeCSV(spark.sparkContext.parallelize(
      recoMeasurements).toDF.coalesce(1), writePath("Recommendation-metrics"))

  }


  def getRecoMulti(subpath:String,
                   scorepath:String,
                   otherpaths:String *
                  ) = {


    val (historyDF,multiTranRDD,num_rawlogs) = getMultiTransaction(scorepath,otherpaths,subpath)
    historyDF.persist()

    val numHistory = historyDF.count
    //write historyfiles
    writeCSV(historyDF,writePath("User-history-logs"))

    multiTranRDD.persist()
    val numTranRDD = multiTranRDD.count
    val (rulesRDD,frequentsRDD) = this.getAssociationRule(multiTranRDD)
    multiTranRDD.unpersist()
    frequentsRDD.persist()
    writeCSV(
      frequentsRDD.filter(_._1.length<10)
        .map{case(items,freq)=>
          (items.mkString(","),items.length,freq,op_time)
        }.toDF(),writePath("Frequents"))

    rulesRDD.persist()
    writeCSV(rulesRDD.map{case(ante,conse,conf)=>
      (ante.mkString(","),
        conse.mkString(","),
        conf,
        op_time)
    }.toDF(),writePath("Rules"))

    val recoDF = this.recommendFromRules(
      rulesRDD,
      historyDF.drop(date_string_field),
      frequentsRDD.map(_._1))
    //TODO delete the part which is only for test
    val num_his_users= historyDF.select(log_user_id_field).distinct().count
    historyDF.unpersist()


    recoDF.persist()
    val num_reco_users = recoDF.select(log_user_id_field).distinct.count()
    val num_reco_distinct_items = recoDF.select(item_id_field).distinct().count()
    val num_reco_items = recoDF.count()

    val coverage = num_reco_distinct_items.toDouble/this.num_all_items

    recoDF.unpersist()

    val recoMeasurements = Array(
      ("OP_TIME",op_time),
      ("ARGUMENTS",linkArgs(minSupport,minConfidence,maxItems,topN,numPartitions)),
      ("NUM_RAW_LOGS" ,num_rawlogs),
      ("NUM_PROCESSED_LOG", numHistory),
      ("NUM_ACTION_TRANSACTIONS", numTranRDD),
      ("NUM_ALL_USERS", num_his_users),
      ("NUM_POSSIBLE_USERS", num_reco_users),
      ("NUM_POSSIBLE_ITEMS",num_reco_distinct_items),
      ("COVERAGE",toMinusDigits(coverage))
    ).map(measure=>(measure._1,measure._2.toString))

    writeCSV(spark.sparkContext.parallelize(
      recoMeasurements).toDF.coalesce(1), writePath("Recommendation-metrics"))

  }





  /**reset parameters**/
  def setMinSupport(minsupport:Double): this.type = {
    this.minSupport = minsupport
    this
  }

  def setMinConfidence(minconfidence:Double): this.type = {
    this.minConfidence = minconfidence
    this
  }

  def setTopN(topn:Integer): this.type = {
    this.topN = topn
    this
  }

  def setNumPartitions(numpartitions:Integer): this.type = {
    this.numPartitions = numpartitions
    this
  }


}