package tianyu.algorithm

import org.apache.spark.sql.SparkSession
import tianyu.algorithm.recommendation.MatrixFactorization
import tianyu.algorithm.util.hdfs

/**
  * Created by lynnjiang on 2017/5/2.
  */
object exALSLocalTest extends App{


  val spark = SparkSession.builder().appName("exALSLocal").master("local[*]")
    .config("spark.some.config.option", "some-value")
    .enableHiveSupport()
    .getOrCreate()



  val mf = new MatrixFactorization(spark,
    outDir="/Users/lynnjiang/Desktop",
    matrix_rank=5,
    reg = 1.0d,
    maxIter = 5,
    topN = 10,
    numBlocks = 100)


  /**
    * Get Recommendation
    * @param filename: String type. The file path for loading data.

    **/
  //mf.ValidFromPath(filename)
  mf.RecoFromPath(hdfs.scorepath,hdfs.subpath)
  //ar.getRecommendFromPath(filename)
  //ar.getComparison(userid="shixun_594844")
  //ar.getComparisonFromPath(filename = "down_test.csv")

}
