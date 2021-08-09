package tianyu.algorithm

import org.apache.spark.sql.SparkSession
import tianyu.algorithm.recommendation.CosineSimularity
import tianyu.algorithm.util.hdfs

/**
  * Created by lynnjiang on 2017/5/11.
  */
object CSLocalTest extends App{

  val spark = SparkSession.builder().appName("CosineSimularity").master("local[*]")
    .config("spark.some.config.option", "some-value")
    .enableHiveSupport()
    .getOrCreate()


  val outDir = "/Users/lynnjiang/Desktop"
  val cs = new CosineSimularity(
    spark,
    outDir=outDir,
    minSim=0.7,
    topSim=20,
    minCommons = 5,
    minRating= 2)

//  val filename = "t_score/*/*/part*"
  //mf.ValidFromPath(filename)

  cs.getRecoFromPath(hdfs.scorepath,hdfs.subpath)

}
