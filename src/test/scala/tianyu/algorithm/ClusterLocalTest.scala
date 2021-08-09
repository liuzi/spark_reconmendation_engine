package tianyu.algorithm

import org.apache.spark.sql.SparkSession
import tianyu.algorithm.recommendation.Cluster
import tianyu.algorithm.util.hdfs

/**
  * Created by lynnjiang on 2017/4/20.
  */
object ClusterLocalTest extends App{


  val spark = SparkSession.builder().appName("Cluster").master("local[*]")
    .config("spark.some.config.option", "some-value")
    .enableHiveSupport()
    .getOrCreate()

  val cluster = new Cluster(spark, outDir="/Users/lynnjiang/Desktop",k=5)

  cluster.getRecoFrmoMultiPath(
    hdfs.subpath,
    hdfs.scorepath,
    (hdfs.downpath,0.3f),
    (hdfs.collectpath,0.6f))

}
