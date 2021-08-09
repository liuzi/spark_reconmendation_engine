package tianyu.algorithm

import org.apache.spark.sql.SparkSession
import scopt.OptionParser
import tianyu.algorithm.recommendation.Cluster
import tianyu.algorithm.util.hdfs

/**
  * Created by lynnjiang on 2017/4/25.
  */
object ClusterDcosTest {

  case class Params(outDir: String = "",
                    numCluster: Int = 4,
                    maxIterations: Int = 10,
                    topN: Int = 10) extends AbstractParams[Params]


  //val score_tune:Int = 3

  def main(args: Array[String]) {

    val defaultParams = Params()

    val parser = new OptionParser[Params]("ClusterDcosTest") {
      head("ClusterDcosTest: an example app.")
      opt[String]("outDir").required()
        .text("root directory of analysis result")
        .action((x, c) => c.copy(outDir = x))
//      opt[String]("endTime").required()
//        .text("specified current time/ end time")
//        .action((x,c) => c.copy(endTime = x))
      opt[Int]("numCluster").required()
        .text("k of clusters")
        .action((x, c)=>c.copy(numCluster = x))
      opt[Int]("maxIterations").required()
        .text(s"maximum of Iterations")
        .action((x, c) => c.copy(maxIterations = x))
      opt[Int]("topN").required()
        .text(s"recommend topM items")
        .action((x, c) => c.copy(topN = x))

    }

    parser.parse(args, defaultParams) match {
      case Some(params) => run(params)
      case _ => sys.exit(1)
    }
  }

  def run(params: Params): Unit = {

//    val userfile: String = "t_user_daily_full"
//    val itemfile: String = "t_product_daily_full"

    val spark = SparkSession
      .builder
      .appName("ClusterDcosTest")
      .getOrCreate()

    val cluster = new Cluster(spark,
      outDir = params.outDir,
      k=params.numCluster,
      maxIterations = params.maxIterations
    )

    cluster.getRecoFrmoMultiPath(
      hdfs.subpath,
      hdfs.scorepath,
      (hdfs.downpath,0.3f),
      (hdfs.collectpath,0.6f))
  }

}
