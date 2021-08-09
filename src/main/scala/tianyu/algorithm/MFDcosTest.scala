package tianyu.algorithm

import org.apache.spark.sql.SparkSession
import scopt.OptionParser
import tianyu.algorithm.recommendation.MatrixFactorization
import tianyu.algorithm.util.hdfs

/**
  * Created by lynnjiang on 2017/5/3.
  */
object MFDcosTest {


  case class Params(outDir: String = null,
                    rank:Int = 10,
                    reg:Double = 1d,
                    maxIter:Int = 10,
                    topN:Int = 10,
                    numBlocks:Int = 10) extends AbstractParams[Params]


  def main(args: Array[String]) {

    val defaultParams = Params()

    val parser = new OptionParser[Params]("MFDcosTest") {
      head("MFDcosTest: an example app.")
      opt[String]("outDir").required()
        .text("root directory for saving the analysis results")
        .action((x, c) => c.copy(outDir = x))
      opt[Int]("rank").required()
        .text("number of latent factors in the model")
        .action((x,c)=>c.copy(rank= x))
      opt[Double]("reg").required()
        .text(s"regularization parameter in ALS")
        .action((x, c) => c.copy(reg = x))
      opt[Int]("maxIter").required()
        .text(s"maximum of Iterations")
        .action((x, c) => c.copy(maxIter = x))
      opt[Int]("topN").required()
        .text(s"recommend topN items")
        .action((x, c) => c.copy(topN = x))
      opt[Int]("numBlocks").required()
        .text(s"number of blocks the users and items will be partitioned into in order to parallelize computation")
        .action((x, c) => c.copy(numBlocks = x))
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
      .appName("MFDcosTest")
      .getOrCreate()

    val mfDcos = new MatrixFactorization(spark,
      outDir=params.outDir,
      matrix_rank = params.rank,
      reg = params.reg,
      maxIter = params.maxIter,
      topN = params.topN,
      numBlocks = params.numBlocks
    )

    mfDcos.RecoFromPath(hdfs.scorepath,hdfs.subpath)
    //cluster.getRecoFromPath(params.filename)
  }

}

