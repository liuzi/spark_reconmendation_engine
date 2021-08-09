package tianyu.algorithm

import org.apache.spark.sql.SparkSession
import scopt.OptionParser
import tianyu.algorithm.recommendation.CosineSimularity
import tianyu.algorithm.util.hdfs

/**
  * Created by lynnjiang on 2017/5/11.
  */
object CSDcosTest {
  case class Params(outDir:String = "",
                    minSim:Double = 0.7,
                    topSim:Int=20,
                    minCommons:Int=5,
                    topN: Int = 10) extends AbstractParams[Params]


  //val score_tune:Int = 3

  def main(args: Array[String]) {

    val defaultParams = Params()

    val parser = new OptionParser[Params]("CSDcosTest") {
      head("CSDcosTest: an example app.")
      opt[String]("outDir").required()
        .text("root directory of analysis result")
        .action((x, c) => c.copy(outDir = x))
      //      opt[String]("endTime").required()
      //        .text("specified current time/ end time")
      //        .action((x,c) => c.copy(endTime = x))
      opt[Double]("minSim").required()
        .text("Minimum of adjusted similarities to filter (i,j) pair similarities")
        .action((x, c)=>c.copy(minSim = x))
      opt[Int]("topSim").required()
        .text("Maximum of similar items for each item (for calculate predictions)")
        .action((x,c)=>c.copy(topSim = x))
      opt[Int]("minCommons").required()
        .text("Minimum number of same (rui)s to determine two items are similar")
        .action((x,c)=>c.copy(minCommons = x))
      opt[Int]("topN").required()
        .text(s"recommend topM items")
        .action((x, c) => c.copy(topN = x))
//      minRating= 2)

    }

    parser.parse(args, defaultParams) match {
      case Some(params) => run(params)
      case _ => sys.exit(1)
    }
  }

  def run(params: Params): Unit = {


    val spark = SparkSession
      .builder
      .appName("CSDcosTest")
      .getOrCreate()

    val cs = new CosineSimularity(
      spark,
      outDir=params.outDir,
      minSim=params.minSim,
      topSim=params.topSim,
      minCommons = params.minCommons,
      topN = params.topN)

//    val filename = "t_score/*/*/part*"
    //mf.ValidFromPath(filename)

    cs.getRecoFromPath(hdfs.scorepath,hdfs.subpath)
  }
}
