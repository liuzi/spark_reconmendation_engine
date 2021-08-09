package tianyu.algorithm.util

import org.apache.spark.sql.DataFrame

/**
  * Created by lynnjiang on 2017/4/19.
  */
//TODO:Delete this object
object WriteFile {



  final def writeTXT(file:DataFrame,path:String): Unit ={

    file.write
      .mode("overwrite")
      .text(path)//hdfs.getPath(writepath,filename)
  }

  final def writeCSV(file:DataFrame,path:String): Unit ={


    file.write
      .format("com.databricks.spark.csv")
      .mode("overwrite")
      .save(path)//hdfs.getPath(writepath,filename)
  }

  final def writeAVRO(file:DataFrame,path:String): Unit ={
    //TODO:remember to delete path transformation
    //val path = "/"+hdfs.getPath(writepath,filename)

    file.write
      .format("com.databricks.spark.avro")
      .mode("overwrite")
      .save(path)//hdfs.getPath(writepath,filename)
  }

  final def writeLibsvm(file:DataFrame,path:String): Unit ={
    //TODO:remember to delete path transformation
    //val path = "/"+hdfs.getPath(writepath,filename)

    file.write
      .format("libsvm")
      .mode("overwrite")
      .save(path)//hdfs.getPath(writepath,filename)
  }

}
