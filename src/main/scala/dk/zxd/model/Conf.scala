package dk.zxd.model

/**
 * Created by zxd on 2017/4/24.
 */
class Conf(memory: String, cores: Int, val master: String, val hdfsUri: String) {
    val sparkParams = scala.collection.mutable.Map[String, String]()
    val platformParams = scala.collection.mutable.Map[String, String]()

    override def toString = s"Conf($sparkParams, $platformParams)"
}

object Conf {
    def apply(memory: String, cores: Int, master: String, hdfsUri: String) = {
        val conf = new Conf(memory, cores, master, hdfsUri)
        //set default spark params
        conf.sparkParams ++= Map(
            //"spark.master" -> "spark://localhost:7077",
            "spark.executor.memory" -> memory,
            "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
            "spark.driver.memory" -> "2g",
            "spark.driver.extraJavaOptions" -> "-server -XX:+UserG1GC",
            "spark.executor.extraJavaOptions" -> "-server -XX:+UserG1GC",
            "spark.shuffle.file.buffer" -> "64K",
            "spark.reducer.maxSizeInFlight" -> "96m",
            "spark.executor.cores" -> cores.toString
        )
        conf
    }
}