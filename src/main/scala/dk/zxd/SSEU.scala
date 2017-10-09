package dk.zxd

import dk.zxd.control.DAGController
import org.apache.log4j.Logger

/**
 * Created by zxd on 2017/4/27.
 */
object SSEU {
    private val logger = Logger.getLogger(SSEU.getClass)

    /**
      * java -jar sseu.jar table conf/test_table.xml task conf/test_task.xml conf conf/test_conf.xml hdfs hdfs://master:9000
      * @param args
      */
    def main(args: Array[String]): Unit = {
        if (args.length < 8) {
            println("USSAGE: sseu.jar table [table_xml_path] task [task_xml_path] conf [conf_xml_path] hdfs [hdfs_name_node_uri]")
            System.exit(-1)
        }
        val params = 0.until(args.length, 2).map(i => {
            (args(i), args(i + 1))
        }).toMap
        sseu(params)
    }

    def sseu(params: Map[String, String]): Unit = {
        val controller = DAGController(params)
        controller.init()
        logger.info("init complete")
        controller.schedule()
        logger.info("schedule complete")
        controller.finish()
        logger.info("mission complete")
    }
}
