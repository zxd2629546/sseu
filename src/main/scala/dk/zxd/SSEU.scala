package dk.zxd

import dk.zxd.control.DAGController
import org.apache.log4j.Logger

/**
 * Created by zxd on 2017/4/27.
 */
object SSEU {
    private val logger = Logger.getLogger(SSEU.getClass)

    def main(args: Array[String]): Unit = {
        if (args.length != 6) {
            println("USSAGE: params should be \"table table_file_path task task_file_path conf conf_file_path\"")
            System.exit(-1)
        }
        val params = 0.to(args.length, 2).map(i => {
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
