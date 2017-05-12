package dk.zxd.test

import dk.zxd.SSEU
import org.junit.Test

/**
  * Created by zxd on 5/5/17.
  */
class SSEUTest {

    @Test
    def sseuTest(): Unit = {
        val params = Map(
            "table_xml_path" -> "conf/jdata_table.xml",
            "task_xml_path" -> "conf/jdata_task.xml",
            "conf_xml_path" -> "conf/test_conf.xml",
            "hdfs_name_node_uri" -> "hdfs://master:9000"
        )
        SSEU.sseu(params)
    }
}
