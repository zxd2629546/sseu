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
            "table" -> "conf/test_table.xml",
            "task" -> "conf/test_task.xml",
            "conf" -> "conf/test_conf.xml",
            "hdfs" -> "hdfs://master:9000"
        )
        SSEU.sseu(params)
    }
}
