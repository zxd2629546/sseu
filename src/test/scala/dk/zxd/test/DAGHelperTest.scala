package dk.zxd.test

import dk.zxd.model.{Table, Task}
import dk.zxd.util.DAGHelper
import org.junit.{Assert, Before, Test}

/**
  * Created by zxd on 5/5/17.
  */
class DAGHelperTest {

    var helper: DAGHelper = _
    val tables = Seq(
        new Table("ta", null, null, null),
        new Table("tb", null, null, null),
        new Table("tc", null, null, null),
        new Table("td", null, null, null),
        new Table("te", null, null, null)
    )
    val tasks = Seq(
        new Task(null, Array("ta", "tb"), "tf"),
        new Task(null, Array("te"), "tg"),
        new Task(null, Array("tc", "tf"), "th"),
        new Task(null, Array("ta", "tf", "tg", "tj"), "ti"),
        new Task(null, Array("tf", "th"), "tj")
    )

    /**
      * ta
      *     tf
      * tb          tj
      *         th
      * tc              ti
      *
      * td
      *
      * te  tg
      */

    @Before
    def setUp(): Unit = {
        helper = new DAGHelper(tables, tasks)
    }

    @Test
    def testInit(): Unit = {
        helper.init()
        val graph = helper.graph
        val ids = tables.union(tasks).map(job => helper.id(job))
        val edges = Seq(
            (ids(0), ids(5)), (ids(1), ids(5)),
            (ids(4), ids(6)),
            (ids(2), ids(7)), (ids(5), ids(7)),
            (ids(0), ids(8)), (ids(5), ids(8)), (ids(6), ids(8)), (ids(9), ids(8)),
            (ids(5), ids(9)), (ids(7), ids(9))
        )
        val outCnt = Seq(2, 1, 1, 0, 1, 3, 1, 1, 0, 1)
        for (i <- 0 to 9; j <- 0 to 9) {
            val flag = if (edges.contains((i, j))) true else false
            Assert.assertEquals(flag, graph(i)(j))
        }
        ids.zip(outCnt).foreach(id => {
            Assert.assertEquals(id._2, helper.outCnt(id._1))
        })
    }

    @Test
    def testOrder(): Unit = {
        helper.order()
        val order = helper.topSorted.map(_.name()).zipWithIndex
        tasks.foreach(task => {
            val index = order.find(_._1.equals(task.name())).get._2
            task.dependencyTable.foreach(tableName => {
                Assert.assertEquals(true, order.find(_._1.equals(tableName)).get._2 < index)
            })
        })
    }
}
