package dk.zxd.util

import dk.zxd.model.{Job, Table, Task}
import org.apache.log4j.Logger

import scala.collection.mutable

/**
 * Created by zxd on 2017/4/24.
 */
case class TopItem(inCnt: Int, job: Job)
class DAGHelper(tables: Seq[Table], tasks: Seq[Task]) {
    private val logger = Logger.getLogger(DAGHelper.this.getClass)
    private var jobMap: Map[Job, Int] = null
    private var idMap: Map[Int, Job] = null
    private var refCnt: Array[Int] = null
    var graph: Array[Array[Boolean]] = null
    var outCnt: Array[Int] = null
    var topSorted: Array[Job] = null

    def init(): Unit = {
        val base = tables.size
        val tableMap = tables.zipWithIndex.toMap
        val taskMap = tasks.zipWithIndex.map(x => (x._1, base + x._2)).toMap
        jobMap = tableMap ++ taskMap
        if (jobMap.size < tables.size + tasks.size) {
            logger.error("every table or task need an unique name")
            throw new IllegalArgumentException
        }
        idMap = jobMap.map(item => (item._2, item._1))
        graph = Array.ofDim(jobMap.size, jobMap.size)
        refCnt = new Array[Int](jobMap.size)
        outCnt = new Array[Int](jobMap.size)

        tasks.foreach(task => {
            val y = jobMap(task)
            refCnt(y) += task.dependencyTable.length
            task.dependencyTable.foreach(tableName => {
                val x = jobMap.find(_._1.name().equals(tableName)).get._2
                graph(x)(y) = true
                outCnt(x) += 1
            })
        })
        logger.info("graph construct complete")
    }

    def id(job: Job) = jobMap(job)

    def job(id: Int) = idMap(id)

    def order() = {
        //TO-DO: build graph, top-sort, return job list
        init()
        var cnt = 0
        val queue = mutable.Queue[TopItem]()
        var limit = jobMap.size * jobMap.size
        topSorted = new Array[Job](jobMap.size)
        jobMap.foreach(item => queue.enqueue(TopItem(refCnt(item._2), item._1)))
        while (queue.nonEmpty && limit > 0) {
            limit -= 1
            val item = queue.dequeue()
            val index = jobMap(item.job)
            if (refCnt(index) == 0) {
                topSorted(cnt) = item.job
                cnt += 1
                graph(index).zipWithIndex.foreach(x => if(x._1) refCnt(x._2) -= 1)
            } else {
                queue.enqueue(TopItem(refCnt(index), item.job))
            }
        }
        logger.info(s"after ${jobMap.size * jobMap.size - limit} rounds pop, top sort complete")
        logger.debug(topSorted.map(_.name()).mkString(","))
        if (queue.nonEmpty) {
            logger.error("Error! There are loop refer betwween tasks")
            logger.error("Check the following tasks")
            queue.foreach(item => logger.error(item.job.name()))
            throw new IllegalArgumentException
        }
    }
}
