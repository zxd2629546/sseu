SparkSQL Easy Use
======

This project is designed to benefit people who don't know how to use
Spark by writing three simple xml files.

## Requirements

* spark 2.x
* scala 2.11.x or later
* jdk 1.7 or later

## How to use

To build your task, you should build three xml to describe table, task
and configurations.

### XML for Table

This file can describe source file you are going to load.

    <tables>
        <table>
            <name>action</name>
            <input>file:///home/gpt43t/zxd/spark/jdata/JData_Action_*.csv</input>
            <format>csv</format>
            <schema>file:///home/gpt43t/zxd/spark/jdata/JData_Action_201602_schema</schema>
        </table>
    </tables>
This is the basic format, you should define table name, table source 
files, source file format and the schema (not necessary for json format).
Schema file need format as follow:

|index|field_name|type|
|:---:|----------|----|
|1|test|Integer|

### XML for Task

This file can describe sql task, basic format as follow:

    <task>
        <sqlTask>select user_id, count(distinct sku_id) as sku_cnt, count(*) rec_cnt, count(distinct cate) as cate_cnt, count(distinct brand) as bra_cnt, count(distinct model_id) as mod_cnt from action where type = 1 group by user_id</sqlTask>
        <input>action</input>
        <outputName>view</outputName>
        <outputDir>file:///home/gpt43t/zxd/data/sseu_test/</outputDir>
        <outputFormat>text_tab</outputFormat>
        <saveSchema>true</saveSchema>
        <cacheAble>true</cacheAble>
    </task>
    
### XML for Configuration

This file mostly for setting Spark configurations

    <conf>
        <memory>64G</memory>
        <cores>12</cores>
        <master>local[12]</master>
        <hdfsUri>hdfs://master:9000</hdfsUri>
        <sparkParam>
    
        </sparkParam>
        <!--<platformParam></platformParam>-->
    </conf>

Go to `conf/` for more detail.

## Run

You can simply use `scala -J-Xmx4g -cp "spark/jars/*:sseu.jar" dk.zxd.SSEU table conf/your_table task conf/your_task.xml conf/your_conf.xml`


## Advanced

To deal with daily logs, you can use linux command `sed`, template
xml files and `crontab` to make it.