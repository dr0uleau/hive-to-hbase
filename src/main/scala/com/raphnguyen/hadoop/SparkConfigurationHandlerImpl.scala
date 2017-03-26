package com.raphnguyen.hadoop

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

class SparkConfigurationHandlerImpl extends SparkConfigurationHandler {
    val serialVersionUID = 1L
    private val _sparkConf = new SparkConf
    private var _sparkContext = new SparkContext(_sparkConf)
    private var _hiveContext = new HiveContext(_sparkContext)

    def initialize(): Unit = {
        _sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        _sparkConf.set("spark.kryo.registrationRequired", "true")
        sparkContext_=(new SparkContext(_sparkConf))
        hiveContext_=(new HiveContext(_sparkContext))
    }

    def sparkContext = _sparkContext
    def hiveContext = _hiveContext

    def sparkContext_= (newSparkContext: SparkContext): Unit = _sparkContext = newSparkContext
    def hiveContext_= (newHiveContext: HiveContext): Unit = _hiveContext = newHiveContext
}