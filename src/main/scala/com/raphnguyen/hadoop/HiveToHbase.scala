package com.raphnguyen.hadoop

object HiveToHbase {
    def main(args: Array[String]): Unit = {
        val sparkConfigurationHandler = new SparkConfigurationHandlerImpl
        val hbase = new HbaseImpl
        val hbaseTable = args(0)
        val hiveTable = args(1)
        val uniqueKeys = args(2)
        val partitions = args(3).toInt
        hbase.createTable(hbaseTable, sparkConfigurationHandler.hiveContext)
        hbase.insertRecords(hbaseTable, hiveTable, uniqueKeys, partitions, sparkConfigurationHandler.hiveContext)
    }
}