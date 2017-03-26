package com.raphnguyen.hadoop

import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.spark.sql.hive.HiveContext

trait Hbase {
    def deleteTable(tableName: String, hiveContext: HiveContext, admin: HBaseAdmin): Unit
    def createTable(tableName: String, hiveContext: HiveContext): Unit
    def insertRecords(hbaseTable: String, hiveTable: String, uniqueKeys: String, partitions: Int, hiveContext: HiveContext): Unit
}