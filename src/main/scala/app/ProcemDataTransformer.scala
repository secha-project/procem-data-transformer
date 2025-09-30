package app

import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.storage.StorageLevel


object ProcemDataTransformer extends App {
    val logPrefix: String = "DataTransformer: "

    if (args.length == 1 && args(0) == "--help") {
        printHelp()
        System.exit(0)
    }

    if (args.length != 4) {
        printHelp()
        System.exit(1)
    }

    // No argument validation is done here!
    val dateString: String = args(0)
    val inputPathBase: String = args(1)
    val outputPathBase: String = args(2)
    val metadataPathBase: String = args(3)


    val spark : SparkSession= SparkSession
        .builder()
        .appName("procem-power-quality-data-transformer")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .master("local")
        .getOrCreate()

    spark.conf.set("spark.sql.debug.maxToStringFields", 1000)
    spark.conf.set("spark.sql.shuffle.partitions", 16)
    spark.sparkContext.setLogLevel(org.apache.log4j.Level.WARN.toString())

    val schemaName = "marjamaki"
    spark.sql(s"CREATE SCHEMA IF NOT EXISTS ${schemaName}")


    def printHelp(): Unit = {
        println(s"${logPrefix}Usage: DataTransformer <date-string> <input-path> <output-path> <metadata-path>")
        println(s"${logPrefix}  <date-string>   : Date string in format YYYY-MM-DD")
        println(s"${logPrefix}  <input-path>    : Path to input CSV files")
        println(s"${logPrefix}  <output-path>   : Path to output Delta Lake tables")
        println(s"${logPrefix}  <metadata-path> : Path to metadata CSV file")
    }

    def getInputPath(basePath: String, dateString: String): String = {
        s"${basePath}/${dateString}_part_*.csv"
    }

    def getMetadataPath(basePath: String): String = {
        s"${basePath}/*.csv"
    }

    def getOutputPath(basePath: String, deviceName: String): String = {
        s"${basePath}/${deviceName}"
    }


    def storeDeviceData(deviceName: String, df: DataFrame, targetPath: String): Unit = {
        val deviceData = df
            .filter(col("device_name") === deviceName)
            .orderBy("timestamp")
        val firstRow: Option[Row] = deviceData.head(1).headOption

        if (firstRow.isEmpty) {
            println(s"${logPrefix}- 0 data rows stored for device ${deviceName}")
            return
        }

        val targetFolder: String = getOutputPath(targetPath, deviceName)

        val tableName = s"${schemaName}.${deviceName.toLowerCase()}"
        val tableExists: Boolean = spark.catalog.tableExists(tableName)
        val oldDataExists: Boolean = try {
            DeltaTable.isDeltaTable(spark, targetFolder)
        } catch {
            case _: Throwable => false
        }

        // If the table does not exist, create it with the new data
        if (!tableExists || !oldDataExists) {
            println(s"${logPrefix}- Creating table ${tableName} (old table: ${tableExists}, old data: ${oldDataExists})")
            deviceData
                .write
                .format("delta")
                .mode("overwrite")
                .option("path", targetFolder)
                .saveAsTable(tableName)
        }

        // Load the data from the target folder as a Delta table
        val deltaTable = DeltaTable.forPath(spark, targetFolder)

        // Update old data with new data, avoiding duplicates
        if (tableExists && oldDataExists) {
            deltaTable
                .as("orig")
                .merge(
                    deviceData.alias("new"),
                    col("orig.device_name") === col("new.device_name") &&
                    col("orig.timestamp") === col("new.timestamp")
                )
                .whenNotMatched()
                    .insertAll()
                .execute()
        }

        // Optimize the table
        // deltaTable
        //     .optimize()
        //     .executeCompaction()


        println(s"${logPrefix}- ${deviceData.count()} data rows stored for device ${deviceName}")
        // println(s"${logPrefix}  - ${deltaTable.toDF.count()} data rows in total for device ${deviceName}")
    }


    val dataPath: String = getInputPath(inputPathBase, dateString)
    val metadataPath: String = getMetadataPath(metadataPathBase)

    // Load the data
    val metadataDF: DataFrame = Fetchers.getMetadata(spark, metadataPath)
    val devices: Seq[String] = Fetchers.getDeviceList(spark, metadataDF)
    val dataDF: DataFrame = Fetchers.getData(spark, dataPath, metadataDF)
        .persist(StorageLevel.MEMORY_ONLY)

    // Store data for each device
    println(s"${logPrefix}Storing data from ${devices.length} devices for date ${dateString}")
    devices
        .foreach(deviceId => storeDeviceData(deviceId, dataDF, outputPathBase))


    spark.stop()
}
