package app

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, concat, first, get, lit, lower, split, timestamp_millis, when}
import org.apache.spark.sql.types.DoubleType


object Fetchers {
    def getMetadata(spark: SparkSession, inputPath: String): DataFrame = {
        spark
            .read
            .schema(Schemas.metadataSchema)
            .option("header", true)
            .option("delimiter", ";")
            .csv(inputPath)
            .filter(col("active") === "x")
            .select(
                col("rtl_id").alias("measurement_id"),
                get(split(col("name"), "_"), lit(1)).alias("device_name"),
                lower(
                    concat(
                        col("laatuvahti_name"),
                        when(
                            col("unit").isNull || col("unit") === "" || col("unit") === "NaN" ||
                            col("unit") === "%" || col("unit") === "Hz",
                            lit("")
                        )
                            .otherwise(col("unit")),
                    )
                )
                .alias("measurement_name"),
            )
    }

    def getDeviceList(spark: SparkSession, metadataDF: DataFrame): Seq[String] = {
        import spark.implicits.newStringEncoder

        metadataDF
            .select("device_name")
            .distinct()
            .as[String]
            .collect()
    }

    def getData(spark: SparkSession, inputPath: String, metadataDF: DataFrame): DataFrame = {
        import spark.implicits.newStringEncoder

        val firstDeviceName: String = metadataDF
            .select(col("device_name"))
            .as[String]
            .first()

        val measurementNames: Seq[String] = metadataDF
            .filter(col("device_name") === firstDeviceName)
            .select("measurement_name")
            .as[String]
            .collect()

        spark
            .read
            .schema(Schemas.dataSchema)
            .option("header", false)
            .option("delimiter", "\t")
            .csv(inputPath)
            .join(metadataDF, Seq("measurement_id"))
            .select(
                col("device_name"),
                col("measurement_name"),
                timestamp_millis(col("timestamp")).alias("timestamp"),
                col("value").cast(DoubleType).alias("value"),  // all Marjamäki values are doubles
            )
            .groupBy("device_name", "timestamp")
            .pivot("measurement_name", measurementNames)
            .agg(
                first(col("value"), ignoreNulls = true).alias("value")
            )
    }
}
