package app

import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}


object Schemas {
    val metadataSchema: StructType = new StructType(
        Array(
            StructField("rtl_id", LongType, false),
            StructField("serial_number", StringType, false),
            StructField("laatuvahti_name", StringType, false),
            StructField("name", StringType, false),
            StructField("path", StringType, true),
            StructField("unit", StringType, true),
            StructField("iot_ticket", StringType, true),
            StructField("active", StringType, true),
        )
    )

    val dataSchema: StructType = new StructType(
        Array(
            StructField("measurement_id", LongType, false),
            StructField("value", StringType, false),
            StructField("timestamp", LongType, false),
        )
    )
}
