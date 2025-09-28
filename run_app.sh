#!/bin/bash

# Check if SPARK_HOME is set, otherwise set a default path
if [ -z "${SPARK_HOME}" ]
then
    SPARK_HOME=/opt/spark-3.5.3
fi

LOG_FILE_CURRENT=$(pwd)/data-transformer-output.log
LOG_FILE_HISTORY=$(pwd)/data-transformer-history.log
LOG_IDENTIFIER="DataTransformer: "

# Use build.sbt to extract required information
MAIN_CLASS=$(cat build.sbt | grep "MainClass: String" | cut --delimiter '"' --fields 2)
SCALA_VERSION=$(cat build.sbt | grep "scalaVersion :=" | cut --delimiter '"' --fields 2 | cut --delimiter '.' --fields 1,2)
APP_NAME=$(cat build.sbt | grep "name :=" | cut --delimiter '"' --fields 2)
APP_VERSION=$(cat build.sbt | grep "version :=" | cut --delimiter '"' --fields 2)
COMPILE_TARGET=./target/scala-${SCALA_VERSION}/${APP_NAME}-${APP_VERSION}.jar

# If the jar file does not exist, compile the project
if [ ! -f ${COMPILE_TARGET} ]
then
    bash compile_app.sh
fi

# Run the Spark job
${SPARK_HOME}/bin/spark-submit \
    --driver-memory 4g \
    --executor-memory 4g \
    --class ${MAIN_CLASS} \
    --master local \
    ${COMPILE_TARGET} $1 $2 $3 $4 > ${LOG_FILE_CURRENT} 2>&1
RETURN_CODE=$?

# Append current log to history and print relevant lines to console
cat ${LOG_FILE_CURRENT} >> ${LOG_FILE_HISTORY}
grep "${LOG_IDENTIFIER}" "${LOG_FILE_CURRENT}" | while IFS= read -r line
do
    echo "${line#${LOG_IDENTIFIER}}"
done

exit ${RETURN_CODE}
