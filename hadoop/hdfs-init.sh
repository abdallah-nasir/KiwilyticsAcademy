#!/usr/bin/env bash
  set -euo pipefail

  HDFS_USER="dr.who"

  # HDFS destination

  HDFS_DIR="/data"


  # Linux directory INSIDE the container (must be volume-mounted)
  SRC_IN_CONTAINER="/data"

  # Absolute path to hdfs binary inside the container
  HDFS_BIN="/opt/hadoop-3.2.1/bin/hdfs"

  hdfs_as() {
    docker exec -i namenode bash -lc \
      "export HADOOP_USER_NAME='${HDFS_USER}'; ${HDFS_BIN} $*"
  }

  echo "Running as HDFS user: ${HDFS_USER}"

  echo "Checking HDFS availability..."
  hdfs_as "dfs -ls / >/dev/null"

  echo "Recreating ${HDFS_DIR} as ${HDFS_USER}..."
# Ensure HDFS dir exists (owned by dr.who) because of the UI user
  hdfs_as "dfs -rm -r -f '${HDFS_DIR}' || true"
  hdfs_as "dfs -mkdir '${HDFS_DIR}'"

echo "Uploading ${SRC_IN_CONTAINER} -> ${HDFS_DIR} ..."
hdfs_as "dfs -put -f '${SRC_IN_CONTAINER}'/* '${HDFS_DIR}/' || true"

  echo "Done. Listing HDFS:"
  hdfs_as "dfs -ls -R '${HDFS_DIR}' | head -n 60"
