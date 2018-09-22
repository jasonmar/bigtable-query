/*
 *  Copyright 2018 Google Inc. All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.google.cloud.example


import com.google.bigtable.v2._
import com.google.cloud.bigtable.config.BigtableOptions
import com.google.cloud.bigtable.grpc.scanner.ResultScanner
import com.google.cloud.bigtable.grpc.{BigtableDataClient, BigtableSession}
import com.google.protobuf.ByteString

object BigTableClient {

  // Single session per JVM
  @volatile private var sharedSession: BigtableSession = _

  def getSharedClient(projectId: String, instanceId: String): BigtableDataClient = {
    synchronized {
      if (sharedSession == null) {
        val opts = new BigtableOptions.Builder()
          .setProjectId(projectId)
          .setInstanceId(instanceId)
          .setUserAgent("BigTableClient")
          .build()
        sharedSession = new BigtableSession(opts)
      }
    }
    sharedSession.getDataClient
  }

  def addFilters(readRowsRequest: ReadRowsRequest.Builder, family: String): ReadRowsRequest.Builder = {
    val chainFilter: RowFilter.Chain.Builder = readRowsRequest.getFilterBuilder.getChainBuilder
    chainFilter.addFiltersBuilder().setCellsPerRowLimitFilter(1)
    chainFilter.addFiltersBuilder().setFamilyNameRegexFilter(family)
    readRowsRequest
  }

  def addRowRange(readRowsRequest: ReadRowsRequest.Builder, startKey: String, endKey: String): ReadRowsRequest.Builder = {
    readRowsRequest
      .getRowsBuilder
      .addRowRangesBuilder()
      .setStartKeyClosed(ByteString.copyFromUtf8(startKey))
      .setEndKeyOpen(ByteString.copyFromUtf8(endKey))
    readRowsRequest
  }

  class Reader(projectId: String,
               instanceId: String,
               table: String,
               family: String,
               column: String,
               rowLimit: Int) extends Serializable {
    @transient private val client: BigtableDataClient =
      getSharedClient(projectId, instanceId)

    val defaultRequest: ReadRowsRequest = {
      val readRowsRequest = ReadRowsRequest.newBuilder()
        .setTableName(s"projects/$projectId/instances/$instanceId/tables/$table")
        .setRowsLimit(rowLimit)
      addFilters(readRowsRequest, family)
      readRowsRequest.build()
    }

    def read(startKey: String, endKey: String): Iterator[Row] = {
      val readRowsRequest = ReadRowsRequest.newBuilder(defaultRequest)
      addRowRange(readRowsRequest, startKey, endKey)
      val rs = client.readRows(readRowsRequest.build())
      new BigTableRowIterator(rs)
    }

    def sample(): Seq[SampleRowKeysResponse] = {
      import scala.collection.JavaConverters.iterableAsScalaIterableConverter
      val req = SampleRowKeysRequest.newBuilder()
      req.setTableName(s"projects/$projectId/instances/$instanceId/tables/$table")
      val rs = client.sampleRowKeys(req.build())
      rs.asScala.toSeq
    }
  }

  class BigTableRowIterator(rs: ResultScanner[Row]) extends Iterator[Row] {
    private var first: Option[Row] = Option(rs.next())
    override def hasNext: Boolean = first.isDefined
    override def next(): Row = {
      val r = first.get
      first = Option(rs.next())
      r
    }
  }
}
