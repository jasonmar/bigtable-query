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

import scala.io.Source


/** Reads from BigTable */
object BigTableQuery {
  val Limit = 10000
  val Usage = "Usage: java -cp <jar> <project> <instance> <table> <family> <column> <queryFile>"

  def timed[T](f: () => T): (T, Long) = {
    val t0 = System.nanoTime()
    val r = f()
    val t1 = System.nanoTime()
    val dt = (t1-t0)/1000000
    (r, dt)
  }

  def main(args: Array[String]){
    if (args.length != 6) {
      System.err.println(Usage)
      System.exit(1)
    }

    val project = args(0)
    val instanceId = args(1)
    val table = args(2)
    val family = args(3)
    val column = args(4)
    val queryFile = args(5)

    // Read queries from text file
    // Format: tab separated startKey, endKey, desc
    // Example: "f#123##20180101\tf#123##20180201\ttest1"
    val queries = Source.fromFile(queryFile).getLines().map(_.split("\t"))

    val bigTable = new BigTableClient.Reader(project, instanceId, table, family, column, Limit)

    // Client needs to warm up to obtain consistent latency readings
    def scan(): Unit = {
      var i = 0
      val (n, dt) = timed {() =>
        val rs = bigTable.read(" ", "~")
        val n = rs.foldLeft(0){(n, _) => n + 1}
        n
      }
      System.out.println(s"warmup scan: Read $n rows in $dt ms")
      i += 1
    }
    System.out.println("Warming up BigTable client")
    for (_ <- 1 to 20) scan()

    var i = 0
    queries.foreach{a =>
      val startKey = a(0)
      val endKey = a(1)
      val testName = a.lift(2).getOrElse(s"test$i")

      val (rs, dt) = timed {() =>
        bigTable.read(startKey, endKey).toSeq
      }

      System.out.println(s"$testName: Read ${rs.length} rows in $dt ms from [$startKey, $endKey)")

      rs.foreach{r =>
        val c = r.getFamilies(0).getColumns(0)

        val rowKey = r.getKey.toStringUtf8
        val column = c.getQualifier.toStringUtf8
        val value = c.getCells(0).getValue.toStringUtf8

        System.err.println(s"$rowKey\t$column\t$value")
      }
      i+=1
    }
  }
}
