/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.spark

import java.util.ArrayList
import java.util.List

import scala.collection.JavaConversions._

import org.apache.commons.codec.DecoderException
import org.apache.commons.codec.binary.Hex
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileStatus, FileSystem, Path, PathFilter }
import org.apache.hadoop.hbase.{ KeyValue, Tag }
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.io.hfile.CacheConfig
import org.apache.hadoop.hbase.io.hfile.HFile
import org.apache.hadoop.hbase.io.hfile.HFile.Reader
import org.apache.hadoop.hbase.io.hfile.HFileScanner
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input._
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.google.common.collect.ImmutableList

/*
 * source: https://github.com/apache/crunch/blob/master/crunch-hbase/src/main/java/org/apache/crunch/io/hbase/HFileInputFormat.java
 */
class HFileInputFormat extends FileInputFormat[ImmutableBytesWritable, KeyValue] {
  private val LOG = LoggerFactory.getLogger(classOf[HFileInputFormat])
  lazy val START_ROW_KEY = "hbase.hfile.input.format.start.row"
  lazy val STOP_ROW_KEY = "hbase.hfile.input.format.stop.row"

  /**
   * File filter that removes all "hidden" files. This might be something
   * worth removing from a more general purpose utility; it accounts for the
   * presence of metadata files created in the way we're doing exports.
   */
  private var HIDDEN_FILE_FILTER = new PathFilter() {
    def accept(p: Path): Boolean = {
      val name = p.getName
      !name.startsWith("_") && !name.startsWith(".")
    }
  }

  def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[ImmutableBytesWritable, KeyValue] = {
    new HFileRecordReader()
  }

  override def isSplitable(context: JobContext, filename: Path): Boolean = {
    false
  }

  protected override def listStatus(job: JobContext): List[FileStatus] = {
    var result = new ArrayList[FileStatus]()

    // Explode out directories that match the original FileInputFormat
    // filters since HFiles are written to directories where the
    // directory name is the column name
    for (status <- super.listStatus(job)) {
      if (status.isDirectory()) {
        var fs = status.getPath().getFileSystem(job.getConfiguration())
        for (same <- fs.listStatus(status.getPath(), HIDDEN_FILE_FILTER)) {
          result.add(same)
        }
      } else {
        result.add(status)
      }
    }
    result
  }

  private class HFileRecordReader extends RecordReader[ImmutableBytesWritable, KeyValue] {
    private var in: Reader = _
    private var conf: Configuration = _
    private var scanner: HFileScanner = _

    /**
     * A private cache of the key value so it doesn't need to be loaded
     * twice from the scanner.
     */
    private var value: KeyValue = null
    private var startRow: Array[Byte] = null
    private var stopRow: Array[Byte] = null
    private var reachedStopRow: Boolean = false
    private var count: Long = _
    private var seeked: Boolean = false

    override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
      val fileSplit = split.asInstanceOf[FileSplit]
      conf = context.getConfiguration()
      val path = fileSplit.getPath()
      val fs = path.getFileSystem(conf)
      LOG.info("Initialize HFileRecordReader for {}", path)
      this.in = HFile.createReader(fs, path, new CacheConfig(conf), conf)

      // The file info must be loaded before the scanner can be used.
      // This seems like a bug in HBase, but it's easily worked around.
      this.in.loadFileInfo()
      this.scanner = in.getScanner(false, false)

      val startRowStr = conf.get(START_ROW_KEY)
      if (startRowStr != null) {
        this.startRow = decodeHexOrDie(startRowStr)
      }
      val stopRowStr = conf.get(STOP_ROW_KEY)
      if (stopRowStr != null) {
        this.stopRow = decodeHexOrDie(stopRowStr)
      }
    }

    override def getCurrentKey(): ImmutableBytesWritable = {
      new ImmutableBytesWritable(scanner.getKeyValue().getRow())
    }

    override def getCurrentValue(): KeyValue = {
      value
    }

    override def nextKeyValue(): Boolean = {
      if (reachedStopRow) {
        return false
      }
      var hasNext: Boolean = false
      if (!seeked) {
        if (startRow != null) {
          if (LOG.isInfoEnabled()) {
            LOG.info("Seeking to start row {}", Bytes.toStringBinary(startRow))
          }
          val kv = KeyValue.createFirstOnRow(startRow)
          hasNext = seekAtOrAfter(scanner, kv)
        } else {
          LOG.info("Seeking to start")
          hasNext = scanner.seekTo()
        }
        seeked = true;
      } else {
        hasNext = scanner.next()
      }
      if (!hasNext) {
        return false
      }
      value = KeyValue.cloneAndAddTags(scanner.getKeyValue(), ImmutableList.of[Tag]())
      if (stopRow != null
        && Bytes.compareTo(value.getRowArray(), value.getRowOffset(), value.getRowLength(), stopRow, 0,
          stopRow.length) >= 0) {
        if (LOG.isInfoEnabled()) {
          LOG.info("Reached stop row {}", Bytes.toStringBinary(stopRow))
        }
        reachedStopRow = true
        value = null
        return false
      }
      count = count + 1
      true
    }

    override def getProgress(): Float = {
      1.0f * count / in.getEntries()
    }

    override def close(): Unit = {
      if (in != null) {
        in.close()
        in = null
      }
    }

    private def decodeHexOrDie(s: String): Array[Byte] = {
      try {
        Hex.decodeHex(s.toCharArray())
      } catch {
        case e: DecoderException => throw new AssertionError("Failed to decode hex string: " + s)
      }
    }

    private def seekAtOrAfter(scanner: HFileScanner, kv: KeyValue): Boolean = {
      var result: Int = scanner.seekTo(kv)
      if (result < 0) {
        // Passed KV is smaller than first KV in file, work from start of file
        scanner.seekTo()
      } else if (result > 0) {
        // Passed KV is larger than current KV in file, if there is a next it is the "after", if not then this scanner is done.
        scanner.next()
      }
      // Seeked to the exact key
      true
    }

  }
}