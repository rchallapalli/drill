/**
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

package org.apache.drill.exec.store.lucene;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.store.BufferedChecksumIndexInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSLockFactory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.Version;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;


public class LuceneSubscanSpec {
  private String segmentName;
  private String userName;
  private Map<String, String> attributes;
  private Map<String, String> diagnostics;
  private byte[] segmentId;
  private String codecName;
  private boolean isCompoundFile;
  private String luceneVersion;
  private String dirPathString;
  private int maxDoc;
  private int delCount;
  private long delGen;
  private long fieldInfosGen;
  private long docValuesGen;
  @JsonIgnore
  private SegmentReader segmentReader;

  public LuceneSubscanSpec(SegmentReader segmentReader, String dirPathString) {
    SegmentCommitInfo sci = segmentReader.getSegmentInfo();

    SegmentInfo si = sci.info;
    this.attributes = si.getAttributes();
    this.segmentId = si.getId();
    this.segmentName = si.name;
    this.diagnostics = si.getDiagnostics();
    Codec codec = si.getCodec();
    this.codecName = codec.getName();
    this.isCompoundFile = si.getUseCompoundFile();
    this.maxDoc = si.maxDoc();
    this.dirPathString = dirPathString;
    this.delCount = sci.getDelCount();
    this.delGen = sci.getDelGen();
    this.fieldInfosGen = sci.getFieldInfosGen();
    this.docValuesGen = sci.getDocValuesGen();
    this.segmentReader = segmentReader;
  }

  @JsonCreator
  public LuceneSubscanSpec(String userName,
                           String dirPathString,
                           byte[] segmentId,
                           String segmentName,
                           String codecName,
                           boolean isCompoundFile,
                           int maxDoc,
                           Map<String, String> attributes,
                           Map<String, String> diagnostics,
                           int delCount,
                           long delGen,
                           long fieldInfosGen,
                           long docValuesGen) {

    this.userName = userName;
    this.dirPathString = dirPathString;
    this.segmentId = segmentId;
    this.segmentName = segmentName;
    this.codecName = codecName;
    this.isCompoundFile = isCompoundFile;
    this.maxDoc = maxDoc;
    this.attributes = attributes;
    this.diagnostics = diagnostics;
    this.delCount = delCount;
    this.delGen = delGen;
    this.fieldInfosGen = fieldInfosGen;
    this.docValuesGen = docValuesGen;
    this.constructSegmentReader();
  }

  private void constructSegmentReader() {
    try {
      Path dirPath = Paths.get(dirPathString);
      Directory dir = new SimpleFSDirectory(dirPath, FSLockFactory.getDefault());
      Codec codec = Codec.forName(codecName);
      Version v = Version.LATEST;

      SegmentInfo segmentInfo = new SegmentInfo(dir, Version.LATEST, segmentName, maxDoc, isCompoundFile, codec, diagnostics, segmentId, attributes);
      SegmentCommitInfo segmentCommitInfo = new SegmentCommitInfo(segmentInfo, delCount, delGen, fieldInfosGen, docValuesGen);
      this.segmentReader = new SegmentReader(segmentCommitInfo, IOContext.READ);

    } catch (IOException e) {
      throw new RuntimeException(e);
      // TODO
    }
  }

  public String getSegmentName() {
    return segmentName;
  }

  public String getUserName() {
    return userName;
  }

  public Map<String, String> getAttributes() {
    return attributes;
  }

  public Map<String, String> getDiagnostics() {
    return diagnostics;
  }

  public byte[] getSegmentId() {
    return segmentId;
  }

  public String getCodecName() {
    return codecName;
  }

  public boolean isCompoundFile() {
    return isCompoundFile;
  }

  public String getLuceneVersion() {
    return luceneVersion;
  }

  public String getDirPathString() {
    return dirPathString;
  }

  public int getMaxDoc() {
    return maxDoc;
  }

  public int getDelCount() {
    return delCount;
  }

  public long getDelGen() {
    return delGen;
  }

  public long getFieldInfosGen() {
    return fieldInfosGen;
  }

  public long getDocValuesGen() {
    return docValuesGen;
  }

  public SegmentReader getSegmentReader() {
    return segmentReader;
  }
}
