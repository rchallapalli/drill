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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSLockFactory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.Version;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

@JsonTypeName("lucene-subscan-spec")
public class LuceneSubscanSpec {

  protected String segmentName;
  protected String userName;
  protected Map<String, String> attributes;
  protected Map<String, String> diagnostics;
  protected byte[] segmentId;
  protected String codecName;
  protected boolean compoundFile;
  protected String dirPathString;
  protected int maxDoc;
  protected int delCount;
  protected long delGen;
  protected long fieldInfosGen;
  protected long docValuesGen;
  @JsonIgnore
  private SegmentReader segmentReader;

  @JsonCreator
  public LuceneSubscanSpec(@JsonProperty("userName") String userName,
                           @JsonProperty("dirPathString") String dirPathString,
                           @JsonProperty("segmentId") byte[] segmentId,
                           @JsonProperty("segmentName") String segmentName,
                           @JsonProperty("codecName") String codecName,
                           @JsonProperty("compoundFile") boolean isCompoundFile,
                           @JsonProperty("maxDoc") int maxDoc,
                           @JsonProperty("attributes") Map<String, String> attributes,
                           @JsonProperty("diagnostics") Map<String, String> diagnostics,
                           @JsonProperty("delCount") int delCount,
                           @JsonProperty("delGen") long delGen,
                           @JsonProperty("fieldInfosGen") long fieldInfosGen,
                           @JsonProperty("docValuesGen") long docValuesGen) {

    this.userName = userName;
    this.dirPathString = dirPathString;
    this.segmentId = segmentId;
    this.segmentName = segmentName;
    this.codecName = codecName;
    this.compoundFile = isCompoundFile;
    this.maxDoc = maxDoc;
    this.attributes = attributes;
    this.diagnostics = diagnostics;
    this.delCount = delCount;
    this.delGen = delGen;
    this.fieldInfosGen = fieldInfosGen;
    this.docValuesGen = docValuesGen;
    this.constructSegmentReader();
  }

  public LuceneSubscanSpec(SegmentReader segmentReader, String dirPathString) {
    SegmentCommitInfo sci = segmentReader.getSegmentInfo();

    SegmentInfo si = sci.info;
    this.attributes = si.getAttributes();
    this.segmentId = si.getId();
    this.segmentName = si.name;
    this.diagnostics = si.getDiagnostics();
    Codec codec = si.getCodec();
    this.codecName = codec.getName();
    this.compoundFile = si.getUseCompoundFile();
    this.maxDoc = si.maxDoc();
    this.dirPathString = dirPathString;
    this.delCount = sci.getDelCount();
    this.delGen = sci.getDelGen();
    this.fieldInfosGen = sci.getFieldInfosGen();
    this.docValuesGen = sci.getDocValuesGen();
    this.segmentReader = segmentReader;
  }



  private void constructSegmentReader() {
    try {
      Path dirPath = Paths.get(dirPathString);
      Directory dir = new SimpleFSDirectory(dirPath, FSLockFactory.getDefault());
      Codec codec = Codec.forName(codecName);
      Version v = Version.LATEST;

      SegmentInfo segmentInfo = new SegmentInfo(dir, Version.LATEST, segmentName, maxDoc, compoundFile, codec, diagnostics, segmentId, attributes);
      SegmentCommitInfo segmentCommitInfo = new SegmentCommitInfo(segmentInfo, delCount, delGen, fieldInfosGen, docValuesGen);
      this.segmentReader = new SegmentReader(segmentCommitInfo, IOContext.READ);

    } catch (IOException e) {
      throw new RuntimeException(e);
      // TODO
    }
  }

  @JsonProperty("segmentName")
  public String getSegmentName() {
    return segmentName;
  }

  @JsonProperty("userName")
  public String getUserName() {
    return userName;
  }

  @JsonProperty("attributes")
  public Map<String, String> getAttributes() {
    return attributes;
  }

  @JsonProperty("diagnostics")
  public Map<String, String> getDiagnostics() {
    return diagnostics;
  }

  @JsonProperty("segmentId")
  public byte[] getSegmentId() {
    return segmentId;
  }

  @JsonProperty("codecName")
  public String getCodecName() {
    return codecName;
  }

  @JsonProperty("compoundFile")
  public boolean isCompoundFile() {
    return compoundFile;
  }

  @JsonProperty("dirPathString")
  public String getDirPathString() {
    return dirPathString;
  }

  @JsonProperty("maxDoc")
  public int getMaxDoc() {
    return maxDoc;
  }

  @JsonProperty("delCount")
  public int getDelCount() {
    return delCount;
  }

  @JsonProperty("delGen")
  public long getDelGen() {
    return delGen;
  }

  @JsonProperty("fieldInfosGen")
  public long getFieldInfosGen() {
    return fieldInfosGen;
  }

  @JsonProperty("docValuesGen")
  public long getDocValuesGen() {
    return docValuesGen;
  }

  @JsonIgnore
  public SegmentReader getSegmentReader() {
    return segmentReader;
  }
}
