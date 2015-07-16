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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.schedule.AffinityCreator;
import org.apache.drill.exec.store.schedule.BlockMapBuilder;
import org.apache.drill.exec.store.schedule.CompleteFileWork;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonTypeName("lucene-scan")
public class LuceneGroupScan extends AbstractGroupScan {
  @JsonIgnore
  private final LuceneFormatPlugin formatPlugin;
  private List<SchemaPath> columns;
  private final SegmentInfos segmentInfos;
  private final String segmentsFilename;
  private final List<SegmentReader> segmentReaders;
  private final Map<Integer, List<SegmentReader>> mappings;
  private final int maxWidth;
  private List<CompleteFileWork> chunks;
  private List<EndpointAffinity> endpointAffinities;
  private final LuceneScanSpec luceneScanSpec;


  public LuceneGroupScan(
      @JsonProperty("userName") String userName,
      @JsonProperty("storage") StoragePluginConfig storageConfig,
      @JsonProperty("format") FormatPluginConfig formatConfig,
      @JsonProperty("lucene-scan-spec") LuceneScanSpec luceneScanSpec,
      @JsonProperty("columns") List<SchemaPath> columns,
      @JacksonInject StoragePluginRegistry engineRegistry)
      throws IOException, ExecutionSetupException {
    this(ImpersonationUtil.resolveUserName(userName), luceneScanSpec, (LuceneFormatPlugin)engineRegistry.getFormatPlugin(storageConfig, formatConfig), columns);
  }

  public LuceneGroupScan(String userName,  LuceneScanSpec luceneScanSpec, LuceneFormatPlugin formatPlugin, List<SchemaPath> columns) throws IOException {
    super(userName);
    final DrillFileSystem dfs = ImpersonationUtil.createFileSystem(getUserName(), formatPlugin.getFsConf());
    this.luceneScanSpec = luceneScanSpec;
    Path selectionRootPath = new Path(luceneScanSpec.getSelectionRoot());

    if (dfs.isDirectory(selectionRootPath)) {
      segmentInfos = SegmentInfos.readLatestCommit(FSDirectory.open(Paths.get(luceneScanSpec.getSelectionRoot())));
    } else {
      segmentInfos = SegmentInfos.readLatestCommit(FSDirectory.open(Paths.get(selectionRootPath.getParent().toString())));
    }

    segmentsFilename = segmentInfos.getSegmentsFileName();
    segmentReaders = new ArrayList<SegmentReader>();
    for (SegmentCommitInfo sci : segmentInfos.asList()) {
      segmentReaders.add(new SegmentReader(sci, IOContext.READ));
    }
    maxWidth = segmentInfos.size();
    this.formatPlugin = formatPlugin;
    // if columns are not passed use '*' (ALL_COLUMNS)
    this.columns = columns == null || columns.size() == 0 ? ALL_COLUMNS : columns;
    this.mappings = new HashMap<Integer, List<SegmentReader>>();
    initFromSelection();
  }

  public LuceneGroupScan(LuceneGroupScan that) {
    super(that.getUserName());
    formatPlugin = that.formatPlugin;
    columns = that.columns;
    maxWidth = that.maxWidth;
    segmentsFilename = that.segmentsFilename;
    segmentInfos = that.segmentInfos;
    segmentReaders = that.segmentReaders;
    mappings = that.mappings;
    chunks = that.chunks;
    endpointAffinities = that.endpointAffinities;
    luceneScanSpec = that.luceneScanSpec;
  }

  private void initFromSelection() throws IOException {
    final DrillFileSystem dfs = ImpersonationUtil.createFileSystem(getUserName(), formatPlugin.getFsConf());
    BlockMapBuilder b = new BlockMapBuilder(dfs, formatPlugin.getContext().getBits());
    // TODO figure out how to split a segment at block boundaries if it is even possible
    this.chunks = b.generateFileWork(luceneScanSpec.getSelection().getStatuses(dfs), false);
    this.endpointAffinities = AffinityCreator.getAffinityMap(chunks);
  }

  @JsonProperty("storage")
  public StoragePluginConfig getStorageConfig() {
    return formatPlugin.getStorageConfig();
  }

  @JsonProperty("format")
  public FormatPluginConfig getFormatConfig() {
    return formatPlugin.getConfig();
  }

  @JsonProperty("files")
  public List<String> getFiles() {
    return luceneScanSpec.getSelection().getFiles();
  }

  @JsonProperty("columns")
  public List<SchemaPath> getColumns() {
    return columns;
  }

  @JsonProperty("segments")
  public List<String> getSegments() {
    List<String> segmentNames = new ArrayList<String>();
    for (SegmentReader segmentReader : segmentReaders) {
      segmentNames.add(segmentReader.getSegmentName());
    }
    return segmentNames;
  }

  /*
   * Endpoint Affinities are constucted assuming that the lucene segments are not block splittable
   */
  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
    if (endpointAffinities == null) {
      logger.debug("chunks: {}", chunks.size());
      endpointAffinities = AffinityCreator.getAffinityMap(chunks);
    }
    return endpointAffinities;
  }


  @Override
  public void applyAssignments(List<CoordinationProtos.DrillbitEndpoint> endpoints) throws PhysicalOperatorSetupException {
    for (int i = 0; i < endpoints.size(); i++) {
      mappings.put(i, new ArrayList<SegmentReader>());
    }

    final int count = endpoints.size();
    for (int i = 0; i < segmentReaders.size(); i++) {
      mappings.get(i % count).add(segmentReaders.get(i));
    }
    return;
  }

  @Override
  public SubScan getSpecificScan(int minorFragmentId) throws ExecutionSetupException {
    assert minorFragmentId < mappings.size() : String.format(
        "Mappings length [%d] should be longer than minor fragment id [%d] but it isn't.", mappings.size(),
        minorFragmentId);
    List<SegmentReader> segmentReadersForMinor = mappings.get(minorFragmentId);
    List<LuceneSubscanSpec> specs = new ArrayList<LuceneSubscanSpec>();
    for (SegmentReader segmentReader : segmentReadersForMinor) {
      specs.add(new LuceneSubscanSpec(segmentReader, luceneScanSpec.getSelectionRoot().toString()));
    }
    return new LuceneSubScan(specs, columns, this.formatPlugin, luceneScanSpec.getSearchQuery().toString());
  }

  @Override
  public int getMaxParallelizationWidth() {
    return maxWidth;
  }

  @Override
  public String getDigest() {
    return this.toString();
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
    Preconditions.checkArgument(children.isEmpty());
    return new LuceneGroupScan(this);
  }

  @Override
  public boolean canPushdownProjects(final List<SchemaPath> columns) {
    return true;
  }

  @Override
  public GroupScan clone(List<SchemaPath> columns) {

    if (!formatPlugin.supportsPushDown()) {
      throw new IllegalStateException(String.format("%s doesn't support pushdown.", this.getClass().getSimpleName()));
    }
    LuceneGroupScan newScan = new LuceneGroupScan(this);
    newScan.columns = columns;
    return newScan;
  }

  /*
   * TODO : This needs to be properly implemented
   */
  @Override
  public ScanStats getScanStats(final PlannerSettings settings) {
    long data = 0;
    long estRowCount = 0;
    try {
      for (SegmentReader segmentReader : segmentReaders) {
        data += segmentReader.getSegmentInfo().sizeInBytes();
        estRowCount += segmentReader.numDocs();
      }

      return new ScanStats(ScanStats.GroupScanProperty.NO_EXACT_ROW_COUNT, estRowCount, 1, data);

    } catch (IOException e) {
      throw new RuntimeException(e);
    }

  }

  public LuceneScanSpec getLuceneScanSpec() {
    return this.luceneScanSpec;
  }

  public List<String> getIndexFields() {
    List<String> indexFields = new ArrayList<String>();
    for (FieldInfo fieldInfo : this.segmentReaders.get(0).getFieldInfos()) {
      if (fieldInfo.getIndexOptions() != IndexOptions.NONE) {
        indexFields.add(fieldInfo.name);
      }
    }
    return indexFields;
  }

  public boolean supportsFilterPushDown() {
    return true;
  }

  public LuceneFormatPlugin getFormatPlugin() {
    return formatPlugin;
  }

}
