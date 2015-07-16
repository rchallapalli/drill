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

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.lucene;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.dfs.NamedFormatPluginConfig;

import java.util.Iterator;
import java.util.List;

@JsonTypeName("lucene-sub-scan")
public class LuceneSubScan extends AbstractBase implements SubScan {

  private final List<SchemaPath> columns;
  private final String searchQueryString;
  private List<LuceneSubscanSpec> luceneSubscanSpecs;
  private LuceneFormatPlugin formatPlugin;

  @JsonCreator
  public LuceneSubScan(
      @JsonProperty("columns") List<SchemaPath> columns,
      @JsonProperty("search-query") String searchQueryString,
      @JsonProperty("lucene-subscan-specs") List<LuceneSubscanSpec> luceneSubscanSpecs,
      @JsonProperty("storage") StoragePluginConfig storageConfig,
      @JsonProperty("format") FormatPluginConfig formatConfig,
      @JacksonInject StoragePluginRegistry engineRegistry
      ) throws ExecutionSetupException {
    this.formatPlugin = (LuceneFormatPlugin) engineRegistry.getFormatPlugin(storageConfig, formatConfig);
    this.columns = columns;
    this.searchQueryString = searchQueryString;
    this.luceneSubscanSpecs = luceneSubscanSpecs;
  }

  public LuceneSubScan(List<LuceneSubscanSpec> specs, List<SchemaPath> columns, LuceneFormatPlugin formatPlugin, String searchQueryString) {
    this.luceneSubscanSpecs = specs;
    this.columns = columns;
    this.formatPlugin = formatPlugin;
    this.searchQueryString = searchQueryString;

  }

  @JsonProperty("search-query")
  public String getSearchQueryString() {
    return searchQueryString;
  }


  @JsonIgnore
  public LuceneFormatPlugin getFormatPlugin() {
    return formatPlugin;
  }

  @JsonProperty("storage")
  public StoragePluginConfig getStorageConfig(){
    return formatPlugin.getStorageConfig();
  }

  @JsonProperty("format")
  public FormatPluginConfig getFormatConfig(){
    if (formatPlugin.getName() != null) {
      NamedFormatPluginConfig namedConfig = new NamedFormatPluginConfig();
      namedConfig.name = formatPlugin.getName();
      return namedConfig;
    } else {
      return formatPlugin.getConfig();
    }
  }

  @JsonProperty("columns")
  public List<SchemaPath> getColumns() {
    return columns;
  }

  @JsonProperty("lucene-subscan-specs")
  public List<LuceneSubscanSpec> getLuceneSubscanSpecs() {
    return luceneSubscanSpecs;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitSubScan(this, value);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
    Preconditions.checkArgument(children.isEmpty());
    return new LuceneSubScan(luceneSubscanSpecs, columns, formatPlugin, searchQueryString);
  }

  @Override
  public int getOperatorType() {
    // TODO what does this do
    return 1001;
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return Iterators.emptyIterator();
  }
}
