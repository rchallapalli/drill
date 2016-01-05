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


import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.FieldValueQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;

import java.io.File;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class LuceneScanSpec {
  private FileSelection selection;
  private String selectionRoot;
  private Query searchQuery;

  public LuceneScanSpec(@JsonProperty("files") FileSelection selection,
                        @JsonProperty("selectionRoot") String selectionRoot,
                        @JsonProperty("serializedSearchQuery") byte[] serializedSearchQuery) {
    this.selection = selection;
    this.selectionRoot = selectionRoot;
    QueryParser qp = new QueryParser("dummy", new StandardAnalyzer());
    try {
      searchQuery = qp.parse(new String(serializedSearchQuery, StandardCharsets.UTF_8));
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
  }

  public LuceneScanSpec(String selectionRoot, FileSelection selection, Query searchQuery) {
    this.selection = selection;
    // TODO : This is a temp hack
    if (selectionRoot.startsWith("file:")) {
      this.selectionRoot = selectionRoot.substring(5);
    } else {
      this.selectionRoot = selectionRoot;
    }
    this.searchQuery = searchQuery;
  }

  @JsonIgnore
  public Query getSearchQuery() {
    if (this.searchQuery == null) {
      this.searchQuery = new MatchAllDocsQuery();
    }
    return this.searchQuery;
  }

  public byte[] getSerializedSerachQuery() {
    return getSearchQuery().toString().getBytes();

  }

  public FileSelection getSelection() {
    return this.selection;
  }

  public String getSelectionRoot() {
    return this.selectionRoot;
  }

  public String toString() {
    return "LuceneScanSpec [files=" + selection.toString()
        + ", search-query=" + (searchQuery == null ? null : searchQuery.toString())
        + "]";
  }

}
