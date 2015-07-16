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

import com.google.common.collect.Lists;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.BaseValueVector;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.flexible.standard.QueryParserUtil;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class LuceneRecordReader extends AbstractRecordReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LuceneRecordReader.class);
  private final SegmentReader segmentReader;
  private final IndexSearcher searcher;
  private final Analyzer analyzer;
  private final List<ValueVector> vectors = Lists.newArrayList();
  private final Query searchQuery;
  private boolean matchAll = false;
  private ScoreDoc currentScoreDoc;

  public LuceneRecordReader(SegmentReader segmentReader, List<SchemaPath> columns, String searchQueryString) {
    this.segmentReader = segmentReader;
    this.setColumns(columns);
    analyzer = new StandardAnalyzer();
    searcher = new IndexSearcher(segmentReader);
    // TODO set matchAll to true when the query does not contain any indexed fields
    if (searchQueryString == null) {
      matchAll = true;
    }
    // Note : Lucene sometimes throws an NPE(depending on the query) when we pass null for the default field
    QueryParser qp = new QueryParser("dummy", analyzer);
    try {
      searchQuery = qp.parse(searchQueryString);
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }


  }

  @Override
  public void setup(OperatorContext context, OutputMutator output) throws ExecutionSetupException {
    try {
      for (SchemaPath column : getColumns()) {
        TypeProtos.MajorType type = TypeProtos.MajorType.newBuilder().setMinorType(TypeProtos.MinorType.VARCHAR)
            .setMode((TypeProtos.DataMode.OPTIONAL)).build();
        MaterializedField field = MaterializedField.create(column, type);
        Class vvClass = TypeHelper.getValueVectorClass(type.getMinorType(), type.getMode());
        vectors.add(output.addField(field, vvClass));
      }

    } catch (SchemaChangeException e) {
      throw new ExecutionSetupException(e);
    }
  }

  @Override
  public int next() {
    try {
      for (ValueVector vv : vectors) {
        AllocationHelper.allocateNew(vv, BaseValueVector.INITIAL_VALUE_ALLOCATION);
      }

      TopDocs results;
      if (matchAll) {
        if (currentScoreDoc == null) {
          results = searcher.search(new MatchAllDocsQuery(), BaseValueVector.INITIAL_VALUE_ALLOCATION);
        } else {
          results = searcher.searchAfter(currentScoreDoc, new MatchAllDocsQuery(), BaseValueVector.INITIAL_VALUE_ALLOCATION);
        }

      } else {
        if (currentScoreDoc == null) {
          results = searcher.search(searchQuery, BaseValueVector.INITIAL_VALUE_ALLOCATION);
        } else {
          results = searcher.searchAfter(currentScoreDoc, searchQuery, BaseValueVector.INITIAL_VALUE_ALLOCATION);
        }
      }
      ScoreDoc[] hits = results.scoreDocs;
      for (int i = 0; i < hits.length; i++) {
        this.currentScoreDoc = hits[i];
        Document doc = searcher.doc(hits[i].doc);
        Iterator<SchemaPath> columnsIterator = getColumns().iterator();
        int j = 0;
        // TODO handle the case when * is used
        while (columnsIterator.hasNext()) {
          SchemaPath column = columnsIterator.next();
          String columnName = column.getRootSegment().getPath();
          final String columnValue = doc.get(columnName);
          if (columnValue != null) {
            final byte[] valueBytes = columnValue.getBytes();

            ((NullableVarCharVector) vectors.get(j)).getMutator().setSafe(i, valueBytes, 0, columnValue.length());
          } else {
            ((NullableVarCharVector) vectors.get(j)).getMutator().setNull(i);
          }
          j++;
        }
      }


      for (ValueVector vv : vectors) {
        vv.getMutator().setValueCount(hits.length);
      }

      return hits.length;
    } catch (Exception e) {
      return -1;
    }
  }

  @Override
  public void close() {
    try {
      if (this.segmentReader != null) {
        this.segmentReader.close();
      }
    } catch (IOException e) {
      logger.warn("Failure while closing the segment reader");
    }
  }
}
