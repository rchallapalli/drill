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

package org.apache.drill.exec.planner.logical;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.TermQuery;

import java.util.List;

public class SqlFilterToLuceneQuery extends RexVisitorImpl<Void> {
  private final List<String> indexFields;
  private final DrillRel inputRel;
  private BooleanQuery compositeQuery;
  private Boolean isCurrentFieldIndexed;
  private SqlKind currentSqlKind;
  private Analyzer analyzer;
  private Query currentQuery;
  private boolean composite = false;


  private String currentField;
  private String currentValue;
  private RelDataType currentType;

  public SqlFilterToLuceneQuery(List<String> indexFields, DrillRel inputRel) {
    super(true);
    this.indexFields = indexFields;
    this.inputRel = inputRel;
    compositeQuery = new BooleanQuery();
    isCurrentFieldIndexed = false;
    analyzer = new WhitespaceAnalyzer();
  }

  public Query getLuceneQuery() {
    return compositeQuery;
  }

  @Override
  public Void visitInputRef(RexInputRef rexInputRef) {
    int index = rexInputRef.getIndex();
    currentField = inputRel.getRowType().getFieldList().get(index).getName();
    currentType = inputRel.getRowType().getFieldList().get(index).getType();
    if (indexFields.contains(currentField)) {
      if (currentSqlKind == null) {
        throw new RuntimeException(new Exception("SqlKind not set"));
      }
      if (currentSqlKind != SqlKind.EQUALS) {
        throw new RuntimeException(new Exception("For indexed fields only equality conditions are valid. You have used " + currentSqlKind.toString()));
      }
      isCurrentFieldIndexed = true;
    } else {
      isCurrentFieldIndexed = false;
    }
    return null;
  }

  @Override
  public Void visitLocalRef(RexLocalRef rexLocalRef) {
    currentValue = rexLocalRef.toString();
    currentType = rexLocalRef.getType();
    return null;
  }

  @Override
  public Void visitLiteral(RexLiteral rexLiteral) {
    currentValue = rexLiteral.toString();
    currentType = rexLiteral.getType();
    return null;
  }

  @Override
  public Void visitCall(RexCall rexCall) {
    /*
     * TODO how to evaluate an expression where col1+col2 > 5
     */
    switch (rexCall.getKind()) {
      case AND:
        composite = true;
        for (RexNode rexNode : rexCall.getOperands()) {
          rexNode.accept(this);
          if (currentQuery != null) {
            compositeQuery.add(currentQuery, BooleanClause.Occur.MUST);
          }
        }
        currentQuery = null;
        composite = false;
        break;
      case OR:
        composite = true;
        for (RexNode rexNode : rexCall.getOperands()) {
          rexNode.accept(this);
          if (currentQuery != null) {
            compositeQuery.add(currentQuery, BooleanClause.Occur.SHOULD);
          }
        }
        currentQuery = null;
        composite = false;
        break;
      case NOT:
        composite = true;
        for (RexNode rexNode : rexCall.getOperands()) {
          rexNode.accept(this);
          if (currentQuery != null) {
            compositeQuery.add(currentQuery, BooleanClause.Occur.MUST_NOT);
          }
        }
        currentQuery = null;
        composite = false;
        break;
      case EQUALS:
        currentQuery = null;
        isCurrentFieldIndexed = null;
        currentField = null;
        currentValue = null;
        currentType = null;
        currentSqlKind = SqlKind.EQUALS;
        rexCall.getOperands().get(0).accept(this);
        rexCall.getOperands().get(1).accept(this);
        QueryParser queryParser = new QueryParser(currentField, analyzer);

        if (isCurrentFieldIndexed) {
          try {
            currentQuery = queryParser.parse(currentValue);
          } catch (ParseException e) {
            throw new RuntimeException("Failed to parse the search string : " + currentValue, e);
          }
          //currentQuery = new TermQuery(new Term(currentField, currentValue));
        } else {
          currentQuery = new TermQuery(new Term(currentField, currentValue));
        }

        if (!composite) {
          compositeQuery.add(currentQuery, BooleanClause.Occur.MUST);
        }
        break;
      case GREATER_THAN:
      case GREATER_THAN_OR_EQUAL:
        currentSqlKind = rexCall.getKind();
        currentField = null;
        currentValue = null;
        currentType = null;
        rexCall.getOperands().get(0).accept(this);
        rexCall.getOperands().get(1).accept(this);

        if (currentType.getSqlTypeName().equals(SqlTypeName.INTEGER)) {
          currentQuery = NumericRangeQuery.newLongRange(currentField, Long.parseLong(currentValue), null, rexCall.getKind().equals(SqlKind.GREATER_THAN_OR_EQUAL), false);
        } else if (currentType.getSqlTypeName().equals(SqlTypeName.FLOAT) || currentType.getSqlTypeName().equals(SqlTypeName.REAL)) {
          currentQuery = NumericRangeQuery.newFloatRange(currentField, Float.parseFloat(currentValue), null, rexCall.getKind().equals(SqlKind.GREATER_THAN_OR_EQUAL), false);
        } else if (currentType.getSqlTypeName().equals(SqlTypeName.DOUBLE)) {
          currentQuery = NumericRangeQuery.newDoubleRange(currentField, Double.parseDouble(currentValue), null, rexCall.getKind().equals(SqlKind.GREATER_THAN_OR_EQUAL), false);
        } else {
          throw new RuntimeException("Hmmm... " + currentType.getSqlTypeName() + " is not supported yet ....");
        }

        if (!composite) {
          compositeQuery.add(currentQuery, BooleanClause.Occur.MUST);
        }

        break;
      case LESS_THAN:
      case LESS_THAN_OR_EQUAL:
        currentSqlKind = rexCall.getKind();
        currentField = null;
        currentValue = null;
        currentType = null;
        rexCall.getOperands().get(0).accept(this);
        rexCall.getOperands().get(1).accept(this);

        if (currentType.getSqlTypeName().equals(SqlTypeName.INTEGER)) {
          currentQuery = NumericRangeQuery.newLongRange(currentField, null, Long.parseLong(currentValue),false, rexCall.getKind().equals(SqlKind.LESS_THAN_OR_EQUAL));
        } else if (currentType.getSqlTypeName().equals(SqlTypeName.FLOAT) || currentType.getSqlTypeName().equals(SqlTypeName.REAL)) {
          currentQuery = NumericRangeQuery.newFloatRange(currentField, null, Float.parseFloat(currentValue), false, rexCall.getKind().equals(SqlKind.LESS_THAN_OR_EQUAL));
        } else if (currentType.getSqlTypeName().equals(SqlTypeName.DOUBLE)) {
          currentQuery = NumericRangeQuery.newDoubleRange(currentField, null, Double.parseDouble(currentValue), false, rexCall.getKind().equals(SqlKind.LESS_THAN_OR_EQUAL));
        } else {
          throw new RuntimeException("Hmmm... " + currentType.getSqlTypeName() + " is not supported yet ....");
        }

        if (!composite) {
          compositeQuery.add(currentQuery, BooleanClause.Occur.MUST);
        }

        break;
      case BETWEEN:
      case NOT_EQUALS:
      case LIKE:
      case SIMILAR:
      case IN:
      case PLUS:
      case MINUS:
      default:
    }

    return null;
  }

  @Override
  public Void visitOver(RexOver rexOver) {
    return null;
  }

  @Override
  public Void visitCorrelVariable(RexCorrelVariable rexCorrelVariable) {
    return null;
  }

  @Override
  public Void visitDynamicParam(RexDynamicParam rexDynamicParam) {
    return null;
  }

  @Override
  public Void visitRangeRef(RexRangeRef rexRangeRef) {
    return null;
  }

  @Override
  public Void visitFieldAccess(RexFieldAccess rexFieldAccess) {
    return null;
  }
}
