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
import org.apache.lucene.analysis.standard.ClassicAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.TermQuery;
/*
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.TermQuery;
*/

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SqlFilterToLuceneQuery extends RexVisitorImpl<Void> {
  private final List<String> indexFields;
  private final DrillRel inputRel;
  private BooleanQuery compositeQuery;
  private Boolean isCurrentFieldIndexed;
  private SqlKind currentSqlKind;
  private Analyzer analyzer;
  private Query currentQuery;

  final Map<SqlKind, BooleanClause.Occur> BASE_KIND = new HashMap() {{ put(SqlKind.OR, BooleanClause.Occur.SHOULD); put(SqlKind.AND, BooleanClause.Occur.MUST); put(SqlKind.NOT, BooleanClause.Occur.MUST_NOT); }};


  private String currentField;
  private String currentValue;
  private RelDataType currentType;

  public SqlFilterToLuceneQuery(List<String> indexFields, DrillRel inputRel) {
    super(true);
    this.indexFields = indexFields;
    this.inputRel = inputRel;
    compositeQuery = new BooleanQuery();
    isCurrentFieldIndexed = false;
    //analyzer = new ClassicAnalyzer(); //The classic analyser handles escaping in an almost
    analyzer = new StandardAnalyzer(); //The classic analyser handles escaping in an almost
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

    //Support nested conditions
    if (BASE_KIND.containsKey(rexCall.getKind())) {
        for (RexNode rexNode : rexCall.getOperands()) {
          SqlFilterToLuceneQuery subQuery = new SqlFilterToLuceneQuery(this.indexFields, inputRel);
          rexNode.accept(subQuery);
          compositeQuery.add(subQuery.getLuceneQuery(), BASE_KIND.get(rexCall.getKind()));
        }
    } else {
        //Support nested field/value conditions
        currentQuery = null;
        isCurrentFieldIndexed = null;
        currentField = null;
        currentValue = null;
        currentType = null;
        currentSqlKind = SqlKind.EQUALS;
        rexCall.getOperands().get(0).accept(this);
        rexCall.getOperands().get(1).accept(this);

        if (currentValue.startsWith("'") || currentValue.endsWith("'")) {
            currentValue = currentValue.substring(1).substring(0,currentValue.length()-2);
            currentValue = currentValue.replace("-","\\-").replace("+","\\+").replace(":","\\:");
        }

        switch (rexCall.getKind()) {
            case EQUALS:
            case NOT_EQUALS:
                //All equals queries should be handled as such. LIKE can be used for Lucene queries
                //Todo - find out why this behaves differently than same query in Luke when dealing with - and + values in strings
                if (isCurrentFieldIndexed) {
                    currentQuery = new QueryParser(currentField, analyzer).createPhraseQuery(currentField, currentValue);
                } else {
                    currentQuery = new TermQuery(new Term(currentField, currentValue));
                }
                compositeQuery.add(currentQuery, (rexCall.getKind().equals(SqlKind.EQUALS)) ? BooleanClause.Occur.MUST : BooleanClause.Occur.MUST_NOT);
                //System.out.println("currentQuery " + currentQuery);
                break;
            case GREATER_THAN_OR_EQUAL:
                //toto - Support a A single NOT-EQUAL query by adding a "*:* AND" to the query (Now it returns nothing)
            case GREATER_THAN:
                if (currentType.getSqlTypeName().equals(SqlTypeName.INTEGER)) {
                    currentQuery = NumericRangeQuery.newLongRange(currentField, Long.parseLong(currentValue), null, rexCall.getKind().equals(SqlKind.GREATER_THAN_OR_EQUAL), false);
                } else if (currentType.getSqlTypeName().equals(SqlTypeName.FLOAT) || currentType.getSqlTypeName().equals(SqlTypeName.REAL)) {
                    currentQuery = NumericRangeQuery.newFloatRange(currentField, Float.parseFloat(currentValue), null, rexCall.getKind().equals(SqlKind.GREATER_THAN_OR_EQUAL), false);
                } else if (currentType.getSqlTypeName().equals(SqlTypeName.DOUBLE)) {
                    currentQuery = NumericRangeQuery.newDoubleRange(currentField, Double.parseDouble(currentValue), null, rexCall.getKind().equals(SqlKind.GREATER_THAN_OR_EQUAL), false);
                } else if (currentType.getSqlTypeName().equals(SqlTypeName.CHAR) || currentType.getSqlTypeName().equals(SqlTypeName.VARCHAR)) {
                    currentQuery = TermRangeQuery.newStringRange(currentField, currentValue, null, rexCall.getKind().equals(SqlKind.GREATER_THAN_OR_EQUAL), false);
                } else {
                    throw new RuntimeException("Lucene plugin does not yet support '" + currentType.getSqlTypeName() + "' based range queryes");
                }
                compositeQuery.add(currentQuery, BooleanClause.Occur.MUST);
                break;
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
                if (currentType.getSqlTypeName().equals(SqlTypeName.INTEGER)) {
                    currentQuery = NumericRangeQuery.newLongRange(currentField, null, Long.parseLong(currentValue),false, rexCall.getKind().equals(SqlKind.LESS_THAN_OR_EQUAL));
                } else if (currentType.getSqlTypeName().equals(SqlTypeName.FLOAT) || currentType.getSqlTypeName().equals(SqlTypeName.REAL)) {
                    currentQuery = NumericRangeQuery.newFloatRange(currentField, null, Float.parseFloat(currentValue), false, rexCall.getKind().equals(SqlKind.LESS_THAN_OR_EQUAL));
                } else if (currentType.getSqlTypeName().equals(SqlTypeName.DOUBLE)) {
                    currentQuery = NumericRangeQuery.newDoubleRange(currentField, null, Double.parseDouble(currentValue), false, rexCall.getKind().equals(SqlKind.LESS_THAN_OR_EQUAL));
                } else if (currentType.getSqlTypeName().equals(SqlTypeName.CHAR) || currentType.getSqlTypeName().equals(SqlTypeName.VARCHAR)) {
                    currentQuery = TermRangeQuery.newStringRange(currentField, null, currentValue, false, rexCall.getKind().equals(SqlKind.LESS_THAN_OR_EQUAL));
                } else {
                    throw new RuntimeException("Lucene plugin does not yet support '" + currentType.getSqlTypeName() + "' based range queryes");
                }
                compositeQuery.add(currentQuery, BooleanClause.Occur.MUST);
                break;
            case SIMILAR:
                //todo - decide if this should behave differently (both support Lucene operators now)
            case LIKE:
                //todo - This is not behaving as expected (I will continue to investigate) - http://www.avajava.com/tutorials/lessons/how-do-i-perform-a-wildcard-query.html
                try {
                    if (currentValue.contains("*") || currentValue.contains("?")) {
                        currentQuery = new WildcardQuery(new Term(currentField, currentValue));
                    } else {
                        QueryParser likeParser = new QueryParser(currentField, analyzer);
                        currentQuery = likeParser.parse( currentValue );
                    }
                    //System.out.println("currentQuery: " + currentQuery);
                } catch (ParseException e) {
                    throw new RuntimeException("Lucene parsing error for a Like condition: " + e, e);
                }

                compositeQuery.add(currentQuery, BooleanClause.Occur.MUST);
                break;
            case BETWEEN:
                //todo - BETWEEN is implemented as two MIN and MAX statements and that does not map well to Lucene (I will examine)
            case IN:
                //todo - IN is implemented as multiple EQUALS statements (I will examine)
            case PLUS:
            case MINUS:
            default:
                throw new RuntimeException("Lucene plugin does not yet support '" + rexCall.getKind() + "' conditions");
        }
    }

    //System.out.println("compositeQuery: " + compositeQuery);
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
