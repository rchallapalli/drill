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

import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSyntax;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FindIndexFilters extends RexVisitorImpl<Void> {
  protected final DrillRel inputRel;
  protected List<String> indexFields;
  protected boolean foundIndexColumn;
  protected boolean foundLiteral;
  protected List<Map.Entry<String, String>> indexFilters = new ArrayList<AbstractMap.Entry<String, String>>();
  protected String currentIndexField;
  protected String currentSearchQuery;


  public FindIndexFilters(List<String> indexFields, DrillRel inputRel) {
    super(true);
    this.indexFields = indexFields;
    this.inputRel = inputRel;
  }

  public void analyze(RexNode exp) {
    exp.accept(this);
  }

  public List<Map.Entry<String, String>> getIndexFilters() {
    return indexFilters;
  }

  @Override
  public Void visitInputRef(RexInputRef inputRef) {
    int index = inputRef.getIndex();
    final RelDataTypeField field = inputRel.getRowType().getFieldList().get(index);
    if (indexFields.contains(field.getName())) {
      currentIndexField = field.getName();
      foundIndexColumn = true;
    }
    return null;
  }

  @Override
  public Void visitCall(RexCall call) {

    List<RexNode> operands = call.getOperands();
    foundIndexColumn = false;
    foundLiteral = false;
    final SqlSyntax syntax = call.getOperator().getSyntax();
    if (syntax == SqlSyntax.BINARY) {
      if (call.getKind() == SqlKind.EQUALS) {
        call.getOperands().get(0).accept(this);
        call.getOperands().get(1).accept(this);
        if (foundIndexColumn && foundLiteral) {
          String searchQuery = ((RexLiteral) call.getOperands().get(1)).getValue2().toString();
          indexFilters.add(new AbstractMap.SimpleEntry<String, String>(currentIndexField, searchQuery));
        }
      }
    }
    return null;
  }

  public Void visitLiteral(RexLiteral literal) {
    foundLiteral = true;
    currentSearchQuery = RexLiteral.stringValue(literal);
    return null;
  }

}
