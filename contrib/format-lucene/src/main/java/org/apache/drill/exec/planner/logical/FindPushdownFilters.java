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

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexVisitor;

public class FindPushdownFilters implements RexVisitor<Void> {

  @Override
  public Void visitInputRef(RexInputRef rexInputRef) {
    return null;
  }

  ;

  @Override
  public Void visitLocalRef(RexLocalRef rexLocalRef) {
    return null;
  }

  @Override
  public Void visitLiteral(RexLiteral rexLiteral) {
    return null;
  }

  @Override
  public Void visitCall(RexCall rexCall) {
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

  enum PushFilter {PUSH, NO_PUSH, NO_PUSH_COMPLETE}
}
