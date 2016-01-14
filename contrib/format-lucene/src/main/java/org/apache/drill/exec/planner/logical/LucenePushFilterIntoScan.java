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


import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rex.RexNode;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.planner.common.DrillJoinRelBase;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.apache.drill.exec.store.lucene.LuceneGroupScan;
import org.apache.drill.exec.store.lucene.LuceneScanSpec;

import java.io.IOException;

public abstract class LucenePushFilterIntoScan extends StoragePluginOptimizerRule {

  public static final StoragePluginOptimizerRule LUCENE_FILTER_ON_SCAN = new LucenePushFilterIntoScan(RelOptHelper.some(DrillFilterRel.class, RelOptHelper.any(DrillScanRel.class)),
      "LucenePushPartitionFilterIntoScan:Filter_On_Scan") {
    @Override
    public boolean matches(RelOptRuleCall call) {
      final DrillScanRel scan = (DrillScanRel) call.rel(1);

      GroupScan groupScan = scan.getGroupScan();
      if (groupScan instanceof LuceneGroupScan) {
        if (((LuceneGroupScan) groupScan).supportsFilterPushDown()) {
          return true;
        }
      }
      return false;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final DrillFilterRel filterRel = (DrillFilterRel) call.rel(0);
      final DrillScanRel scanRel = (DrillScanRel) call.rel(1);
      doOnMatch(call, filterRel, null, scanRel);
    }
  };

  public static final StoragePluginOptimizerRule LUCENE_FILTER_ON_PROJECT = new LucenePushFilterIntoScan(RelOptHelper.some(DrillFilterRel.class, RelOptHelper.some(DrillProjectRel.class, RelOptHelper.any(DrillScanRel.class))),
      "LucenePushPartitionFilterIntoScan:Filter_On_Project") {
    @Override
    public boolean matches(RelOptRuleCall call) {

      final DrillScanRel scan = (DrillScanRel) call.rel(2);
      GroupScan groupScan = scan.getGroupScan();
      if (groupScan instanceof LuceneGroupScan) {
        if (((LuceneGroupScan) groupScan).supportsFilterPushDown()) {
          return true;
        }
      }
      return false;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final DrillFilterRel filterRel = (DrillFilterRel) call.rel(0);
      final DrillProjectRel projectRel = (DrillProjectRel) call.rel(1);
      final DrillScanRel scanRel = (DrillScanRel) call.rel(2);
      doOnMatch(call, filterRel, projectRel, scanRel);
    }
  };

  public static final StoragePluginOptimizerRule LUCENE_FILTER_ON_JOIN_REL = new LucenePushFilterIntoScan(RelOptHelper.some(DrillJoinRel.class, RelOptHelper.any(DrillScanRel.class)), "LucenePushPartitionFilterIntoScan:Filter_On_Join_Filter") {

    @Override
    public void doOnJoinMatch(RelOptRuleCall call, DrillJoinRel joinRel, DrillScanRel scanRel) {
      super.doOnJoinMatch(call, joinRel, scanRel);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {

      final DrillScanRel scan = (DrillScanRel) call.rel(1);
      GroupScan groupScan = scan.getGroupScan();

      if (groupScan instanceof LuceneGroupScan) {
        if (((LuceneGroupScan) groupScan).supportsFilterPushDown()) {
          return true;
        }
      }
      return false;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      System.out.println("call " + call.rels + " " + call.rels.length);
      final DrillJoinRel joinRel = (DrillJoinRel) call.rel(0);
      final DrillScanRel scanRel = (DrillScanRel) call.rel(1);
      doOnJoinMatch(call, joinRel, scanRel);
    }

  };

  public void doOnJoinMatch(RelOptRuleCall call, DrillJoinRel joinRel, DrillScanRel scanRel) {

    DrillRel inputRel = scanRel;

    scanRel.getGroupScan();

    SqlFilterToLuceneQuery sqlFilterToLuceneQuery = new SqlFilterToLuceneQuery(((LuceneGroupScan) scanRel.getGroupScan()).getIndexFields(), inputRel);

    //filterRel.getCondition().accept(sqlFilterToLuceneQuery);

    LuceneGroupScan luceneGroupScan = (LuceneGroupScan) scanRel.getGroupScan();
    LuceneScanSpec luceneScanSpec = luceneGroupScan.getLuceneScanSpec();
    LuceneScanSpec newLuceneScanSpec = new LuceneScanSpec(luceneScanSpec.getSelectionRoot(), luceneScanSpec.getSelection(), sqlFilterToLuceneQuery.getLuceneQuery());
    try {
      LuceneGroupScan newLuceneGroupScan = new LuceneGroupScan(
              luceneGroupScan.getUserName(),
              newLuceneScanSpec,
              luceneGroupScan.getFormatPlugin(),
              luceneGroupScan.getColumns()
      );

      DrillScanRel newScanRel = new DrillScanRel(
              scanRel.getCluster(),
              scanRel.getTraitSet().plus(DrillRel.DRILL_LOGICAL),
              scanRel.getTable(),
              newLuceneGroupScan,
              scanRel.getRowType(),
              scanRel.getColumns()
      );

      call.transformTo(newScanRel);
    } catch (IOException e) {
      throw new DrillRuntimeException(e);
    }

  }

  private LucenePushFilterIntoScan(RelOptRuleOperand operand, String id) {
    super(operand, id);
  }

  public void doOnMatch(RelOptRuleCall call, DrillFilterRel filterRel, DrillProjectRel projectRel, DrillScanRel scanRel) {

    DrillRel inputRel = projectRel != null ? projectRel : scanRel;
    SqlFilterToLuceneQuery sqlFilterToLuceneQuery = new SqlFilterToLuceneQuery(((LuceneGroupScan) scanRel.getGroupScan()).getIndexFields(), inputRel);

    filterRel.getCondition().accept(sqlFilterToLuceneQuery);

    LuceneGroupScan luceneGroupScan = (LuceneGroupScan) scanRel.getGroupScan();
    LuceneScanSpec luceneScanSpec = luceneGroupScan.getLuceneScanSpec();
    LuceneScanSpec newLuceneScanSpec = new LuceneScanSpec(luceneScanSpec.getSelectionRoot(), luceneScanSpec.getSelection(), sqlFilterToLuceneQuery.getLuceneQuery());
    try {
      LuceneGroupScan newLuceneGroupScan = new LuceneGroupScan(
          luceneGroupScan.getUserName(),
          newLuceneScanSpec,
          luceneGroupScan.getFormatPlugin(),
          luceneGroupScan.getColumns()
      );
      DrillScanRel newScanRel = new DrillScanRel(
          scanRel.getCluster(),
          scanRel.getTraitSet().plus(DrillRel.DRILL_LOGICAL),
          scanRel.getTable(),
          newLuceneGroupScan,
          scanRel.getRowType(),
          scanRel.getColumns()
      );

      if (projectRel != null) {
        DrillProjectRel newProjectRel = new DrillProjectRel(projectRel.getCluster(), projectRel.getTraitSet(),
            newScanRel, projectRel.getProjects(), projectRel.getRowType());
        inputRel = newProjectRel;
      } else {
        inputRel = newScanRel;
      }

      // does this remove the filter from the project ?
      call.transformTo(inputRel);
    } catch (IOException e) {
      throw new DrillRuntimeException(e);
    }

  }
}
