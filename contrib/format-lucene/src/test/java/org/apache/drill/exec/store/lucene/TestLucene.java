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

import org.apache.drill.BaseTestQuery;
import org.apache.drill.common.types.TypeProtos;
import org.junit.Test;

public class TestLucene extends BaseTestQuery {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestLucene.class);

  /*
   * Test Cases TODO:
   *     1. Test combinations of indexed and non-indexed filters
   *     2. Test the above with Or, And, Not operators
   *     3. Test interpreter based evaluation as well
   *     4. Test a star query
   *     5. Test Queries with non-existing fields
   *     6. Test queries on indexes containing more than one segment reader
   *     7. Test pushdown when we have subqueries in the filter
   *     8. Test hierarchical lucene indexes
   */

  @Test
  public void luceneQueryFilter1() throws Exception {
    testBuilder()
        .sqlQuery("select path from dfs_test.`/home/rchallapalli/Desktop/Work/drill/exec/java-exec/src/test/resources/testframework/index` where contents='maxItemsPerBlock'")
        .unOrdered()
        .csvBaselineFile("testframework/lucene.tsv")
        .baselineTypes(TypeProtos.MinorType.VARCHAR)
        .baselineColumns("path")
        .build().run();
  }

  @Test
  public void luceneQueryFilter2() throws Exception {
    testBuilder()
        .sqlQuery("select path from dfs_test.`/home/rchallapalli/Desktop/Work/drill/exec/java-exec/src/test/resources/testframework/index` where contents='maxItemsPerBlock' and " +
            "contents = 'BlockTreeTermsIndex'")
        .unOrdered()
        .csvBaselineFile("testframework/lucene1.tsv")
        .baselineTypes(TypeProtos.MinorType.VARCHAR)
        .baselineColumns("path")
        .build().run();
  }

  @Test
  public void luceneQueryFilter3() throws Exception {
    testBuilder()
        .sqlQuery("select path from dfs_test.`/home/rchallapalli/Desktop/Work/drill/exec/java-exec/src/test/resources/testframework/index` where contents='maxItemsPerBlock' and " +
            "path = '/home/rchallapalli/Desktop/lucene-5.1.0/core/src/java/org/apache/lucene/codecs/blocktree/Stats.java'")
        .unOrdered()
        .csvBaselineFile("testframework/lucene2.tsv")
        .baselineTypes(TypeProtos.MinorType.VARCHAR)
        .baselineColumns("path")
        .build().run();
  }

  @Test
  public void luceneQueryFilter4() throws Exception {
    testBuilder()
        .sqlQuery("select RID from dfs_test.`/var/as/data/historical/venuepoint/luceneIndexes/ASEntity_all` where address='0' and zip_code='1352'")
        .unOrdered()
        .csvBaselineFile("testframework/lucene4.tsv")
        .baselineTypes(TypeProtos.MinorType.VARCHAR)
        .baselineColumns("RID")
        .build().run();
  }

  @Test
  public void testMoreThanString() throws Exception {
    testBuilder()
        //.sqlQuery("select RID from dfs_test.`/var/as/data/historical/venuepoint/luceneIndexes/ASEntity_all` where label > 'Rundvisning Produktion - Grupp'")
        //.sqlQuery("select RID from dfs_test.`/var/as/data/historical/venuepoint/luceneIndexes/ASEntity_all` where label > 'Rundvisning Produktion - Grupp' and label < 'S'")
        //.sqlQuery("select RID from dfs_test.`/var/as/data/historical/venuepoint/luceneIndexes/ASEntity_all` where label BETWEEN 'Rundvisning Produktion' AND 'S'")
        //.sqlQuery("select RID from dfs_test.`/var/as/data/historical/venuepoint/luceneIndexes/ASEntity_all` where (label = 'Karen Lisbeth Høyer' AND label = 'Lisbeth Daluiso Salmonsen') OR label = 'Karen Lisbeth Høyer' ")
        //.sqlQuery("select RID from dfs_test.`/var/as/data/historical/venuepoint/luceneIndexes/ASEntity_all` where label = 'Karen Lisbeth Høyer'")
        //.sqlQuery("select RID, label, category from dfs_test.`/var/as/data/historical/venuepoint/luceneIndexes/ASEntity_all` where RID = '#180:24'")
        //.sqlQuery("select label, category, entity_id, RID, begins from dfs_test.`/var/as/data/historical/venuepoint/luceneIndexes/ASEntity_all` where RID = '#180:24'")
        //.sqlQuery("select label, category, entity_id, RID, begins from dfs_test.`/var/as/data/historical/venuepoint/luceneIndexes/ASEntity_all` where label > 'Z'")
        //.sqlQuery("select label, category, entity_id, RID, begins from dfs_test.`/var/as/data/historical/venuepoint/luceneIndexes/ASEntity_all` where label between 'Ø' and 'Øb'")
        //.sqlQuery("select RID from dfs_test.`/var/as/data/historical/venuepoint/luceneIndexes/ASEntity_all`")
        //.sqlQuery("select RID from dfs_test.`/var/as/data/historical/venuepoint/luceneIndexes/ASEntity_all` where label <> 'Karen Lisbeth Høyer' AND label > 'A'") //2019401
        //.sqlQuery("select RID from dfs_test.`/var/as/data/historical/venuepoint/luceneIndexes/ASEntity_all` where label > 'A'") //
        .sqlQuery("select RID from dfs_test.`/var/as/data/historical/venuepoint/luceneIndexes/ASEntity_all` where entity_id = '1-1577434'") //
        //.sqlQuery("select RID from dfs_test.`/var/as/data/historical/venuepoint/luceneIndexes/ASEntity_all` where entity_id > '1-1577434'") //
        //.sqlQuery("select RID from dfs_test.`/var/as/data/historical/venuepoint/luceneIndexes/ASEntity_all` where (label <> 'Karen Lisbeth Høyer' and label > 'A') OR (label = 'Karen Lisbeth Høyer' OR label = 'Karen Lisbeth Høyer') AND label = 'Karen Lisbeth Høyer'")
        //.sqlQuery("select RID from dfs_test.`/var/as/data/historical/venuepoint/luceneIndexes/ASEntity_all` where label LIKE 'Kar*'")
        //.sqlQuery("select RID from dfs_test.`/var/as/data/historical/venuepoint/luceneIndexes/ASEntity_all` where label SIMILAR TO 'Karen Lisbeth H*'")
        //.sqlQuery("select RID from dfs_test.`/var/as/data/historical/venuepoint/luceneIndexes/ASEntity_all` where label IN ('Lisbeth Østergaar','Sjanne Petersen')")
        //.sqlQuery("select RID from dfs_test.`/var/as/data/historical/venuepoint/luceneIndexes/ASEntity_all` where label like '\"Rundvisning Produktion\"~4'")
        .unOrdered()
        .csvBaselineFile("testframework/lucene4.tsv")
        .baselineTypes(TypeProtos.MinorType.VARCHAR)
        .baselineColumns("RID")
        .build().run();
  }

  @Test
  public void testMoreThanNumeric() throws Exception {
    testBuilder()
        .sqlQuery("select RID from dfs_test.`/var/as/data/historical/venuepoint/luceneIndexes/ASEntity_all` where `begins` > 0")
        .unOrdered()
        .csvBaselineFile("testframework/lucene4.tsv")
        .baselineTypes(TypeProtos.MinorType.VARCHAR)
        .baselineColumns("RID")
        .build().run();
  }

  @Test
  public void testQueriesWithNoConditions() throws Exception {
    testBuilder()
        .sqlQuery("select RID from dfs_test.`/var/as/data/historical/venuepoint/luceneIndexes/ASEntity_all`")
        .unOrdered()
        .csvBaselineFile("testframework/lucene4.tsv")
        .baselineTypes(TypeProtos.MinorType.VARCHAR)
        .baselineColumns("RID")
        .build().run();
  }
}
