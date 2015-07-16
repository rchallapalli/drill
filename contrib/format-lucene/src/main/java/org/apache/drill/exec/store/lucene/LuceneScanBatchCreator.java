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

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.record.CloseableRecordBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.dfs.DrillFileSystem;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class LuceneScanBatchCreator implements BatchCreator<LuceneSubScan> {
  @Override
  public CloseableRecordBatch getBatch(FragmentContext context, LuceneSubScan subScan, List<RecordBatch> children) throws ExecutionSetupException {
    List<RecordReader> readers = new ArrayList<RecordReader>();
    for (LuceneSubscanSpec spec : subScan.getLuceneSubscanSpecs()) {
      readers.add(new LuceneRecordReader(spec.getSegmentReader(), subScan.getColumns(), subScan.getSearchQueryString()));
    }

    OperatorContext opContext = context.newOperatorContext(subScan);
    DrillFileSystem dfs;
    try {
      dfs = opContext.newFileSystem(subScan.getFormatPlugin().getFsConf());
    } catch (IOException e) {
      throw new ExecutionSetupException(String.format("Failed to create FileSystem: %s", e.getMessage()), e);
    }

    return new ScanBatch(subScan, context, readers.iterator());
  }
}
