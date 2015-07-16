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

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.dfs.FormatMatcher;
import org.apache.drill.exec.store.dfs.FormatPlugin;
import org.apache.drill.exec.store.dfs.FormatSelection;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.index.SegmentInfos;

import java.io.IOException;

public class LuceneFormatMatcher extends FormatMatcher {
  FormatPlugin plugin;

  public LuceneFormatMatcher(FormatPlugin plugin) {
    this.plugin = plugin;
  }

  @Override
  public boolean supportDirectoryReads() {
    return false;
  }

  @Override
  public FormatSelection isReadable(DrillFileSystem fs, FileSelection selection) throws IOException {
    String[] fileNames = new String[selection.getAsFiles().size()];
    int i = 0;
    for (FileStatus fileStatus : selection.getFileStatusList(fs)) {
      Path filePath = fileStatus.getPath();
      fileNames[i] = filePath.getName();
      i++;
    }
    long lastCommitGeneration = SegmentInfos.getLastCommitGeneration(fileNames);
    if (lastCommitGeneration != -1) {
      return new FormatSelection(getFormatPlugin().getConfig(), selection);
    }

    return null;
  }

  @Override
  public FormatPlugin getFormatPlugin() {
    return plugin;
  }
}
