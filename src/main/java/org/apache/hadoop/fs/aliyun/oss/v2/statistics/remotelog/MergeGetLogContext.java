/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.aliyun.oss.v2.statistics.remotelog;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.aliyun.oss.v2.prefetchstream.BufferData;

import java.util.ArrayList;
import java.util.List;

public class MergeGetLogContext extends RemoteLogContext {
    private int retryTime = 0;
    private BufferData.State firstState;
    private int dataListSize = 0;
    private List<Pair<Integer, String>> dataList = new ArrayList<>();

    public MergeGetLogContext(ReadAheadLogContext readAheadLogContext) {
        super(readAheadLogContext);
        this.type = "BM";
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        if (type != null) {
            sb.append("_ty").append(type);
        }
        if (now != null) {
            sb.append("_ts").append(now);
        }
        if (uuid != null) {
            sb.append("_ui").append(uuid);
        }
        sb.append("_rt").append(retryTime);
        sb.append("_fs").append(firstState);
        sb.append("_dl").append(dataListSize);
        sb.append("_dr").append(dataList.toString());

        if (parent != null) {
            sb.append("_pa").append(parent);
        }
        sb.append('}');
        return sb.toString();
    }

    public void increaseRetryTime() {
        this.retryTime++;
    }

    public void setFirstState(BufferData.State state) {
        this.firstState = state;
    }

    public void setDataListSize(int size) {
        this.dataListSize = size;
    }

    public int getCurrentTime() {
        return retryTime;
    }

    public void setRestryRes(boolean isFirst, int blockNum, boolean done, int currentTime) {
        StringBuilder sb = new StringBuilder();
        sb.append(isFirst ? "f" : "s").append("-").append(done ? "t" : "f").append("-").append(blockNum).append("-")
                .append(lastError);

        dataList.add(Pair.of(currentTime, sb.toString()));

    }
}
