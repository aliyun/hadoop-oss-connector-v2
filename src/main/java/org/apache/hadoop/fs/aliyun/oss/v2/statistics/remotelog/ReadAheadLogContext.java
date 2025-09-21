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

public class ReadAheadLogContext extends RemoteLogContext {
    private int toBlockNumber;
    private long readPos;
    private int prefetchCount;
    private int endBlockNumber = -1;
    private int len;
    private long loop;
    private int releaseNum = -1;
    private int cacheNum = -1;
    private boolean cancelPrefetches = false;
    private boolean outOfOrderRead = false;

    public ReadAheadLogContext(RemoteLogContext logContext) {
        super(logContext);
        this.setType("RA");
    }

    public void setToBlockNumber(int toBlockNumber) {
        this.toBlockNumber = toBlockNumber;
    }

    public void setReadPos(long readPos) {
        this.readPos = readPos;
    }

    public void setPrefetchCount(int prefetchCount) {
        this.prefetchCount = prefetchCount;
    }

    public void setEndBlockNumber(int endBlockNumber) {
        this.endBlockNumber = endBlockNumber;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        if (type != null) {
            sb.append("_ty").append(type);
        }
        if (uuid != null) {
            sb.append("_ui").append(uuid);
        }
        if (now != null) {
            sb.append("_ts").append(now);
        }
        sb.append("_rp").append(readPos);
        sb.append("_pc").append(prefetchCount);
        sb.append("_tb").append(toBlockNumber);

        if (endBlockNumber >= 1) {
            sb.append("_eb").append(endBlockNumber);
        }

        sb.append("_le").append(len);
        sb.append("_lp").append(loop);
        if (releaseNum >= 1) {
            sb.append("_ln").append(releaseNum);
        }

        if (cacheNum >= 1) {
            sb.append("_cn").append(cacheNum);
        }

        if (cancelPrefetches) {
            sb.append("_cp").append("Y");
        }

        sb.append("_od").append(outOfOrderRead);

        if (parent != null) {
            sb.append("_pa").append(parent);
        }
        sb.append('}');
        return sb.toString();
    }

    public void setLen(int len) {
        this.len = len;
    }

    public void setLoop(long loop) {
        this.loop = loop;
    }

    public void setRelease(int blockNumber) {
        this.releaseNum = blockNumber;
    }

    public void setCache(int blockNumber) {
        this.cacheNum = blockNumber;
    }

    public void cancelPrefetches() {
        this.cancelPrefetches = true;
    }

    public void setOutOfOrderRead() {
        this.outOfOrderRead = true;
    }
}
