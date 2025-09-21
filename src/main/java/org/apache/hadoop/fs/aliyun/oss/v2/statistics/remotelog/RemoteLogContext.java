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

import java.time.Instant;
import java.util.UUID;

public class RemoteLogContext {
    public String getUuid() {
        return uuid;
    }

    protected String uuid;
    protected Instant now;
    protected RemoteLogContext parent;
    protected String type;
    protected String lastError;

    RemoteLogContext() {
        this.uuid = UUID.randomUUID().toString().substring(0, 6);
        this.now = Instant.now();
    }

    RemoteLogContext(RemoteLogContext parent) {
        this.uuid = UUID.randomUUID().toString().substring(0, 4);
        this.now = Instant.now();
        this.parent = parent;
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
        if (parent != null) {
            sb.append("_pa").append(parent);
        }
        sb.append('}');
        return sb.toString();
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setLastError(String s) {
        this.lastError = s;
    }

    public String getLastError() {
        return lastError;
    }
}
