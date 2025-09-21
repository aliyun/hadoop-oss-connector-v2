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
package org.apache.hadoop.fs.aliyun.oss.v2.prefetchstream;

import org.apache.hadoop.fs.aliyun.oss.v2.Constants;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Locale;

import static org.apache.hadoop.fs.Options.OpenFileOptions.*;



public enum InputPolicy {

    Normal(FS_OPTION_OPENFILE_READ_POLICY_DEFAULT, false, true),
    Random(FS_OPTION_OPENFILE_READ_POLICY_RANDOM, true, false),
    Sequential(FS_OPTION_OPENFILE_READ_POLICY_SEQUENTIAL, false, false);

    private final String policy;
    private final boolean randomIO;
    private final boolean adaptive;

    InputPolicy(String policy,
                boolean randomIO,
                boolean adaptive) {
        this.policy = policy;
        this.randomIO = randomIO;
        this.adaptive = adaptive;
    }

    @Override
    public String toString() {
        return policy;
    }

    String getPolicy() {
        return policy;
    }

    boolean isRandomIO() {
        return randomIO;
    }

    public boolean isAdaptive() {
        return adaptive;
    }

    public static InputPolicy getPolicy(
            String name,
            @Nullable InputPolicy defaultPolicy) {
        String trimmed = name.trim().toLowerCase(Locale.ENGLISH);
        switch (trimmed) {
            case FS_OPTION_OPENFILE_READ_POLICY_ADAPTIVE:
            case FS_OPTION_OPENFILE_READ_POLICY_DEFAULT:
            case Constants.INPUT_FADV_NORMAL:
                return Normal;

            // all these options currently map to random IO.
//    case FS_OPTION_OPENFILE_READ_POLICY_HBASE:
            case FS_OPTION_OPENFILE_READ_POLICY_RANDOM:
            case FS_OPTION_OPENFILE_READ_POLICY_VECTOR:
                return Random;

            // columnar formats currently map to random IO,
            // though in future this may be enhanced.
            case "columnar":
            case "orc":
            case "parquet":
                return Random;

            // handle the sequential formats.
//    case FS_OPTION_OPENFILE_READ_POLICY_AVRO:
//    case FS_OPTION_OPENFILE_READ_POLICY_CSV:
//    case FS_OPTION_OPENFILE_READ_POLICY_JSON:
            case FS_OPTION_OPENFILE_READ_POLICY_SEQUENTIAL:
            case FS_OPTION_OPENFILE_READ_POLICY_WHOLE_FILE:
                return Sequential;
            default:
                return defaultPolicy;
        }
    }


    public static InputPolicy getFirstSupportedPolicy(
            Collection<String> policies,
            @Nullable InputPolicy defaultPolicy) {
        for (String s : policies) {
            InputPolicy nextPolicy = InputPolicy.getPolicy(s, null);
            if (nextPolicy != null) {
                return nextPolicy;
            }
        }
        return defaultPolicy;
    }

}
