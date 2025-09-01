/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.aliyun.oss.v2.performance;

import java.util.EnumSet;
import java.util.Set;

/**
 * Enum of probes which be used by AliyunOSS.
 */

public enum AliyunOSSStatusProbeEnum {

    /**
     * The actual path.
     */
    Head,
    /**
     * HEAD of the path + /.
     */
    DirMarker,
    /**
     * LIST under the path.
     */
    List;

    /**
     * Look for files and directories.
     */
    public static final Set<AliyunOSSStatusProbeEnum> ALL = EnumSet.of(Head, DirMarker, List);

    /**
     * We only want the HEAD.
     */
    public static final Set<AliyunOSSStatusProbeEnum> HEAD_ONLY = EnumSet.of(Head);

    /**
     * List operation only.
     */
    public static final Set<AliyunOSSStatusProbeEnum> LIST_ONLY = EnumSet.of(List);

    /**
     * Look for files.
     */
    public static final Set<AliyunOSSStatusProbeEnum> FILE = HEAD_ONLY;

    /**
     * look for directories.
     * <p>
     * The cost of list is much higher than Head DirMarker. We try to perform Head
     * DirMarker checks before listing.
     */
    public static final Set<AliyunOSSStatusProbeEnum> DIRECTORIES = EnumSet.of(DirMarker, List);
}