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

package org.apache.hadoop.fs.aliyun.oss.v2;

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.aliyun.oss.v2.performance.AliyunOSSDirEmptyFlag;

/**
 * This class is used by listStatus for oss files.
 */

@InterfaceStability.Evolving
public class OSSFileStatus extends FileStatus {
    private AliyunOSSDirEmptyFlag emptyFlag;
    private String eTag;
    private String versionId;


    public OSSFileStatus(long length, boolean isdir, int blockReplication, long blocksize,
                         long modTime, Path path, String user) {
        super(length, isdir, blockReplication, blocksize, modTime, path);
        setOwner(user);
        setGroup(user);
        setEmptyFlag(AliyunOSSDirEmptyFlag.UNKNOWN);
        setETag(null);
        setVersionId(null);
    }

    public OSSFileStatus(long length, boolean isdir, int blockReplication, long blocksize,
                         long modTime, Path path, String user,String eTag,String versionId) {
        super(length, isdir, blockReplication, blocksize, modTime, path);
        setOwner(user);
        setGroup(user);
        setEmptyFlag(AliyunOSSDirEmptyFlag.UNKNOWN);
        setETag(eTag);
        setVersionId(versionId);
    }

    public OSSFileStatus(AliyunOSSDirEmptyFlag emptyStatus, long length, int blockReplication,
                         long blocksize, long modTime, Path path, String user) {
        //If emptyFlag is set, this is definitely a directory
        super(length, true, blockReplication, blocksize, modTime, path);
        setOwner(user);
        setGroup(user);
        setEmptyFlag(emptyStatus);
        setETag(null);
        setVersionId(null);
    }

    public OSSFileStatus(AliyunOSSDirEmptyFlag emptyStatus, long length, int blockReplication,
                         long blocksize, long modTime, Path path, String user, String eTag,String versionId) {
        //If emptyFlag is set, this is definitely a directory
        super(length, true, blockReplication, blocksize, modTime, path);
        setOwner(user);
        setGroup(user);
        setEmptyFlag(emptyStatus);
        setETag(eTag);
        setVersionId(versionId);
    }

    private void setVersionId(String versionId) {
        this.versionId = versionId;
    }

    private void setETag(String eTag) {
        this.eTag = eTag;
    }

    public AliyunOSSDirEmptyFlag getEmptyFlag() {
        return emptyFlag;
    }

    public AliyunOSSDirEmptyFlag setEmptyFlag(AliyunOSSDirEmptyFlag status) {
        return emptyFlag = status;
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    public String geteTag() {
        return eTag;
    }

    public String getVersionId() {
        return versionId;
    }
}