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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.fs.aliyun.oss.v2.model;

import java.math.BigInteger;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;

/**
 * OSS Object's metadata. It has the user's custom metadata, as well as some
 * standard http headers sent to OSS, such as Content-Length, ETag, etc.
 */
public class ObjectMetadataParam {
    protected Map<String, Object> metadata = new TreeMap<String, Object>(String.CASE_INSENSITIVE_ORDER);

    protected long contentLength = 0;

    public static final String AES_256_SERVER_SIDE_ENCRYPTION = "AES256";

    public static final String KMS_SERVER_SIDE_ENCRYPTION = "KMS";


    /**
     * Sets the user's custom metadata.
     *
     * @param metadata The user's custom metadata.
     */
    public void setMetadata(Map<String, String> metadata) {
        this.metadata.clear();
        if (metadata != null && !metadata.isEmpty()) {
            this.metadata.putAll(metadata);
        }
    }

    /**
     * Sets the http header (SDK internal usage only).
     *
     * @param key   The key of header
     * @param value The value of the key.
     */
    public void setHeader(String key, Object value) {
        metadata.put(key, value);
    }

    /**
     * Removes the http header (SDK internal usage only).
     *
     * @param key The key of header.
     */
    public void removeHeader(String key) {
        metadata.remove(key);
    }


    /**
     * Gets the value of Last-Modified header, which means the last modified
     * time of the object.
     *
     * @return Object's last modified time.
     */
    public Date getLastModified() {

        return (Date) metadata.get(OSSHeaders.LAST_MODIFIED);

    }

    /**
     * Sets the value of Last-Modified header, which means the last modified
     * time of the object.
     *
     * @param lastModified Object's last modified time.
     */
    public void setLastModified(Date lastModified) {
        metadata.put(OSSHeaders.LAST_MODIFIED, lastModified);
    }


    /**
     * Gets Content-Length header, which is the object content's size.
     *
     * @return The object content's size.
     */
    public long getContentLength() {
        return contentLength;
    }

    /**
     * Sets the Content-Length header to indicate the object's size. The correct
     * Content-Length header is needed for a file upload.
     *
     * @param contentLength Object content size.
     */
    public void setContentLength(long contentLength) {
        this.contentLength = contentLength;
    }

    /**
     * Gets the Content-Type header to indicate the object content's type in
     * MIME type format.
     *
     * @return The content-type header in MIME type format.
     */
    public String getContentType() {
        return (String) metadata.get(OSSHeaders.CONTENT_TYPE);
    }

    /**
     * Sets the Content-Type header to indicate the object content's type in
     * MIME type format.
     *
     * @param contentType The content-type header in MIME type format.
     */
    public void setContentType(String contentType) {
        metadata.put(OSSHeaders.CONTENT_TYPE, contentType);
    }

    public String getContentMD5() {
        return (String) metadata.get(OSSHeaders.CONTENT_MD5);
    }

    public void setContentMD5(String contentMD5) {
        metadata.put(OSSHeaders.CONTENT_MD5, contentMD5);
    }

    /**
     * Gets the Content-Encoding header which is to encode the object content.
     *
     * @return Object content's encoding.
     */
    public String getContentEncoding() {
        return (String) metadata.get(OSSHeaders.CONTENT_ENCODING);
    }

    /**
     * Sets the Content-Encoding header which is to encode the object content.
     *
     * @param encoding Object content's encoding.
     */
    public void setContentEncoding(String encoding) {
        metadata.put(OSSHeaders.CONTENT_ENCODING, encoding);
    }

    /**
     * Sets Content-Disposition header.
     *
     * @param disposition Content-Disposition header.
     */
    public void setContentDisposition(String disposition) {
        metadata.put(OSSHeaders.CONTENT_DISPOSITION, disposition);
    }

    /**
     * Gets the ETag of the object. ETag is the 128bit MD5 signature in Hex.
     *
     * @return the object's ETag.
     */
    public String getETag() {
        return (String) metadata.get(OSSHeaders.ETAG);
    }

    /**
     * Gets the object's server side encryption.
     *
     * @return The server side encryption. Null means no encryption.
     */
    public String getServerSideEncryption() {
        return (String) metadata.get(OSSHeaders.OSS_SERVER_SIDE_ENCRYPTION);
    }

    /**
     * Sets the object's server side encryption.
     *
     * @param serverSideEncryption The server side encryption.
     */
    public void setServerSideEncryption(String serverSideEncryption) {
        metadata.put(OSSHeaders.OSS_SERVER_SIDE_ENCRYPTION, serverSideEncryption);
    }

    /**
     * Gets the object's server side encryption key ID.
     *
     * @return The server side encryption key ID. Null means no encryption key ID.
     */
    public String getServerSideEncryptionKeyId() {
        return (String) metadata.get(OSSHeaders.OSS_SERVER_SIDE_ENCRYPTION_KEY_ID);
    }

    /**
     * Sets the object's server side encryption key ID.
     *
     * @param serverSideEncryptionKeyId The server side encryption key ID.
     */
    public void setServerSideEncryptionKeyId(String serverSideEncryptionKeyId) {
        metadata.put(OSSHeaders.OSS_SERVER_SIDE_ENCRYPTION_KEY_ID, serverSideEncryptionKeyId);
    }

    /**
     * Sets the object's server side data encryption.
     *
     * @param serverSideDataEncryption The server side data encryption.
     */
    public void setServerSideDataEncryption(String serverSideDataEncryption) {
        metadata.put(OSSHeaders.OSS_SERVER_SIDE_DATA_ENCRYPTION, serverSideDataEncryption);
    }

    /**
     * Gets the object's server side data encryption.
     *
     * @return The server side data encryption. Null means no data encryption.
     */
    public String getServerSideDataEncryption() {
        return (String) metadata.get(OSSHeaders.OSS_SERVER_SIDE_DATA_ENCRYPTION);
    }

    /**
     * Gets the object's storage type, which only supports "normal" and
     * "appendable" for now.
     *
     * @return Object's storage type.
     */
    public String getObjectType() {
        return (String) metadata.get(OSSHeaders.OSS_OBJECT_TYPE);
    }


    /**
     * Gets the request Id.
     *
     * @return RequestId.
     */
    public String getRequestId() {
        return (String) metadata.get(OSSHeaders.OSS_HEADER_REQUEST_ID);
    }

    /**
     * Gets the version ID of the associated OSS object if available.
     * Version IDs are only assigned to objects when an object is uploaded to an
     * OSS bucket that has object versioning enabled.
     *
     * @return The version ID of the associated OSS object if available.
     */
    public String getVersionId() {
        return (String) metadata.get(OSSHeaders.OSS_HEADER_VERSION_ID);
    }

    /**
     * Gets the service crc.
     *
     * @return service crc.
     */
    public Long getServerCRC() {
        String strSrvCrc = (String) metadata.get(OSSHeaders.OSS_HASH_CRC64_ECMA);

        if (strSrvCrc != null) {
            BigInteger bi = new BigInteger(strSrvCrc);
            return bi.longValue();
        }
        return null;
    }


    public void setETag(String eTag) {
        metadata.put(OSSHeaders.ETAG, eTag);
    }
}
