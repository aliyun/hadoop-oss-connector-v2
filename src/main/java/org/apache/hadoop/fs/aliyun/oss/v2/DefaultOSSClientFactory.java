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

import com.aliyun.sdk.service.oss2.ClientConfiguration;
import com.aliyun.sdk.service.oss2.DefaultOSSClient;
import com.aliyun.sdk.service.oss2.OSSClient;
import com.aliyun.sdk.service.oss2.credentials.CredentialsProvider;
import com.aliyun.sdk.service.oss2.transport.HttpClientOptions;
import com.aliyun.sdk.service.oss2.transport.apache5client.Apache5HttpClient;
import com.aliyun.sdk.service.oss2.transport.apache5client.Apache5HttpClientBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.aliyun.oss.v2.legency.AliyunOSSUtils;
import org.apache.hadoop.util.VersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;

import static org.apache.hadoop.fs.aliyun.oss.v2.Constants.*;
import static org.apache.hadoop.fs.aliyun.oss.v2.Constants.REGION;


public class DefaultOSSClientFactory extends Configured implements OSSClientFactory {

    Logger LOG =
            LoggerFactory.getLogger(DefaultOSSClientFactory.class);

    @Override
    public OSSClient createOSSClient(Configuration conf) throws IOException {

        ClientConfiguration clientConf = intiConfig(conf);
        try {
            OSSClient client = new DefaultOSSClient(clientConf);
            return client;

        } catch (Exception e) {
            LOG.error("Failed to create OSSClient", e);
            throw new IOException(e);
        }
    }

    @Override
    public OSSClient createAccOSSClient(Configuration conf) throws IOException {
        ClientConfiguration clientConf = intiConfig(conf);
        String accEndpoint = conf.getTrimmed(ACC_ENDPOINT_KEY, "");
        if (accEndpoint.isEmpty()) {
            return null;
        }

        clientConf = clientConf.toBuilder().endpoint(accEndpoint).build();
        try {
            OSSClient accClient = new DefaultOSSClient(clientConf);
            return accClient;

        } catch (Exception e) {
            LOG.error("Failed to create OSSClient", e);
            throw new IOException(e);
        }
    }


    ClientConfiguration intiConfig(Configuration conf) throws IOException {

        HttpClientOptions httpOptions = HttpClientOptions.custom()
                .readWriteTimeout(Duration.ofDays(conf.getInt(SOCKET_TIMEOUT_KEY,
                        SOCKET_TIMEOUT_DEFAULT)))
                .connectTimeout(Duration.ofDays(conf.getLong(ESTABLISH_TIMEOUT_KEY,
                        ESTABLISH_TIMEOUT_DEFAULT)))
                .build();
        Apache5HttpClient httpClient = Apache5HttpClientBuilder.create()
                .options(httpOptions)
                .maxConnections(conf.getInt(MAXIMUM_CONNECTIONS_KEY,
                        MAXIMUM_CONNECTIONS_DEFAULT)).
                build();

        CredentialsProvider provider =
                AliyunOSSUtils.getCredentialsProvider(conf);

        boolean enabledSSL = conf.getBoolean(SECURE_CONNECTIONS_KEY, SECURE_CONNECTIONS_DEFAULT);
        ClientConfiguration clientConf = ClientConfiguration.newBuilder()
                .httpClient(httpClient)
                .disableSsl(!enabledSSL)
                .region(conf.getTrimmed(REGION, ""))
                .endpoint(conf.getTrimmed(ENDPOINT_KEY, ""))
                .credentialsProvider(provider)
                .enabledRedirect(conf.getBoolean(REDIRECT_ENABLE_KEY, REDIRECT_ENABLE_DEFAULT))
                .retryMaxAttempts(conf.getInt(MAX_ERROR_RETRIES_KEY, MAX_ERROR_RETRIES_DEFAULT))
                .connectTimeout(Duration.ofMillis(conf.getInt(ESTABLISH_TIMEOUT_KEY, ESTABLISH_TIMEOUT_DEFAULT)))
                .userAgent(conf.get(USER_AGENT_PREFIX, USER_AGENT_PREFIX_DEFAULT) + ", Hadoop/" + VersionInfo.getVersion())
                .build();


        return clientConf;
    }
}