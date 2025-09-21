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
package org.apache.hadoop.fs.aliyun.oss.v2.acc;

import org.apache.hadoop.fs.aliyun.oss.v2.legency.AliyunOSSInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class OssAccRuleManager {
    private List<OssAccRule> rules;
    public static final Logger LOG = LoggerFactory.getLogger(OssAccRuleManager.class);

    public OssAccRuleManager(String content) {
        LOG.debug("OssAccRuleManager: {}", content);
        if (content.isEmpty()) {
            this.rules = new ArrayList<>();
            return;
        }

        this.rules = new ArrayList<>();

        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document document = builder.parse(new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)));

            NodeList ruleNodes = document.getElementsByTagName("rule");
            for (int i = 0; i < ruleNodes.getLength(); i++) {
                Element ruleElement = (Element) ruleNodes.item(i);

                // Extract bucket pattern (default to "*" if not specified)
                String bucketPattern = "*";
                NodeList bucketPatternNodes = ruleElement.getElementsByTagName("bucketPattern");
                if (bucketPatternNodes.getLength() > 0) {
                    bucketPattern = bucketPatternNodes.item(0).getTextContent();
                }

                // Extract key prefixes
                List<String> keyPrefixes = new ArrayList<>();
                NodeList keyPrefixNodes = ruleElement.getElementsByTagName("keyPrefix");
                for (int j = 0; j < keyPrefixNodes.getLength(); j++) {
                    keyPrefixes.add(keyPrefixNodes.item(j).getTextContent());
                }

                // Extract key suffixes
                List<String> keySuffixes = new ArrayList<>();
                NodeList keySuffixNodes = ruleElement.getElementsByTagName("keySuffix");
                for (int j = 0; j < keySuffixNodes.getLength(); j++) {
                    keySuffixes.add(keySuffixNodes.item(j).getTextContent());
                }

                // Extract size ranges
                List<ObjectRange> sizeRanges = new ArrayList<>();
                NodeList rangeNodes = ruleElement.getElementsByTagName("range");
                for (int j = 0; j < rangeNodes.getLength(); j++) {
                    Element rangeElement = (Element) rangeNodes.item(j);
                    String minSizeStr = rangeElement.getElementsByTagName("minSize").item(0).getTextContent();
                    String maxSizeStr = rangeElement.getElementsByTagName("maxSize").item(0).getTextContent();

                    long minSize, maxSize;
                    if ("end".equals(minSizeStr)) {
                        minSize = Long.MAX_VALUE;
                    } else if (minSizeStr.startsWith("end-")) {
                        minSize = Long.MAX_VALUE - Long.parseLong(minSizeStr.substring(4));
                    } else {
                        minSize = Long.parseLong(minSizeStr);
                    }

                    if ("end".equals(maxSizeStr)) {
                        maxSize = Long.MAX_VALUE;
                    } else if (maxSizeStr.startsWith("end-")) {
                        maxSize = Long.MAX_VALUE - Long.parseLong(maxSizeStr.substring(4));
                    } else {
                        maxSize = Long.parseLong(maxSizeStr);
                    }

                    sizeRanges.add(new ObjectRange(minSize, maxSize));
                }

                // Extract IO size ranges
                List<IOSizeRange> ioSizeRanges = new ArrayList<>();
                NodeList ioSizeRangeNodes = ruleElement.getElementsByTagName("ioSizeRange");
                for (int j = 0; j < ioSizeRangeNodes.getLength(); j++) {
                    Element ioSizeRangeElement = (Element) ioSizeRangeNodes.item(j);
                    String ioType = ioSizeRangeElement.getElementsByTagName("ioType").item(0).getTextContent();
                    IOSizeRange.IOType type = IOSizeRange.IOType.valueOf(ioType);

                    if (type == IOSizeRange.IOType.SIZE) {
                        // For SIZE type, we have minIOSize and maxIOSize
                        long minIOSize = Long.parseLong(ioSizeRangeElement.getElementsByTagName("minIOSize").item(0).getTextContent());
                        long maxIOSize = Long.parseLong(ioSizeRangeElement.getElementsByTagName("maxIOSize").item(0).getTextContent());
                        ioSizeRanges.add(new IOSizeRange(type, minIOSize, maxIOSize));
                    } else {
                        // For HEAD and TAIL types, we have ioSize
                        long ioSize = Long.parseLong(ioSizeRangeElement.getElementsByTagName("ioSize").item(0).getTextContent());
                        ioSizeRanges.add(new IOSizeRange(type, ioSize));
                    }
                }

                // Extract operations
                List<String> operations = new ArrayList<>();
                NodeList operationNodes = ruleElement.getElementsByTagName("operation");
                for (int j = 0; j < operationNodes.getLength(); j++) {
                    operations.add(operationNodes.item(j).getTextContent());
                }

                // Create OssAccRule and add to rules list
                OssAccRule rule = new OssAccRule(bucketPattern, keyPrefixes, keySuffixes, sizeRanges, operations, ioSizeRanges);
                rules.add(rule);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse OssAccRuleManager XML configuration", e);
        }
    }

    public List<OssAccRule> getRules() {
        return rules;
    }
}