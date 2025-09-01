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

package org.apache.hadoop.fs.aliyun.oss.v2;

/**
 * Performance level enumeration for OSS operations.
 * This enum defines different levels of performance metrics collection:
 * - NONE: No performance metrics collection
 * - STATISTIC: Basic statistics collection
 * - DETAIL: Detailed performance metrics collection
 */
public enum PerformanceLevel {
  NONE(0, "none"),
  STATISTIC(1, "statistic"),
  DETAIL(2, "detail");

  private final int value;
  private final String name;

  PerformanceLevel(int value, String name) {
    this.value = value;
    this.name = name;
  }

  /**
   * Get the integer value of this performance level.
   * @return the integer value
   */
  public int getValue() {
    return value;
  }

  /**
   * Get the name of this performance level.
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * Check if this performance level is greater than or equal to another.
   * @param other the other performance level to compare with
   * @return true if this level is greater than or equal to the other
   */
  public boolean isAtLeast(PerformanceLevel other) {
    return this.compareTo(other) >= 0;
  }

  /**
   * Get the PerformanceLevel by its integer value.
   * @param value the integer value
   * @return the corresponding PerformanceLevel
   * @throws IllegalArgumentException if no matching level is found
   */
  public static PerformanceLevel fromValue(int value) {
    for (PerformanceLevel level : values()) {
      if (level.value == value) {
        return level;
      }
    }
    throw new IllegalArgumentException("No PerformanceLevel with value: " + value);
  }

  /**
   * Get the PerformanceLevel by its name.
   * @param name the name
   * @return the corresponding PerformanceLevel
   * @throws IllegalArgumentException if no matching level is found
   */
  public static PerformanceLevel fromName(String name) {
    for (PerformanceLevel level : values()) {
      if (level.name.equalsIgnoreCase(name)) {
        return level;
      }
    }
    throw new IllegalArgumentException("No PerformanceLevel with name: " + name);
  }
}