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

import com.aliyun.sdk.service.oss2.models.*;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.store.LogExactlyOnce;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;

import static org.apache.hadoop.fs.aliyun.oss.v2.Constants.*;

/**
 * Object change detection policy.
 * Determines which attribute is used to detect change and what to do when
 * change is detected.
 */


public abstract class ChangeDetectionPolicy {

  private static final Logger LOG =
      LoggerFactory.getLogger(ChangeDetectionPolicy.class);

  public static final String CHANGE_DETECTED = "change detected on client";
  public static final String CHANGE_DETECT_SOURCE_ETAG = "etag";


  @VisibleForTesting
  private final Mode mode;
  private final boolean requireVersion;

  private final LogExactlyOnce logNoVersionSupport = new LogExactlyOnce(LOG);

  public enum Source {
    ETag(CHANGE_DETECT_SOURCE_ETAG),
    None("none");

    private final String source;

    Source(String source) {
      this.source = source;
    }

    private static Source fromString(String trimmed) {
      for (Source value : values()) {
        if (value.source.equals(trimmed)) {
          return value;
        }
      }
      LOG.warn("Unrecognized " + CHANGE_DETECT_SOURCE + " value: \"{}\"",
          trimmed);
      return fromString(CHANGE_DETECT_SOURCE_ETAG);
    }

  }

  public enum Mode {
    Client(CHANGE_DETECT_MODE_CLIENT),
    Server(CHANGE_DETECT_MODE_SERVER),
    Warn(CHANGE_DETECT_MODE_WARN),
    None(CHANGE_DETECT_MODE_NONE);

    private final String mode;

    Mode(String mode) {
      this.mode = mode;
    }

    private static Mode fromString(String trimmed) {
      for (Mode value : values()) {
        if (value.mode.equals(trimmed)) {
          return value;
        }
      }
      LOG.warn("Unrecognized " + CHANGE_DETECT_MODE + " value: \"{}\"",
          trimmed);
      return fromString(CHANGE_DETECT_MODE_DEFAULT);
    }

    static Mode fromConfiguration(Configuration configuration) {
      String trimmed = configuration.get(CHANGE_DETECT_MODE,
          CHANGE_DETECT_MODE_DEFAULT)
          .trim()
          .toLowerCase(Locale.ENGLISH);
      return fromString(trimmed);
    }
  }

  protected ChangeDetectionPolicy(Mode mode, boolean requireVersion) {
    this.mode = mode;
    this.requireVersion = requireVersion;
  }

  public Mode getMode() {
    return mode;
  }

  public abstract Source getSource();

  public boolean isRequireVersion() {
    return requireVersion;
  }

  public LogExactlyOnce getLogNoVersionSupport() {
    return logNoVersionSupport;
  }

  public static ChangeDetectionPolicy getPolicy(Configuration configuration) {
    Mode mode = Mode.fromConfiguration(configuration);
    Source source = Source.fromString(CHANGE_DETECT_SOURCE_ETAG);
    boolean requireVersion = false;
    return createPolicy(mode, source, requireVersion);
  }

  @VisibleForTesting
  public static ChangeDetectionPolicy createPolicy(final Mode mode,
      final Source source, final boolean requireVersion) {
    switch (source) {
    case ETag:
      return new ETagChangeDetectionPolicy(mode, requireVersion);
    default:
      return new NoChangeDetection();
    }
  }

  @Override
  public String toString() {
    return "Policy " + getSource() + "/" + getMode();
  }


  public abstract void applyRevisionConstraint(CopyObjectRequest.Builder requestBuilder,
      String revisionId);

  public abstract void applyRevisionConstraint(HeadObjectRequest.Builder requestBuilder,
      String revisionId);

  public ImmutablePair<Boolean, RemoteFileChangedException> onChangeDetected(
      String revisionId,
      String newRevisionId,
      String uri,
      long position,
      String operation,
      long timesAlreadyDetected) {
    String positionText = position >= 0 ? (" at " + position) : "";
    switch (mode) {
    case None:
      return new ImmutablePair<>(false, null);
    case Warn:
      if (timesAlreadyDetected == 0) {
        LOG.warn(
            String.format(
                "%s change detected on %s %s%s. Expected %s got %s",
                getSource(), operation, uri, positionText, revisionId,
                newRevisionId));
        return new ImmutablePair<>(true, null);
      }
      return new ImmutablePair<>(false, null);
    case Client:
    case Server:
    default:
      return new ImmutablePair<>(true,
          new RemoteFileChangedException(uri,
              operation,
              String.format("%s "
                      + CHANGE_DETECTED
                      + " during %s%s."
                    + " Expected %s got %s",
              getSource(), operation, positionText, revisionId, newRevisionId)));
    }
  }


  static class ETagChangeDetectionPolicy extends ChangeDetectionPolicy {

    ETagChangeDetectionPolicy(Mode mode, boolean requireVersion) {
      super(mode, requireVersion);
    }



    public void applyRevisionConstraint(GetObjectRequest.Builder builder,
        String revisionId) {
      if (revisionId != null) {
        LOG.debug("Restricting get request to etag {}", revisionId);
        builder.ifMatch(revisionId);
      } else {
        LOG.debug("No etag revision ID to use as a constraint");
      }
    }

    @Override
    public void applyRevisionConstraint(CopyObjectRequest.Builder requestBuilder,
        String revisionId) {
      if (revisionId != null) {
        LOG.debug("Restricting copy request to etag {}", revisionId);
        requestBuilder.copySourceIfMatch(revisionId);
      } else {
        LOG.debug("No etag revision ID to use as a constraint");
      }
    }

    @Override
    public void applyRevisionConstraint(HeadObjectRequest.Builder requestBuilder,
        String revisionId) {
      LOG.debug("Unable to restrict HEAD request to etag; will check later");
    }

    @Override
    public Source getSource() {
      return Source.ETag;
    }

    @Override
    public String toString() {
      return "ETagChangeDetectionPolicy mode=" + getMode();
    }

  }


  static class NoChangeDetection extends ChangeDetectionPolicy {

    NoChangeDetection() {
      super(Mode.None, false);
    }

    @Override
    public Source getSource() {
      return Source.None;
    }



    public void applyRevisionConstraint(final GetObjectRequest.Builder builder,
        final String revisionId) {

    }

    @Override
    public void applyRevisionConstraint(CopyObjectRequest.Builder requestBuilder,
        String revisionId) {

    }

    @Override
    public void applyRevisionConstraint(HeadObjectRequest.Builder requestBuilder,
                                        String revisionId) {

    }

    @Override
    public String toString() {
      return "NoChangeDetection";
    }

  }
}
