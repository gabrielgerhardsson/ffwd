/*
 * Copyright 2018 Spotify AB. All rights reserved.
 *
 * The contents of this file are licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.spotify.ffwd.module;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Optional;
import lombok.Data;

@Data
public class Batching {
    protected final Optional<Long> flushInterval;
    protected final Optional<Long> batchSizeLimit;
    protected final Optional<Long> maxPendingFlushes;

    @JsonCreator
    public Batching(
        @JsonProperty("flushInterval") Optional<Long> flushInterval,
        @JsonProperty("batchSizeLimit") Optional<Long> batchSizeLimit,
        @JsonProperty("maxPendingFlushes") Optional<Long> maxPendingFlushes
    ) {
        this.flushInterval = flushInterval;
        this.batchSizeLimit = batchSizeLimit;
        this.maxPendingFlushes = maxPendingFlushes;
    }

    public static Batching empty() {
        return from(Optional.empty(), Optional.empty(), Optional.empty());
    }

    public static Batching from(
        final Optional<Long> flushInterval, final Optional<Batching> batching
    ) {
        return from(flushInterval, batching, Optional.empty());
    }

    public static Batching from(
        final Optional<Long> flushInterval, final Optional<Batching> batching,
        final Optional<Long> defaultFlushInterval
    ) {
        if (flushInterval.isPresent() && batching.isPresent()) {
            throw new RuntimeException(
                "Can't have both 'batching' and 'flushInterval' on the same level in the " +
                    "configuration. Maybe move 'flushInterval' into 'batching'?");
        }
        if (batching.isPresent()) {
            return batching.get();
        }
        if (flushInterval.isPresent()) {
            return new Batching(flushInterval, Optional.empty(), Optional.empty());
        }
        return new Batching(defaultFlushInterval, Optional.empty(), Optional.empty());
    }
}
