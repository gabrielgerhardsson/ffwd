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
public class Flushing {
    protected final Optional<Long> flushInterval;

    @JsonCreator
    public Flushing(@JsonProperty("flushing") Optional<Long> flushInterval) {
        this.flushInterval = flushInterval;
    }

    public static Flushing from(
        final Optional<Long> flushInterval, final Optional<Flushing> flushing) {
        return from(flushInterval, flushing, Optional.empty());
    }

    public static Flushing from(
        final Optional<Long> flushInterval, final Optional<Flushing> flushing,
        final Optional<Long> defaultFlushInterval
    ) {
        if (flushInterval.isPresent() && flushing.isPresent()) {
            throw new RuntimeException(
                "Can't have both 'flushing' and 'flushInterval' on the same level in the " +
                    "configuration. Maybe move 'flushInterval' into 'flushing'?");
        }
        if (flushing.isPresent()) {
            return flushing.get();
        }
        if (flushInterval.isPresent()) {
            return new Flushing(flushInterval);
        }
        return new Flushing(defaultFlushInterval);
    }

    public static Flushing empty() {
        return from(Optional.empty(), Optional.empty(), Optional.empty());
    }
}
