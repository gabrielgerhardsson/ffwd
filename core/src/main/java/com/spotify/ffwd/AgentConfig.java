/*
 * Copyright 2013-2017 Spotify AB. All rights reserved.
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
package com.spotify.ffwd;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.spotify.ffwd.input.InputManagerModule;
import com.spotify.ffwd.output.OutputManagerModule;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Data
public class AgentConfig {
    public static final int DEFAULT_ASYNC_THREADS = 4;
    public static final int DEFAULT_SCHEDULER_THREADS = 4;
    public static final int DEFAULT_BOSS_THREADS = 2;
    public static final int DEFAULT_WORKER_THREADS = 4;

    public static final Map<String, String> DEFAULT_TAGS = Maps.newHashMap();
    public static final Set<String> DEFAULT_RIEMANNTAGS = Sets.newHashSet();

    public static final String DEFAULT_QLOG = "./qlog/";

    private final Optional<Debug> debug;
    private final String host;
    private final Map<String, String> tags;
    private final Set<String> riemannTags;
    private final Set<String> skipTagsForKeys;
    private final InputManagerModule input;
    private final OutputManagerModule output;
    private final int schedulerThreads;
    private final int asyncThreads;
    private final int bossThreads;
    private final int workerThreads;
    private final long ttl;
    private final Path qlog;

    @JsonCreator
    public AgentConfig(
        @JsonProperty("debug") Debug debug, @JsonProperty("host") String host,
        @JsonProperty("tags") Map<String, String> tags,
        @JsonProperty("riemannTags") Set<String> riemannTags,
        @JsonProperty("skipTagsForKeys") Set<String> skipTagsForKeys,
        @JsonProperty("input") InputManagerModule input,
        @JsonProperty("output") OutputManagerModule output,
        @JsonProperty("asyncThreads") Integer asyncThreads,
        @JsonProperty("schedulerThreads") Integer schedulerThreads,
        @JsonProperty("bossThreads") Integer bossThreads,
        @JsonProperty("workerThreads") Integer workerThreads, @JsonProperty("ttl") Long ttl,
        @JsonProperty("qlog") String qlog
    ) {
        this.debug = Optional.ofNullable(debug);
        this.host = Optional.ofNullable(host).orElseGet(this::buildDefaultHost);
        this.tags = Optional.ofNullable(tags).orElse(DEFAULT_TAGS);
        this.riemannTags = Optional.ofNullable(riemannTags).orElse(DEFAULT_RIEMANNTAGS);
        this.skipTagsForKeys = Optional.ofNullable(skipTagsForKeys).orElse(Sets.newHashSet());
        this.input = Optional.ofNullable(input).orElseGet(InputManagerModule.supplyDefault());
        this.output = Optional.ofNullable(output).orElseGet(OutputManagerModule.supplyDefault());
        this.asyncThreads = Optional.ofNullable(asyncThreads).orElse(DEFAULT_ASYNC_THREADS);
        this.schedulerThreads =
            Optional.ofNullable(schedulerThreads).orElse(DEFAULT_SCHEDULER_THREADS);
        this.bossThreads = Optional.ofNullable(bossThreads).orElse(DEFAULT_BOSS_THREADS);
        this.workerThreads = Optional.ofNullable(workerThreads).orElse(DEFAULT_WORKER_THREADS);
        this.ttl = Optional.ofNullable(ttl).orElse(0L);
        this.qlog = Paths.get(Optional.ofNullable(qlog).orElse(DEFAULT_QLOG));
    }

    private String buildDefaultHost() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new RuntimeException("unable to get local host", e);
        }
    }

    public AgentConfig withOutput(OutputManagerModule output) {
        return new AgentConfig(debug, host, tags, riemannTags, skipTagsForKeys, input, output,
            asyncThreads, schedulerThreads, bossThreads, workerThreads, ttl, qlog);
    }

    @Data
    public static final class Debug {
        public static final String DEFAULT_HOST = "localhost";
        public static final int DEFAULT_PORT = 19001;

        private final InetSocketAddress localAddress;

        @JsonCreator
        public Debug(@JsonProperty("host") String host, @JsonProperty("port") Integer port) {
            this.localAddress = buildLocalAddress(Optional.ofNullable(host).orElse(DEFAULT_HOST),
                Optional.ofNullable(port).orElse(DEFAULT_PORT));
        }

        private InetSocketAddress buildLocalAddress(String host, Integer port) {
            return new InetSocketAddress(host, port);
        }
    }
}
