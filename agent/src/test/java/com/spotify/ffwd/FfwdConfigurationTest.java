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

import com.google.inject.Injector;
import eu.toolchain.async.AsyncFramework;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.function.Supplier;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class FfwdConfigurationTest {

    private AsyncFramework async;

    @Before
    public void setup() {
    }

    @Test
    public void testConfAllPluginsEnabled() throws IOException {
        final InputStream configStream = stream("ffwd-all-plugins.yaml").get();
        final FastForwardAgent agent =
            FastForwardAgent.setup(Optional.empty(), Optional.of(configStream));
        final Injector primaryInjector = agent.getCore().getPrimaryInjector();
    }

    private Supplier<InputStream> stream(String name) {
        return () -> getClass().getClassLoader().getResourceAsStream(name);
    }
}
