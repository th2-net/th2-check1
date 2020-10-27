/*
 *  Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.exactpro.th2.check1.configuration;

import com.exactpro.th2.configuration.MicroserviceConfiguration;

import java.io.IOException;
import java.io.InputStream;

import static java.lang.System.getenv;
import static org.apache.commons.lang3.math.NumberUtils.toInt;

public class Configuration extends MicroserviceConfiguration {
    public static final String MESSAGE_CACHE_SIZE = "MESSAGE_CACHE_SIZE";
    public static final int DEFAULT_MESSAGE_CACHE_SIZE = 1000;
    private int messageCacheSize = getEnvMessageCacheSize();

    public int getMessageCacheSize() {
        return messageCacheSize;
    }

    public void setMessageCacheSize(int messageCacheSize) {
        this.messageCacheSize = messageCacheSize;
    }

    private static int getEnvMessageCacheSize() {
        return toInt(getenv(MESSAGE_CACHE_SIZE), DEFAULT_MESSAGE_CACHE_SIZE);
    }

    public static Configuration load(InputStream inputStream) throws IOException {
        return YAML_READER.readValue(inputStream, Configuration.class);
    }

    @Override
    public String toString() {
        return "Configuration{" +
                "messageCacheSize=" + messageCacheSize +
                ", " + super.toString() +
                '}';
    }
}
