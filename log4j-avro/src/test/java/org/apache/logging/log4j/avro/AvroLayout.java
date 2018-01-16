/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache license, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the license for the specific language governing permissions and
 * limitations under the license.
 */
package org.apache.logging.log4j.avro;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.ThreadContext;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.Node;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderFactory;
import org.apache.logging.log4j.core.layout.AbstractLayout;
import org.apache.logging.log4j.core.layout.ByteBufferDestination;
import org.apache.logging.log4j.util.ReadOnlyStringMap;
import org.apache.logging.log4j.util.TriConsumer;

/**
 * Lays out events in Apache Avro binary format.
 *
 * @see <a href="https://avro.apache.org/docs/current/spec.html">Avro specification</a>
 */
@Plugin(name = "AvroLayout", category = Node.CATEGORY, elementType = Layout.ELEMENT_TYPE, printObject = true)
public final class AvroLayout extends AbstractLayout<org.apache.logging.log4j.avro.LogEvent> {

    private final boolean locationInfo;
    private final boolean includeStacktrace;

    public static class Builder<B extends Builder<B>> extends AbstractLayout.Builder<B>
        implements org.apache.logging.log4j.core.util.Builder<AvroLayout> {

        @PluginBuilderAttribute
        private boolean locationInfo;

        @PluginBuilderAttribute
        private boolean includeStacktrace = true;

        @Override
        public AvroLayout build() {
            return new AvroLayout(getConfiguration(), includeStacktrace, locationInfo);
        }

        public boolean isLocationInfo() {
            return locationInfo;
        }

        public boolean isIncludeStacktrace() {
            return includeStacktrace;
        }

        /**
         * Whether to include stacktrace of logged Throwables (optional, default to true).
         *
         * If set to false, the Throwable will be omitted.
         *
         * @return this builder
         */
        public B setLocationInfo(final boolean locationInfo) {
            this.locationInfo = locationInfo;
            return asBuilder();
        }

        /**
         * Whether to include the location information (optional, defaults to false).
         *
         * Generating location information is an expensive operation and may impact performance.
         * Use with caution.
         *
         * @return this builder
         */
        public B setIncludeStacktrace(final boolean includeStacktrace) {
            this.includeStacktrace = includeStacktrace;
            return asBuilder();
        }
    }

    private AvroLayout(final Configuration config,
                       final boolean locationInfo,
                       final boolean includeStacktrace) {
        super(config, null, null);
        this.locationInfo = locationInfo;
        this.includeStacktrace = includeStacktrace;
    }

    @PluginBuilderFactory
    public static <B extends Builder<B>> B newBuilder() {
        return new Builder<B>().asBuilder();
    }

    @Override
    public Map<String, String> getContentFormat() {
        return Collections.emptyMap();
    }

    @Override
    public String getContentType() {
        return "application/octet-stream";
    }

    @Override
    public byte[] toByteArray(final LogEvent event) {
        ByteBuffer byteBuffer = toByteBuffer(event);
        if (byteBuffer.hasArray()) {
            return byteBuffer.array();
        } else {
            byteBuffer.flip();
            byte[] array = new byte[byteBuffer.remaining()];
            byteBuffer.get(array);
            return array;
        }
    }

    @Override
    public void encode(final LogEvent event, final ByteBufferDestination destination) {
        destination.writeBytes(toByteBuffer(event));
    }

    private ByteBuffer toByteBuffer(final LogEvent event) {
        org.apache.logging.log4j.avro.LogEvent datum = toSerializable(event);

        ByteBuffer byteBuffer;
        try {
            byteBuffer = datum.toByteBuffer();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return byteBuffer;
    }

    @Override
    public org.apache.logging.log4j.avro.LogEvent toSerializable(final LogEvent event) {
        org.apache.logging.log4j.avro.LogEvent datum =
            new org.apache.logging.log4j.avro.LogEvent();

        ReadOnlyStringMap contextData = event.getContextData();
        if (!contextData.isEmpty()) {
            Map<CharSequence, CharSequence> contextMap = new HashMap<>(contextData.size());
            contextData.forEach(PUT_ALL, contextMap);
            datum.setContextMap(contextMap);
        }

        ThreadContext.ContextStack contextStack = event.getContextStack();
        if (!contextStack.isEmpty()) {
            List<CharSequence> list = new ArrayList<>(contextStack.getDepth());
            list.addAll(contextStack);
            datum.setContextStack(list);
        }

        datum.setLoggerFqcn(event.getLoggerFqcn());
        datum.setLevel(event.getLevel().name());
        datum.setLoggerName(event.getLoggerName());
        // TODO marker
        datum.setMessage(event.getMessage().getFormattedMessage());
        datum.setTimeMillis(event.getTimeMillis());
        if (locationInfo) {
            // TODO source
        }
        datum.setThread(event.getThreadName());
        datum.setThreadId(event.getThreadId());
        datum.setThreadPriority(event.getThreadPriority());
        if (includeStacktrace) {
            // TODO thrown
        }
        datum.setEndOfBatch(event.isEndOfBatch());
        datum.setNanoTime(event.getNanoTime());
        return datum;
    }

    private static TriConsumer<String, String, Map<CharSequence, CharSequence>> PUT_ALL =
        new TriConsumer<String, String, Map<CharSequence, CharSequence>>() {
            @Override
            public void accept(final String key, final String value, final Map<CharSequence, CharSequence> stringStringMap) {
                stringStringMap.put(key, value);
            }
        };

}
