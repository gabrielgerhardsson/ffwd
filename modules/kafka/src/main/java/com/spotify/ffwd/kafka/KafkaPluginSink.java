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
package com.spotify.ffwd.kafka;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.spotify.ffwd.model.Event;
import com.spotify.ffwd.model.Metric;
import com.spotify.ffwd.output.BatchedPluginSink;
import com.spotify.ffwd.serializer.Serializer;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.FutureFailed;
import eu.toolchain.async.FutureResolved;
import eu.toolchain.async.ResolvableFuture;
import eu.toolchain.async.Transform;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

@Slf4j
public class KafkaPluginSink implements BatchedPluginSink {
    @Inject
    private AsyncFramework async;

    @Inject
    private Supplier<Producer<Integer, byte[]>> producerSupplier;

    private Producer<Integer, byte[]> producer;

    @Inject
    private KafkaRouter router;

    @Inject
    private KafkaPartitioner partitioner;

    @Inject
    private Serializer serializer;

    private final int batchSize;

    private final ExecutorService executorService = Executors.newCachedThreadPool(
        new ThreadFactoryBuilder().setNameFormat("ffwd-kafka-async-%d").build());

    public KafkaPluginSink(int batchSize) {
        this.batchSize = batchSize;
    }

    @Override
    public void init() {
        // Initialize Producer here, delayed, instead of in Constructor, to facilitate unit testing
        producer = producerSupplier.get();
    }

    @Override
    public void sendEvent(final Event event) {
        try {
            producer.send(eventConverter.toMessage(event));
        } catch (Exception e) {
            log.info("Failed to send event: {}", e);
        }
    }

    @Override
    public void sendMetric(final Metric metric) {
        try {
            producer.send(metricConverter.toMessage(metric));
        } catch (Exception e) {
            log.info("Failed to send metric: {}", e);
        }
    }

    @Override
    public AsyncFuture<Void> sendEvents(final Collection<Event> events) {
        return send(toBatches(iteratorFor(events, eventConverter)));
    }

    /**
     * Send 1 or more batches of metrics/events
     */
    private AsyncFuture<Void> send(final Iterator<List<ProducerRecord<Integer, byte[]>>> batches) {
        final UUID id = UUID.randomUUID();

        final List<AsyncFuture<Void>> batchFutures = new ArrayList<>();
        final List<Long> times = new ArrayList<>();

        log.info("{}: Start sending of batches", id);

        while (batches.hasNext()) {
            final List<AsyncFuture<Void>> futures = new ArrayList<>();
            final Stopwatch watch = Stopwatch.createStarted();

            final List<ProducerRecord<Integer, byte[]>> batch = batches.next();
            for (final ProducerRecord<Integer, byte[]> m : batch) {
                final ResolvableFuture<Void> future = async.future();

                producer.send(m, (recordMetadata, exception) -> {
                    if (Objects.isNull(exception)) {
                        future.resolve(null);
                    } else {
                        // Wrap to avoid self-suppression when multiple metrics get same exception
                        future.fail(new Exception(id + ": Failed while sending: ", exception));
                    }
                });

                futures.add(future);
            }

            final AsyncFuture<Void> batchFuture =
                async.collectAndDiscard(futures).transform(new Transform<Void, Void>() {
                    @Override
                    public Void transform(final Void aVoid) throws Exception {
                        times.add(watch.elapsed(TimeUnit.MILLISECONDS));
                        return aVoid;
                    }
                });

            batchFutures.add(batchFuture);
        }

        return async.collectAndDiscard(batchFutures).on(new FutureFailed() {
            @Override
            public void failed(final Throwable throwable) throws Exception {
                log.info("{}: Failed sending batches", id);
            }
        }).on(new FutureResolved<Void>() {
            @Override
            public void resolved(final Void aVoid) throws Exception {
                log.info("{}: Done sending batches (timings in ms: {})", id, times);
            }
        });
    }

    @Override
    public AsyncFuture<Void> sendMetrics(final Collection<Metric> metrics) {
        return send(toBatches(iteratorFor(metrics, metricConverter)));
    }

    @Override
    public AsyncFuture<Void> start() {
        return async.resolved(null);
    }

    @Override
    public AsyncFuture<Void> stop() {
        return async.call(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                producer.close();
                return null;
            }
        }, executorService);
    }

    @Override
    public boolean isReady() {
        // TODO: how to check that producer is ready?
        return true;
    }

    /**
     * Convert the given message iterator to an iterator of batches of a specific size.
     * <p>
     * This is an attempt to reduce the required maximum amount of live memory required at a single
     * time.
     *
     * @param iterator Iterator to convert into batches.
     * @return
     */
    private Iterator<List<ProducerRecord<Integer, byte[]>>> toBatches(
        final Iterator<ProducerRecord<Integer, byte[]>> iterator
    ) {
        return Iterators.partition(iterator, batchSize);
    }

    final Converter<Metric> metricConverter = new Converter<Metric>() {
        @Override
        public ProducerRecord<Integer, byte[]> toMessage(final Metric metric) throws Exception {
            final String topic = router.route(metric);
            int partition = partitioner.partition(metric);
            final byte[] payload = serializer.serialize(metric);
            return new ProducerRecord<>(topic, partition, payload);
        }
    };

    final Converter<Event> eventConverter = new Converter<Event>() {
        @Override
        public ProducerRecord<Integer, byte[]> toMessage(final Event event) throws Exception {
            final String topic = router.route(event);
            final int partition = partitioner.partition(event);
            final byte[] payload = serializer.serialize(event);
            return new ProducerRecord<>(topic, partition, payload);
        }
    };

    final <T> Iterator<ProducerRecord<Integer, byte[]>> iteratorFor(
        Iterable<? extends T> iterable, final Converter<T> converter
    ) {
        final Iterator<? extends T> iterator = iterable.iterator();

        return new Iterator<ProducerRecord<Integer, byte[]>>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public ProducerRecord<Integer, byte[]> next() {
                try {
                    return converter.toMessage(iterator.next());
                } catch (final Exception e) {
                    throw new RuntimeException("Failed to produce next element", e);
                }
            }

            @Override
            public void remove() {
            }
        };
    }

    static interface Converter<T> {
        ProducerRecord<Integer, byte[]> toMessage(T object) throws Exception;
    }
}
