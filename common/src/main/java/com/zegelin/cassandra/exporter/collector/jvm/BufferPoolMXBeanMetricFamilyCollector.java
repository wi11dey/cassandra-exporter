package com.zegelin.cassandra.exporter.collector.jvm;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.zegelin.cassandra.exporter.collector.util.LabeledObjects;
import com.zegelin.cassandra.exporter.collector.util.Sources;
import com.zegelin.jmx.NamedObject;
import com.zegelin.jmx.ObjectNames;
import com.zegelin.cassandra.exporter.MBeanGroupMetricFamilyCollector;
import com.zegelin.prometheus.domain.GaugeMetricFamily;
import com.zegelin.prometheus.domain.Labels;
import com.zegelin.prometheus.domain.MetricFamily;
import com.zegelin.prometheus.domain.NumericMetric;
import com.zegelin.prometheus.domain.source.MBeanQuerySource;
import com.zegelin.prometheus.domain.source.Source;

import javax.management.ObjectName;
import java.lang.management.BufferPoolMXBean;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static com.zegelin.cassandra.exporter.MetricValueConversionFunctions.neg1ToNaN;

public class BufferPoolMXBeanMetricFamilyCollector extends MBeanGroupMetricFamilyCollector {
    private static final ObjectName BUFFER_POOL_MXBEAN_QUERY = ObjectNames.create("java.nio:type=BufferPool,name=*");

    public static final Factory FACTORY = mBean -> {
        if (!BUFFER_POOL_MXBEAN_QUERY.apply(mBean.name))
            return null;

        final NamedObject<BufferPoolMXBean> bufferPoolMXBean = mBean.cast();

        final Labels poolLabels = Labels.of("pool", bufferPoolMXBean.object.getName());

        final Source source = new MBeanQuerySource(BUFFER_POOL_MXBEAN_QUERY, Labels.of(), mBean.interfaceClassName);

        return new BufferPoolMXBeanMetricFamilyCollector(ImmutableMap.of(poolLabels, bufferPoolMXBean), ImmutableSet.of(source));
    };

    private final Map<Labels, NamedObject<BufferPoolMXBean>> labeledBufferPoolMXBeans;
    private final Set<Source> sources;

    private BufferPoolMXBeanMetricFamilyCollector(final Map<Labels, NamedObject<BufferPoolMXBean>> labeledBufferPoolMXBeans, final Set<Source> sources) {
        this.labeledBufferPoolMXBeans = ImmutableMap.copyOf(labeledBufferPoolMXBeans);
        this.sources = ImmutableSet.copyOf(sources);
    }

    @Override
    public MBeanGroupMetricFamilyCollector merge(final MBeanGroupMetricFamilyCollector rawOther) {
        if (!(rawOther instanceof BufferPoolMXBeanMetricFamilyCollector)) {
            throw new IllegalStateException();
        }

        final BufferPoolMXBeanMetricFamilyCollector other = (BufferPoolMXBeanMetricFamilyCollector) rawOther;

        return new BufferPoolMXBeanMetricFamilyCollector(
                LabeledObjects.merge(labeledBufferPoolMXBeans, other.labeledBufferPoolMXBeans),
                Sources.merge(sources, other.sources)
        );
    }

    @Override
    public Stream<MetricFamily> collect() {
        final Stream.Builder<NumericMetric> estimatedBuffersMetrics = Stream.builder();
        final Stream.Builder<NumericMetric> totalCapacityBytesMetrics = Stream.builder();
        final Stream.Builder<NumericMetric> usedBytesMetrics = Stream.builder();

        for (final Map.Entry<Labels, NamedObject<BufferPoolMXBean>> entry : labeledBufferPoolMXBeans.entrySet()) {
            final Labels labels = entry.getKey();
            final NamedObject<BufferPoolMXBean> bufferPoolMXBean = entry.getValue();

            estimatedBuffersMetrics.add(new NumericMetric(labels, bufferPoolMXBean.object.getCount()));
            totalCapacityBytesMetrics.add(new NumericMetric(labels, bufferPoolMXBean.object.getTotalCapacity()));
            usedBytesMetrics.add(new NumericMetric(labels, neg1ToNaN(bufferPoolMXBean.object.getMemoryUsed())));
        }

        return Stream.of(
                new GaugeMetricFamily("cassandra_jvm_nio_buffer_pool_estimated_buffers", "Estimated current number of buffers in the pool.", sources, estimatedBuffersMetrics.build()),
                new GaugeMetricFamily("cassandra_jvm_nio_buffer_pool_estimated_capacity_bytes_total", "Estimated total capacity of the buffers in the pool.", sources, totalCapacityBytesMetrics.build()),
                new GaugeMetricFamily("cassandra_jvm_nio_buffer_pool_estimated_used_bytes", "Estimated memory usage by the JVM for the pool.", sources, usedBytesMetrics.build())
        );
    }
}
