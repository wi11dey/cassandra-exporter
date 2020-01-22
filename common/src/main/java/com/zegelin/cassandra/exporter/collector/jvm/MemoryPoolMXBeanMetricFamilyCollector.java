package com.zegelin.cassandra.exporter.collector.jvm;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.zegelin.cassandra.exporter.collector.util.LabeledObjects;
import com.zegelin.cassandra.exporter.collector.util.Sources;
import com.zegelin.jmx.ObjectNames;
import com.zegelin.cassandra.exporter.MBeanGroupMetricFamilyCollector;
import com.zegelin.prometheus.domain.GaugeMetricFamily;
import com.zegelin.prometheus.domain.Labels;
import com.zegelin.prometheus.domain.MetricFamily;
import com.zegelin.prometheus.domain.NumericMetric;
import com.zegelin.prometheus.domain.source.MBeanQuerySource;
import com.zegelin.prometheus.domain.source.Source;

import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static com.zegelin.cassandra.exporter.MetricValueConversionFunctions.neg1ToNaN;

public class MemoryPoolMXBeanMetricFamilyCollector extends MBeanGroupMetricFamilyCollector {
    private static final ObjectName MEMORY_POOL_MXBEAN_QUERY = ObjectNames.create(ManagementFactory.MEMORY_POOL_MXBEAN_DOMAIN_TYPE + ",*");

    public static final Factory FACTORY = mBean -> {
        if (!MEMORY_POOL_MXBEAN_QUERY.apply(mBean.name))
            return null;

        final MemoryPoolMXBean memoryPoolMXBean = (MemoryPoolMXBean) mBean.object;

        final Labels poolLabels = new Labels(ImmutableMap.of(
                "pool", memoryPoolMXBean.getName(),
                "type", memoryPoolMXBean.getType().name()
        ));

        final MBeanQuerySource source = new MBeanQuerySource(MEMORY_POOL_MXBEAN_QUERY, Labels.of(), mBean.interfaceClassName);

        return new MemoryPoolMXBeanMetricFamilyCollector(ImmutableMap.of(poolLabels, memoryPoolMXBean), ImmutableSet.of(source));
    };

    private final Map<Labels, MemoryPoolMXBean> labeledMemoryPoolMXBeans;
    private final Set<Source> sources;

    private MemoryPoolMXBeanMetricFamilyCollector(final Map<Labels, MemoryPoolMXBean> labeledMemoryPoolMXBeans, final Set<Source> sources) {
        this.labeledMemoryPoolMXBeans = ImmutableMap.copyOf(labeledMemoryPoolMXBeans);
        this.sources = ImmutableSet.copyOf(sources);
    }


    @Override
    public MBeanGroupMetricFamilyCollector merge(final MBeanGroupMetricFamilyCollector rawOther) {
        if (!(rawOther instanceof MemoryPoolMXBeanMetricFamilyCollector)) {
            throw new IllegalStateException();
        }

        final MemoryPoolMXBeanMetricFamilyCollector other = (MemoryPoolMXBeanMetricFamilyCollector) rawOther;

        return new MemoryPoolMXBeanMetricFamilyCollector(
                LabeledObjects.merge(labeledMemoryPoolMXBeans, other.labeledMemoryPoolMXBeans),
                Sources.merge(sources, other.sources)
        );
    }

    @Override
    public MBeanGroupMetricFamilyCollector removeMBean(final ObjectName mBeanName) {
        return null;
    }

    @Override
    public Stream<MetricFamily> collect() {
        final Stream.Builder<NumericMetric> initialBytesMetrics = Stream.builder();
        final Stream.Builder<NumericMetric> usedBytesMetrics = Stream.builder();
        final Stream.Builder<NumericMetric> committedBytesMetrics = Stream.builder();
        final Stream.Builder<NumericMetric> maximumBytesMetrics = Stream.builder();

        for (final Map.Entry<Labels, MemoryPoolMXBean> entry : labeledMemoryPoolMXBeans.entrySet()) {
            final Labels labels = entry.getKey();
            final MemoryPoolMXBean memoryPoolMXBean = entry.getValue();

            final MemoryUsage usage = memoryPoolMXBean.getUsage();

            initialBytesMetrics.add(new NumericMetric(labels, neg1ToNaN(usage.getInit())));
            usedBytesMetrics.add(new NumericMetric(labels, usage.getUsed()));
            committedBytesMetrics.add(new NumericMetric(labels, usage.getCommitted()));
            maximumBytesMetrics.add(new NumericMetric(labels, neg1ToNaN(usage.getMax())));
        }

        return Stream.of(
                new GaugeMetricFamily("cassandra_jvm_memory_pool_initial_bytes", "Initial size of the memory pool.", sources, initialBytesMetrics.build()),
                new GaugeMetricFamily("cassandra_jvm_memory_pool_used_bytes", "Current memory pool usage.", sources, usedBytesMetrics.build()),
                new GaugeMetricFamily("cassandra_jvm_memory_pool_committed_bytes", null, sources, committedBytesMetrics.build()),
                new GaugeMetricFamily("cassandra_jvm_memory_pool_maximum_bytes", "Maximum size of the memory pool.", sources, maximumBytesMetrics.build())
        );
    }
}
