package com.zegelin.prometheus.domain;

import com.google.common.collect.ImmutableList;
import com.zegelin.prometheus.domain.source.Source;

import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HistogramMetricFamily extends MetricFamily<HistogramMetricFamily.Histogram> {
    public HistogramMetricFamily(final String name, final String help, Set<Source> sources, final Stream<Histogram> metrics) {
        this(name, help, sources, () -> metrics);
    }

    private HistogramMetricFamily(final String name, final String help, Set<Source> sources, final Supplier<Stream<Histogram>> metricsStreamSupplier) {
        super(name, help, sources, metricsStreamSupplier);
    }

    @Override
    public <R> R accept(final MetricFamilyVisitor<R> visitor) {
        return visitor.visit(this);
    }

    @Override
    public HistogramMetricFamily cachedCopy() {
        final List<Histogram> metrics = metrics().collect(Collectors.toList());

        return new HistogramMetricFamily(name, help, sources, metrics::stream);
    }

    public static class Histogram extends Metric {
        public final float sum;
        public final float count;
        public final Iterable<Interval> buckets;

        public Histogram(final Labels labels, final float sum, final float count, final Iterable<Interval> buckets) {
            super(labels, source);

            this.sum = sum;
            this.count = count;
            this.buckets = ImmutableList.copyOf(buckets);
        }
    }
}
