package com.zegelin.prometheus.domain;

import com.google.common.collect.ImmutableList;
import com.zegelin.prometheus.domain.source.Source;

import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SummaryMetricFamily extends MetricFamily<SummaryMetricFamily.Summary> {
    public SummaryMetricFamily(final String name, final String help, Set<Source> sources, final Stream<Summary> metrics) {
        this(name, help, sources, () -> metrics);
    }

    private SummaryMetricFamily(final String name, final String help, Set<Source> sources, final Supplier<Stream<Summary>> metricsStreamSupplier) {
        super(name, help, sources, metricsStreamSupplier);
    }

    @Override
    public <R> R accept(final MetricFamilyVisitor<R> visitor) {
        return visitor.visit(this);
    }

    @Override
    public SummaryMetricFamily cachedCopy() {
        final List<Summary> metrics = metrics().collect(Collectors.toList());

        return new SummaryMetricFamily(name, help, sources, metrics::stream);
    }

    public static class Summary extends Metric {
        public final float sum;
        public final float count;
        public final Iterable<Interval> quantiles;

        public Summary(final Labels labels, final float sum, final float count, final Iterable<Interval> quantiles) {
            super(labels, source);

            this.sum = sum;
            this.count = count;
            this.quantiles = ImmutableList.copyOf(quantiles);
        }
    }
}
