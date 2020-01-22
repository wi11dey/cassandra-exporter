package com.zegelin.prometheus.domain;

import com.zegelin.prometheus.domain.source.Source;

import javax.management.QueryExp;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CounterMetricFamily extends MetricFamily<NumericMetric> {
    public CounterMetricFamily(final String name, final String help, Set<Source> sources, final Stream<NumericMetric> metrics) {
        this(name, help, sources, () -> metrics);
    }

    private CounterMetricFamily(final String name, final String help, Set<Source> sources, final Supplier<Stream<NumericMetric>> metricsStreamSupplier) {
        super(name, help, sources, metricsStreamSupplier);
    }

    @Override
    public <R> R accept(final MetricFamilyVisitor<R> visitor) {
        return visitor.visit(this);
    }

    @Override
    public CounterMetricFamily cachedCopy() {
        final List<NumericMetric> metrics = metrics().collect(Collectors.toList());

        return new CounterMetricFamily(name, help, sources, metrics::stream);
    }
}
