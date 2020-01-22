package com.zegelin.prometheus.domain;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;
import com.zegelin.prometheus.domain.source.Source;

import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

public abstract class MetricFamily<T extends Metric> {
    public final String name;
    public final String help;
    public final Set<Source> sources;

    private Supplier<Stream<T>> metricsStreamSupplier;

    public MetricFamily(final String name, final String help, final Set<Source> sources, final Supplier<Stream<T>> metricsStreamSupplier) {
        this.name = name;
        this.help = help;
        this.sources = sources == null ? ImmutableSet.of() : ImmutableSet.copyOf(sources);
        this.metricsStreamSupplier = metricsStreamSupplier;
    }

    public abstract <R> R accept(final MetricFamilyVisitor<R> visitor);

    public abstract MetricFamily<T> cachedCopy();

    public Stream<T> metrics() {
        return metricsStreamSupplier.get();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof MetricFamily)) return false;

        final MetricFamily<?> that = (MetricFamily<?>) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("name", name)
                .add("help", help)
                .add("sources", sources)
                .toString();
    }
}
