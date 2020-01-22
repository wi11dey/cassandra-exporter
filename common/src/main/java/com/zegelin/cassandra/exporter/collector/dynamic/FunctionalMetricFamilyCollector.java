package com.zegelin.cassandra.exporter.collector.dynamic;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.zegelin.cassandra.exporter.collector.util.LabeledObjects;
import com.zegelin.cassandra.exporter.collector.util.Sources;
import com.zegelin.jmx.NamedObject;
import com.zegelin.cassandra.exporter.MBeanGroupMetricFamilyCollector;
import com.zegelin.prometheus.domain.Labels;
import com.zegelin.prometheus.domain.MetricFamily;
import com.zegelin.prometheus.domain.source.Source;

import javax.management.ObjectName;
import javax.management.Query;
import javax.management.QueryExp;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

public class FunctionalMetricFamilyCollector<T> extends MBeanGroupMetricFamilyCollector {
    private final String name;
    private final Set<Source> sources;
    private final String help;

    public interface LabeledObjectGroup<T> {
        String name();
        String help();
        Set<Source> sources();
        Map<Labels, T> labeledObjects();
    }

    public interface CollectorFunction<T> extends Function<LabeledObjectGroup<T>, Stream<MetricFamily>> {}


    private final CollectorFunction<T> collectorFunction;

    private final Map<Labels, NamedObject<T>> labeledObjects;

    private final LabeledObjectGroup<T> objectGroup = new LabeledObjectGroup<T>() {
        @Override
        public String name() {
            return FunctionalMetricFamilyCollector.this.name;
        }

        @Override
        public String help() {
            return FunctionalMetricFamilyCollector.this.help;
        }

        @Override
        public Set<Source> sources() {
            return FunctionalMetricFamilyCollector.this.sources;
        }

        @Override
        public Map<Labels, T> labeledObjects() {
            return Maps.transformValues(FunctionalMetricFamilyCollector.this.labeledObjects, o -> o.object);
        }
    };

    public FunctionalMetricFamilyCollector(final String name, final Set<Source> sources, final String help,
                                           final Map<Labels, NamedObject<T>> labeledObjects,
                                           final CollectorFunction<T> collectorFunction) {
        this.name = name;
        this.sources = ImmutableSet.copyOf(sources);
        this.help = help;
        this.labeledObjects = ImmutableMap.copyOf(labeledObjects);
        this.collectorFunction = collectorFunction;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public MBeanGroupMetricFamilyCollector merge(final MBeanGroupMetricFamilyCollector rawOther) {
        if (!(rawOther instanceof FunctionalMetricFamilyCollector)) {
            throw new IllegalStateException();
        }

        final FunctionalMetricFamilyCollector<T> other = (FunctionalMetricFamilyCollector<T>) rawOther;

        return new FunctionalMetricFamilyCollector<>(name,
                Sources.merge(sources, other.sources), help,
                LabeledObjects.merge(labeledObjects, other.labeledObjects), collectorFunction);
    }

    @Override
    public MBeanGroupMetricFamilyCollector removeMBean(final ObjectName objectName) {
        @SuppressWarnings("ConstantConditions") // ImmutableMap values cannot be null
        final Map<Labels, NamedObject<T>> metrics = ImmutableMap.copyOf(Maps.filterValues(this.labeledObjects, m -> !m.name.equals(objectName)));

        if (metrics.isEmpty())
            return null;

        return new FunctionalMetricFamilyCollector<>(name, sources, help, metrics, collectorFunction);
    }

    @Override
    public Stream<MetricFamily> collect() {
        return collectorFunction.apply(objectGroup);
    }
}
