package com.zegelin.cassandra.exporter.collector.util;

import com.zegelin.prometheus.domain.Labels;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

public final class LabeledObjects {
    private LabeledObjects() {}

    public static <T> Map<Labels, T> merge(final Map<Labels, T> a, final Map<Labels, T> b) {
        return merge(a, b, (o1, o2) -> {
            throw new IllegalStateException(String.format("Object %s and %s cannot be merged, yet their labels are the same.", o1, o2));
        });
    }

    public static <T> Map<Labels, T> merge(final Map<Labels, T> a, final Map<Labels, T> b, final BiFunction<T, T, T> valueMergeFunction) {
        final Map<Labels, T> labeledObjects = new HashMap<>(a);
        for (final Map.Entry<Labels, T> entry: b.entrySet()) {
            labeledObjects.merge(entry.getKey(), entry.getValue(), valueMergeFunction);
        }

        return labeledObjects;
    }
}
