package com.zegelin.cassandra.exporter.collector.util;

import com.zegelin.prometheus.domain.source.Source;

import java.util.HashSet;
import java.util.Set;

public final class Sources {
    private Sources() {}

    public static Set<Source> merge(final Set<Source> a, final Set<Source> b) {
        final Set<Source> sources = new HashSet<>(a);
        sources.addAll(b);

        return sources;
    }
}
