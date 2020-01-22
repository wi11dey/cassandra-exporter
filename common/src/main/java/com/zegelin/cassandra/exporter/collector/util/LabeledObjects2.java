package com.zegelin.cassandra.exporter.collector.util;

import com.google.common.collect.ForwardingMap;
import com.zegelin.jmx.NamedObject;
import com.zegelin.prometheus.domain.Labels;

import java.util.Map;

public class LabeledObjects2<T> extends ForwardingMap<Labels, NamedObject<T>> {
    @Override
    protected Map<Labels, NamedObject<T>> delegate() {
        return null;
    }
}
