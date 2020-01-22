package com.zegelin.jmx;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

import javax.management.ObjectName;
import java.util.function.BiFunction;

/**
 * An arbitrary object (typically an MBean) and its JMX name.
 *
 * @param <T> Object type
 */
public class NamedObject<T> {
    public final ObjectName name;
    public final T object;
    public final String interfaceClassName;

    public NamedObject(final ObjectName name, final T object, final String interfaceClassName) {
        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(object);

        this.name = name;
        this.object = object;
        this.interfaceClassName = interfaceClassName;
    }

    @SuppressWarnings("unchecked")
    public <U> NamedObject<U> cast() {
        return map((n, o) -> (U) o);
    }

    public <U> NamedObject<U> map(final BiFunction<ObjectName, ? super T, ? extends U> mapper) {
        final U mappedObject = mapper.apply(name, object);

        return mappedObject == null ? null : new NamedObject<>(name, mappedObject, interfaceClassName);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("name", name)
                .add("object", object)
                .toString();
    }
}
