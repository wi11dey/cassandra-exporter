package com.zegelin.prometheus.domain.source;

import com.zegelin.prometheus.domain.Labels;

import javax.management.QueryExp;
import java.util.Objects;

public class MBeanQuerySource extends Source {
    public final QueryExp mBeanQuery;
    public final Labels staticLabels;
    public final String mBeanClassName;

    public MBeanQuerySource(final QueryExp mBeanQuery, final Labels staticLabels, final String mBeanClassName) {
        this.mBeanQuery = mBeanQuery;
        this.staticLabels = staticLabels;
        this.mBeanClassName = mBeanClassName;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final MBeanQuerySource that = (MBeanQuerySource) o;

        return mBeanQuery.equals(that.mBeanQuery) &&
                staticLabels.equals(that.staticLabels) &&
                mBeanClassName.equals(that.mBeanClassName);
    }

    public MBeanAttributeSource attribute(final String attributeName) {
        return new MBeanAttributeSource(mBeanQuery, staticLabels, mBeanClassName, attributeName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mBeanQuery, staticLabels, mBeanClassName);
    }

    @Override
    public void visit(final SourceVisitor visitor) {
        visitor.visit(this);
    }
}
