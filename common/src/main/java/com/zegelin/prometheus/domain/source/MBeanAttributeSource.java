package com.zegelin.prometheus.domain.source;

import com.zegelin.prometheus.domain.Labels;

import javax.management.QueryExp;

public class MBeanAttributeSource extends MBeanQuerySource {
    public final String attributeName;

    public MBeanAttributeSource(final QueryExp mBeanQuery, final Labels staticLabels, final String mBeanClassName, final String attributeName) {
        super(mBeanQuery, staticLabels, mBeanClassName);

        this.attributeName = attributeName;
    }

    @Override
    public void visit(final SourceVisitor visitor) {
        visitor.visit(this);
    }
}
