package com.zegelin.prometheus.domain.source;

public interface SourceVisitor {
    void visit(final MBeanQuerySource mBeanQuerySource);

    void visit(final MBeanAttributeSource mBeanQuerySource);
}
