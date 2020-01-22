package com.zegelin.prometheus.domain.source;

public abstract class Source {
    public abstract void visit(final SourceVisitor visitor);
}
