package com.zegelin.function;

@FunctionalInterface
public interface FloatFloatFunction {
    float apply(float f);

    FloatFloatFunction IDENTITY = (f) -> f;
}
