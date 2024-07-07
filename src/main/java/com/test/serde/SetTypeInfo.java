package com.test.serde;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.util.Set;

/**
 * See {@link org.apache.flink.api.java.typeutils.ListTypeInfo ListTypeInfo}.
 *
 * @param <T> The type of the elements contained in the set.
 */
public class SetTypeInfo<T> extends TypeInformation<Set<T>> {
    private static final long serialVersionUID = 1L;

    private final TypeInformation<T> elementTypeInfo;

    public SetTypeInfo(TypeInformation<T> elementTypeInfo) {
        this.elementTypeInfo = elementTypeInfo;
    }

    @Override
    public boolean isBasicType() {
        return false;
    }

    @Override
    public boolean isTupleType() {
        return false;
    }

    @Override
    public int getArity() {
        return 0;
    }

    @Override
    public int getTotalFields() {
        return 1;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Class<Set<T>> getTypeClass() {
        return (Class<Set<T>>) (Class<?>) Set.class;
    }

    @Override
    public boolean isKeyType() {
        return false;
    }

    @Override
    public TypeSerializer<Set<T>> createSerializer(ExecutionConfig config) {
        TypeSerializer<T> elementTypeSerializer = elementTypeInfo.createSerializer(config);
        return new SetSerializer<>(elementTypeSerializer);
    }

    @Override
    public String toString() {
        return "Set<" + elementTypeInfo + '>';
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (obj instanceof SetTypeInfo) {
            final SetTypeInfo<?> other = (SetTypeInfo<?>) obj;
            return other.canEqual(this) && elementTypeInfo.equals(other.elementTypeInfo);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return 31 * elementTypeInfo.hashCode() + 1;
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj != null && obj.getClass() == getClass();
    }
}
