package com.test.serde;

import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.util.Set;

/**
 * See {@link org.apache.flink.api.common.typeutils.base.ListSerializerSnapshot ListSerializerSnapshot}.
 */
public class SetSerializerSnapshot<T> extends CompositeTypeSerializerSnapshot<Set<T>, SetSerializer<T>> {
    private static final int CURRENT_VERSION = 1;

    public SetSerializerSnapshot() {
        super(SetSerializer.class);
    }

    public SetSerializerSnapshot(SetSerializer<T> serializerInstance) {
        super(serializerInstance);
    }

    @Override
    protected int getCurrentOuterSnapshotVersion() {
        return CURRENT_VERSION;
    }

    @Override
    protected TypeSerializer<?>[] getNestedSerializers(SetSerializer<T> outerSerializer) {
        return new TypeSerializer<?>[]{outerSerializer.getElementSerializer()};
    }

    @Override
    protected SetSerializer<T> createOuterSerializerWithNestedSerializers(TypeSerializer<?>[] nestedSerializers) {
        @SuppressWarnings("unchecked")
        TypeSerializer<T> elementSerializer = (TypeSerializer<T>) nestedSerializers[0];
        return new SetSerializer<>(elementSerializer);
    }
}
