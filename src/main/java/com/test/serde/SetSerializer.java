package com.test.serde;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/**
 * See {@link ListSerializer}.
 *
 * @param <T> The type of the elements contained in the set.
 */
public class SetSerializer<T> extends TypeSerializer<Set<T>> {
    private static final long serialVersionUID = 1L;

    private final TypeSerializer<T> elementSerializer;

    public SetSerializer(TypeSerializer<T> elementSerializer) {
        this.elementSerializer = elementSerializer;
    }

    public TypeSerializer<T> getElementSerializer() {
        return elementSerializer;
    }

    @Override
    public Set<T> createInstance() {
        return new LinkedHashSet<>();
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<Set<T>> duplicate() {
        TypeSerializer<T> duplicateElement = elementSerializer.duplicate();
        return duplicateElement == elementSerializer
                ? this
                : new SetSerializer<>(duplicateElement);
    }

    @Override
    public Set<T> copy(Set<T> from) {
        Set<T> set = new LinkedHashSet<>();

        if (elementSerializer.isImmutableType()) {
            set.addAll(from);
        } else {
            for (T element : from) {
                set.add(elementSerializer.copy(element));
            }
        }

        return set;
    }

    @Override
    public Set<T> copy(Set<T> from, Set<T> reuse) {
        return copy(from);
    }

    @Override
    public void serialize(Set<T> record, DataOutputView target) throws IOException {
        target.writeInt(record.size());
        for (T element : record) {
            elementSerializer.serialize(element, target);
        }
    }

    @Override
    public Set<T> deserialize(DataInputView source) throws IOException {
        final int size = source.readInt();
        final Set<T> set = new LinkedHashSet<>();
        for (int i = 0; i < size; i++) {
            set.add(elementSerializer.deserialize(source));
        }
        return set;
    }

    @Override
    public Set<T> deserialize(Set<T> reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        final int num = source.readInt();
        target.writeInt(num);
        for (int i = 0; i < num; i++) {
            elementSerializer.copy(source, target);
        }
    }

    @Override
    public TypeSerializerSnapshot<Set<T>> snapshotConfiguration() {
        return new SetSerializerSnapshot<>(this);
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this || (
                obj != null &&
                        obj.getClass() == getClass() &&
                        elementSerializer.equals(((SetSerializer<?>) obj).elementSerializer)
        );
    }

    @Override
    public int hashCode() {
        return Objects.hash(SetSerializer.class, elementSerializer);
    }
}
