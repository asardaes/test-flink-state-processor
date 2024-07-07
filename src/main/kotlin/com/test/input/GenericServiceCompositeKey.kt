package com.test.input

import java.io.Serializable

data class GenericServiceCompositeKey(
    var serviceId: String = "",
    var countryCode: String = "foo",
) : Serializable, Comparable<GenericServiceCompositeKey> {

    companion object {
        private const val serialVersionUID = 1L

        private val comparator = compareBy(GenericServiceCompositeKey::serviceId, GenericServiceCompositeKey::countryCode)
    }

    override fun compareTo(other: GenericServiceCompositeKey): Int {
        return comparator.compare(this, other)
    }
}
