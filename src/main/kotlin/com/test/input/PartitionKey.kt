package com.test.input

import java.io.Serializable

data class PartitionKey(
    var key: String = "",
) : Serializable {
    companion object {
        private const val serialVersionUID = 1L
    }
}
