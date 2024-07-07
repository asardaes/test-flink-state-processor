package com.test.input

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import java.io.Serializable

@JsonIgnoreProperties(ignoreUnknown = true)
data class GenericService(
    var serviceId: String = "",
    var status: ServiceStatus = ServiceStatus.OFF,
    var countryCode: String = "foo",
) : Serializable {
    companion object {
        private const val serialVersionUID = 1L
    }
}
