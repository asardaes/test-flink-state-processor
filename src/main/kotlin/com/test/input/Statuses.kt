package com.test.input

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import org.apache.flink.api.common.typeinfo.TypeInfo
import org.apache.flink.api.common.typeinfo.TypeInfoFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeinfo.Types
import java.io.Serializable
import java.lang.reflect.Type

@JsonIgnoreProperties(ignoreUnknown = true)
@TypeInfo(Statuses.TypeInformationFactory::class)
data class Statuses(
    val id: String = "",
    val key: String = "",
    val services: List<GenericService> = emptyList(),
) : Serializable {
    companion object {
        private const val serialVersionUID = 1L
    }

    class TypeInformationFactory : TypeInfoFactory<Statuses>() {
        override fun createTypeInfo(
            t: Type?,
            genericParameters: MutableMap<String, TypeInformation<*>>?
        ): TypeInformation<Statuses> {
            return Types.POJO(
                Statuses::class.java, mapOf(
                    "id" to Types.STRING,
                    "key" to Types.STRING,
                    "services" to Types.LIST(TypeInformation.of(GenericService::class.java)),
                )
            )
        }
    }
}
