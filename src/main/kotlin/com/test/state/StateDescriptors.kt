package com.test.state

import com.test.input.GenericServiceCompositeKey
import com.test.serde.SetTypeInfo
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeinfo.Types

object StateDescriptors {
    val ACTIVE_SERVICES: MapStateDescriptor<GenericServiceCompositeKey, MutableSet<String>> = MapStateDescriptor(
        "ActiveServicesCache",
        TypeInformation.of(GenericServiceCompositeKey::class.java),
        SetTypeInfo(Types.STRING),
    )
}
