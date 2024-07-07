package com.test.state

import com.test.serde.SetTypeInfo
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.Types

object StateDescriptors {
    val ACTIVE_SERVICES: MapStateDescriptor<String, MutableSet<String>> = MapStateDescriptor(
        "ActiveServicesCache",
        Types.STRING,
        SetTypeInfo(Types.STRING),
    )
}
