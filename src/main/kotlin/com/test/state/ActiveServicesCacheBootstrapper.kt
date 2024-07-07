package com.test.state

import com.test.input.PartitionKey
import com.test.input.Statuses
import org.apache.flink.api.common.state.MapState
import org.apache.flink.configuration.Configuration
import org.apache.flink.state.api.functions.KeyedStateBootstrapFunction

class ActiveServicesCacheBootstrapper : KeyedStateBootstrapFunction<PartitionKey, Statuses>() {

    @Transient
    private lateinit var mapState: MapState<String, MutableSet<String>?>

    override fun open(parameters: Configuration?) {
        super.open(parameters)
        mapState = runtimeContext.getMapState(StateDescriptors.ACTIVE_SERVICES)
    }

    override fun processElement(value: Statuses, ctx: Context) {
        value.services.forEach {
            val key = it.serviceId
            val set = mapState.get(key) ?: mutableSetOf()
            set.add(value.id)
            mapState.put(key, set)
        }
    }
}
