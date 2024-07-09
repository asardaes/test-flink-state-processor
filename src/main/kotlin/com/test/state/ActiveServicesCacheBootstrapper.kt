package com.test.state

import com.test.input.GenericServiceCompositeKey
import com.test.input.PartitionKey
import com.test.input.Statuses
import org.apache.flink.api.common.state.MapState
import org.apache.flink.configuration.Configuration
import org.apache.flink.state.api.functions.KeyedStateBootstrapFunction

class ActiveServicesCacheBootstrapper : KeyedStateBootstrapFunction<PartitionKey, Statuses>() {

    private var latestKey: String? = null

    private val helperMap = mutableMapOf<GenericServiceCompositeKey, MutableSet<String>>()

    @Transient
    private lateinit var mapState: MapState<GenericServiceCompositeKey, MutableSet<String>?>

    override fun open(parameters: Configuration?) {
        super.open(parameters)
        mapState = runtimeContext.getMapState(StateDescriptors.ACTIVE_SERVICES)
    }

    /**
     * IMPORTANT: state processor API requires batch mode,
     * and Flink sorts the data by key,
     * so the logic takes advantage of that.
     */
    override fun processElement(value: Statuses, ctx: Context) {
        if (value.key != latestKey) {
            latestKey = value.key
            helperMap.values.forEach(MutableSet<String>::clear)
        }
        value.services.forEach {
            val key = GenericServiceCompositeKey(it.serviceId, it.countryCode)
            val set = helperMap.computeIfAbsent(key) { mutableSetOf() }
            set.add(value.id)
            mapState.put(key, set)
        }
    }
}
