package com.test.input

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.apache.flink.api.common.io.DefaultInputSplitAssigner
import org.apache.flink.api.common.io.InputFormat
import org.apache.flink.api.common.io.statistics.BaseStatistics
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.io.GenericInputSplit
import org.apache.flink.core.io.InputSplit
import org.apache.flink.core.io.InputSplitAssigner

class TestInputFormat(private val total: Int) : InputFormat<Statuses, InputSplit> {
    companion object {
        private val jsonMapper by lazy { jacksonObjectMapper() }
    }

    private var n = 0

    override fun configure(parameters: Configuration) {
        // nop
    }

    override fun getStatistics(cachedStatistics: BaseStatistics): BaseStatistics {
        return cachedStatistics
    }

    override fun createInputSplits(minNumSplits: Int): Array<InputSplit> {
        return arrayOf(GenericInputSplit(0, 1))
    }

    override fun getInputSplitAssigner(inputSplits: Array<out InputSplit>?): InputSplitAssigner {
        return DefaultInputSplitAssigner(inputSplits)
    }

    override fun open(split: InputSplit?) {
        n = 0
    }

    override fun reachedEnd(): Boolean {
        return n == total
    }

    override fun nextRecord(reuse: Statuses?): Statuses {
        n++
        val json = """
            {
                "id": "${n.mod(10)}",
                "key": $n,
                "services": [
                    { "serviceId": "A", "status": "ON", "epoch": 12345, "countryCode": "foo" },
                    { "serviceId": "B", "status": "ON", "epoch": 12345, "countryCode": "foo" },
                    { "serviceId": "C", "status": "ON", "epoch": 12345, "countryCode": "foo" },
                    { "serviceId": "D", "status": "ON", "epoch": 12345, "countryCode": "foo" },
                    { "serviceId": "E", "status": "ON", "epoch": 12345, "countryCode": "foo" }
                ]
            }
        """.trimIndent()
        return jsonMapper.readValue(json)
    }

    override fun close() {
        // nop
    }
}
