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
import org.slf4j.LoggerFactory

class TestNdJsonInputFormat : InputFormat<Statuses, InputSplit> {
    companion object {
        private val log = LoggerFactory.getLogger(TestNdJsonInputFormat::class.java)

        private val jsonMapper by lazy { jacksonObjectMapper() }
    }

    @Transient
    private lateinit var statuses: MutableList<Statuses>

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
        statuses = javaClass.classLoader.getResourceAsStream("dummy.txt")!!.bufferedReader().use { reader ->
            reader.lineSequence().mapTo(mutableListOf()) { line ->
                jsonMapper.readValue<Statuses>(line)
            }
        }
        log.info("Read {} statuses", statuses.size)
    }

    override fun reachedEnd(): Boolean {
        return statuses.isEmpty()
    }

    override fun nextRecord(reuse: Statuses?): Statuses {
        return statuses.removeFirst()
    }

    override fun close() {
        if (this::statuses.isInitialized) {
            statuses.clear()
        }
    }
}
