package com.test

import com.test.input.*
import com.test.state.ActiveServicesCacheBootstrapper
import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.connector.source.Boundedness
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.state.api.OperatorIdentifier
import org.apache.flink.state.api.OperatorTransformation
import org.apache.flink.state.api.SavepointWriter
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.transformations.LegacySourceTransformation
import org.apache.flink.util.Collector
import java.nio.file.Paths
import kotlin.io.path.ExperimentalPathApi
import kotlin.io.path.deleteRecursively

@OptIn(ExperimentalPathApi::class)
fun main(args: Array<String>) {
    val flinkEnv = StreamExecutionEnvironment.getExecutionEnvironment().apply {
        setRuntimeMode(RuntimeExecutionMode.BATCH)
    }

    val savepointWriter = SavepointWriter.newSavepoint(flinkEnv, EmbeddedRocksDBStateBackend(), 120)

//    val total = args.firstOrNull()?.toIntOrNull() ?: 200_000
//    val inputFormat = TestInputFormat(total)
    val inputFormat = TestNdJsonInputFormat()
    val source = flinkEnv.createInput(inputFormat).forceNonParallel()
    with(source.transformation as LegacySourceTransformation<*>) {
        boundedness = Boundedness.BOUNDED
    }

//    val allowed = setOf(
//        GenericServiceCompositeKey("A"),
//        GenericServiceCompositeKey("B"),
//        GenericServiceCompositeKey("C"),
//        GenericServiceCompositeKey("D"),
//        GenericServiceCompositeKey("E"),
//    )
    val allowed = (1001..1030).mapTo(mutableSetOf()) { GenericServiceCompositeKey(it.toString()) }
    val filtered = source.process(ActiveServicesFilter(allowed))

    configureStateBootstrap(filtered, savepointWriter, "first")
    configureStateBootstrap(filtered, savepointWriter, "second")
    configureStateBootstrap(filtered, savepointWriter, "third")
    configureStateBootstrap(filtered, savepointWriter, "fourth")
    configureStateBootstrap(filtered, savepointWriter, "fifth")
    configureStateBootstrap(filtered, savepointWriter, "sixth")

    savepointWriter.write("file:///tmp/sp")
    flinkEnv.execute()
    println("DONE")
    Paths.get("/tmp/sp").deleteRecursively()
}

private fun configureStateBootstrap(cacheStream: DataStream<Statuses>, savepointWriter: SavepointWriter, uid: String) {
    val transformation = OperatorTransformation
        .bootstrapWith(cacheStream)
        .keyBy { PartitionKey(it.key) }
        .transform(ActiveServicesCacheBootstrapper())

    val id = OperatorIdentifier.forUid(uid)
    savepointWriter
        .removeOperator(id)
        .withOperator(id, transformation)
}

private class ActiveServicesFilter(
    private val allowedServices: Set<GenericServiceCompositeKey>,
) : ProcessFunction<Statuses, Statuses>() {

    companion object {
        private const val serialVersionUID = 1L
    }

    override fun processElement(value: Statuses, ctx: Context, out: Collector<Statuses>) {
        val services = value.services.filter {
            it.status == ServiceStatus.ON &&
                    GenericServiceCompositeKey(it.serviceId, it.countryCode) in allowedServices
        }
        if (services.isNotEmpty()) {
            out.collect(value.copy(services = services))
        }
    }
}
