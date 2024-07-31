package com.test;

import com.test.input.GenericService;
import com.test.input.GenericServiceCompositeKey;
import com.test.input.ServiceStatus;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.state.api.*;
import org.apache.flink.state.api.functions.WindowReaderFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.List;

public class Application {
    public static void main(String[] args) throws Exception {
        List<GenericService> services = List.of(
                new GenericService("X", ServiceStatus.ON, "FOO"),
                new GenericService("X", ServiceStatus.ON, "BAR")
        );

        StreamExecutionEnvironment flinkEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        flinkEnv.setParallelism(1);
        flinkEnv.setRuntimeMode(RuntimeExecutionMode.BATCH);

        SavepointWriter savepointWriter = SavepointWriter.newSavepoint(flinkEnv, new EmbeddedRocksDBStateBackend(), 30);

        StateBootstrapTransformation<GenericService> transformation = OperatorTransformation.bootstrapWith(flinkEnv.fromCollection(services))
                .keyBy((value) -> new GenericServiceCompositeKey(value.getServiceId(), value.getCountryCode()))
                .window(GlobalWindows.create())
                .aggregate(new AggregateFunctionForMigration());

        savepointWriter.withOperator(OperatorIdentifier.forUid("test"), transformation)
                .write("/tmp/savepoint");

        flinkEnv.execute();
        System.out.println("Savepoint created");

        // -----------------

        SavepointReader.read(flinkEnv, "/tmp/savepoint")
                .window(GlobalWindows.create())
                .aggregate(
                        "test",
                        new AggregateFunctionForMigration(),
                        new StateReader(),
                        TypeInformation.of(GenericServiceCompositeKey.class),
                        TypeInformation.of(GenericService.class),
                        TypeInformation.of(GenericService.class)
                )
                .print();

        flinkEnv.execute();
    }

    private static class AggregateFunctionForMigration implements AggregateFunction<GenericService, GenericService, GenericService> {
        @Override
        public GenericService createAccumulator() {
            return new GenericService();
        }

        @Override
        public GenericService add(GenericService value, GenericService accumulator) {
            return value;
        }

        @Override
        public GenericService getResult(GenericService accumulator) {
            return null;
        }

        @Override
        public GenericService merge(GenericService a, GenericService b) {
            return null;
        }
    }

    private static class StateReader extends WindowReaderFunction<GenericService, GenericService, GenericServiceCompositeKey, GlobalWindow> {
        @Override
        public void readWindow(GenericServiceCompositeKey genericServiceCompositeKey, Context<GlobalWindow> context, Iterable<GenericService> elements, Collector<GenericService> out) {
            System.out.println("KEY=" + genericServiceCompositeKey);
            elements.forEach(e -> {
                if (e == null) {
                    System.out.println("Why is this null?");
                } else {
                    out.collect(e);
                }
            });
        }
    }
}
