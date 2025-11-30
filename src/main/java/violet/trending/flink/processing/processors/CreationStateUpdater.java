package violet.trending.flink.processing.processors;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import violet.trending.flink.common.pojo.Creation;

public class CreationStateUpdater extends KeyedProcessFunction<Long, Creation, Creation> {

    private transient ValueState<Creation> creationState;

    @Override
    public void open(OpenContext openContext) throws Exception {
        ValueStateDescriptor<Creation> descriptor =
                new ValueStateDescriptor<>("creation-state", Creation.class);
        creationState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(Creation creation, Context ctx, Collector<Creation> out) throws Exception {
        creationState.update(creation);
        out.collect(creation);
    }
}
