package violet.trending.flink.processing.functions;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import violet.trending.flink.common.pojo.Action;
import violet.trending.flink.common.pojo.ActionBatch;

import java.util.function.Function;

@Slf4j
public class ActionBatchSplitter implements FlatMapFunction<ActionBatch, Action> {

    @Override
    public void flatMap(ActionBatch batch, Collector<Action> out) {
        if (batch == null || batch.getUserId() == null) {
            log.warn("Received null batch or userId, skipping");
            return;
        }

        Function<String, String[]> safeSplit = str ->
                (str == null || str.trim().isEmpty()) ? new String[0] : str.split(",");

        String[] actionTypes = safeSplit.apply(batch.getActionTypeList());
        String[] creationIds = safeSplit.apply(batch.getCreationIdList());
        String[] timestamps = safeSplit.apply(batch.getTimestampList());

        if (actionTypes.length != creationIds.length || actionTypes.length != timestamps.length) {
            log.error("Inconsistent list lengths for userId={}: actionTypes={}, creationIds={}, timestamps={}",
                    batch.getUserId(), actionTypes.length, creationIds.length, timestamps.length);
        }

        for (int i = 0; i < timestamps.length; i++) {
            try {
                Action action = new Action();
                action.setActionType(Integer.parseInt(actionTypes[i].trim()));
                action.setCreationId(Long.parseLong(creationIds[i].trim()));
                action.setActionTs(Long.parseLong(timestamps[i].trim()));
                action.setUserId(batch.getUserId());

                out.collect(action);
            } catch (NumberFormatException e) {
                log.error("Failed to parse action at index {} for userId={}: actionType={}, creationId={}, timestamp={}",
                        i, batch.getUserId(), actionTypes[i], creationIds[i], timestamps[i], e);
            }
        }
    }
}
