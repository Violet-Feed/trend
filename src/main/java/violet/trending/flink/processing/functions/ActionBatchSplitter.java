package violet.trending.flink.processing.functions;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import violet.trending.flink.common.pojo.Action;
import violet.trending.flink.common.pojo.ActionBatch;



@Slf4j
public class ActionBatchSplitter implements FlatMapFunction<ActionBatch, Action> {

    @Override
    public void flatMap(ActionBatch batch, Collector<Action> out) {
        if (batch == null || batch.getUserId() == null) {
            log.warn("Received null batch or userId, skipping");
            return;
        }

        if (batch.getCreationList() == null || batch.getCreationList().isBlank()) {
            return;
        }

        String[] creationIds = batch.getCreationList().split(",");

        for (String creationIdStr : creationIds) {
            try {
                Action action = new Action();
                action.setActionType(batch.getActionType());
                action.setCreationId(Long.parseLong(creationIdStr.trim()));
                action.setActionTs(batch.getTimestamp());
                action.setUserId(batch.getUserId());

                out.collect(action);
            } catch (NumberFormatException e) {
                log.error("Failed to parse creationId '{}' for userId={}", creationIdStr, batch.getUserId(), e);
            }
        }
    }
}
