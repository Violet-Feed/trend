package violet.trending.flink.common.pojo;

import lombok.Data;

@Data
public class ActionBatch {
    private Long actionId;
    private Integer actionType;
    private Long userId;
    private Long timestamp;
    private String creationList;
}
