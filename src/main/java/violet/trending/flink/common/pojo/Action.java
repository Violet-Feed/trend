package violet.trending.flink.common.pojo;

import lombok.Data;

@Data
public class Action {
    private Long userId;
    private Integer actionType;
    private Long creationId;
    private Long actionTs;
}
