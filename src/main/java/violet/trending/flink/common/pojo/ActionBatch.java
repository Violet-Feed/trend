package violet.trending.flink.common.pojo;

import lombok.Data;

@Data
public class ActionBatch {
    private String actionTypeList;    // "1,1"
    private String creationIdList;    // "111,222"
    private String timestampList;     // "111,222"
    private Long userId;
}
