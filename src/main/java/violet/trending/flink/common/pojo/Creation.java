package violet.trending.flink.common.pojo;

import lombok.Data;

@Data
public class Creation {
    private Long creationId;
    private Long userId;
    private String category;
    private Integer status;
}
