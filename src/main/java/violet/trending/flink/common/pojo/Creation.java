package violet.trending.flink.common.pojo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class Creation {
    private Payload payload;

    public Long getCreationId() {
        return payload == null ? null : payload.getCreationId();
    }

    public Long getUserId() {
        return payload == null ? null : payload.getUserId();
    }

    public String getCategory() {
        return payload == null ? null : payload.getCategory();
    }

    public Integer getStatus() {
        return payload == null ? null : payload.getStatus();
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Payload {
        private Long id;
        private Long creationId;
        private Long userId;
        private String category;
        private Integer status;
    }
}
