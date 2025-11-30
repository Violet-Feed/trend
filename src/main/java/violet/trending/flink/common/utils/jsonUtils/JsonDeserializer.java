package violet.trending.flink.common.utils.jsonUtils;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONException;
import com.alibaba.fastjson2.JSONReader;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class JsonDeserializer<T> extends AbstractDeserializationSchema<T> {
    private final Class<T> targetClass;

    public JsonDeserializer(Class<T> targetClass) {
        this.targetClass = targetClass;
    }

    @Override
    public T deserialize(byte[] message) throws IOException {
        if (message == null || message.length == 0) {
            return null;
        }
        try {
            return JSON.parseObject(message, targetClass, JSONReader.Feature.SupportSmartMatch);
        } catch (JSONException e) {
            throw new IOException("Failed to deserialize JSON to " + targetClass.getSimpleName(), e);
        }
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeInformation.of(targetClass);
    }
}
