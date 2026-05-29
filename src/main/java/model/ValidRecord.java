package model;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.Serializable;

public class ValidRecord implements Serializable {
    private final String rawJson;
    private final JsonNode jsonNode;

    public ValidRecord(String rawJson, JsonNode jsonNode) {
        this.rawJson = rawJson;
        this.jsonNode = jsonNode;
    }

    public String getRawJson() {
        return rawJson;
    }

    public JsonNode getJsonNode() {
        return jsonNode;
    }
}