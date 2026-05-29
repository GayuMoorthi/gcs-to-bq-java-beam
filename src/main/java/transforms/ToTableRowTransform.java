package transforms;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.api.services.bigquery.model.TableRow;

import model.ValidRecord;

import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

public class ToTableRowTransform extends PTransform<PCollection<ValidRecord>, PCollection<TableRow>> {

    @Override
    public PCollection<TableRow> expand(PCollection<ValidRecord> input) {
        return input.apply("ToTableRow", MapElements.into(TypeDescriptor.of(TableRow.class))
                .via(new JsonToTableRowFn()));
    }

    static class JsonToTableRowFn extends SimpleFunction<ValidRecord, TableRow> {
        @Override
        public TableRow apply(ValidRecord input) {
            return convertObject(input.getJsonNode());
        }

        private TableRow convertObject(JsonNode node) {
            TableRow row = new TableRow();
            Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                row.set(entry.getKey(), convertValue(entry.getValue()));
            }
            return row;
        }

        private Object convertValue(JsonNode node) {
            if (node == null || node.isNull()) return null;
            if (node.isTextual()) return node.asText();
            if (node.isInt() || node.isLong()) return node.asLong();
            if (node.isFloat() || node.isDouble() || node.isBigDecimal()) return node.asDouble();
            if (node.isBoolean()) return node.asBoolean();

            if (node.isArray()) {
                List<Object> list = new ArrayList<>();
                for (JsonNode child : node) {
                    list.add(convertValue(child));
                }
                return list;
            }

            if (node.isObject()) {
                Map<String, Object> map = new HashMap<>();
                Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
                while (fields.hasNext()) {
                    Map.Entry<String, JsonNode> e = fields.next();
                    map.put(e.getKey(), convertValue(e.getValue()));
                }
                return map;
            }

            return node.asText();
        }
    }
}