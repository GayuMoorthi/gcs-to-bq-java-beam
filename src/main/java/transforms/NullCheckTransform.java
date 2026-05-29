package transforms;

import com.fasterxml.jackson.databind.JsonNode;

import model.ValidRecord;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

public class NullCheckTransform extends PTransform<PCollection<ValidRecord>, PCollection<ValidRecord>> {

    private final List<String> requiredFields;

    public NullCheckTransform(String requiredFieldsCsv) {
        this.requiredFields = Arrays.stream(requiredFieldsCsv.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
    }

    @Override
    public PCollection<ValidRecord> expand(PCollection<ValidRecord> input) {
        return input.apply("FilterRequiredFields", Filter.by(this::isValid));
    }

    private boolean isValid(ValidRecord record) {
        JsonNode node = record.getJsonNode();
        for (String field : requiredFields) {
            JsonNode value = node.get(field);
            if (value == null || value.isNull()) {
                return false;
            }
            if (value.isTextual() && value.asText().trim().isEmpty()) {
                return false;
            }
        }
        return true;
    }
}