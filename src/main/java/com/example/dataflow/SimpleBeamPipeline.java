package com.example.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import java.util.Arrays;
import java.util.List;

public class SimpleBeamPipeline {

    public static void main(String[] args) {
        // 1. Create pipeline options and pipeline
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
        Pipeline pipeline = Pipeline.create(options);

        // 2. Create an in-memory PCollection of strings (input data)
        // This is a PTransform: Create -> PCollection<String>
        List<String> inputWords = Arrays.asList(
                "hello world",
                "apache beam is great",
                "hello beam",
                "par do transform");

        PCollection<String> lines = pipeline.apply(
                "CreateInput",
                Create.of(inputWords));

        // 3. Split each line into words (ParDo)
        // Input: PCollection<String> (lines)
        // Output: PCollection<String> (individual words)
        PCollection<String> words = lines.apply(
                "SplitIntoWords",
                ParDo.of(new SplitIntoWordsFn()));

        // 4. Convert words to KV<word, 1> for counting
        PCollection<KV<String, Integer>> wordOnePairs = words.apply(
                "WordToKeyValue",
                ParDo.of(new WordToKeyValueFn()));

        // 5. Count occurrences of each word (GroupByKey + Combine)
        // GroupByKey: PCollection<KV<String, Integer>> -> PCollection<KV<String,
        // Iterable<Integer>>>
        PCollection<KV<String, Iterable<Integer>>> grouped = wordOnePairs.apply(
                "GroupByWord",
                GroupByKey.create());

        // CombineValues: sum the 1s for each word
        PCollection<KV<String, Integer>> wordCounts = grouped.apply(
                "CountWords",
                CombineValues.<String, Integer>sumIntegers());

        // 6. Convert KV to formatted string "word: count"
        PCollection<String> formatted = wordCounts.apply(
                "FormatOutput",
                ParDo.of(new FormatOutputFn()));

        // 7. Print results to console (side effect transform)
        formatted.apply(
                "PrintOutput",
                ParDo.of(new LogOutputFn()));

        // 8. Run the pipeline
        pipeline.run().waitUntilFinish();
    }

    // ---------- DoFn: Split line into words ----------
    static class SplitIntoWordsFn extends DoFn<String, String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String line = c.element();
            // Split on whitespace and emit each word
            for (String word : line.toLowerCase().split("\\s+")) {
                if (!word.isEmpty()) {
                    c.output(word);
                }
            }
        }
    }

    // ---------- DoFn: Convert word -> KV<word, 1> ----------
    static class WordToKeyValueFn extends DoFn<String, KV<String, Integer>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String word = c.element();
            c.output(KV.of(word, 1));
        }
    }

    // ---------- DoFn: Format KV to "word: count" ----------
    static class FormatOutputFn extends DoFn<KV<String, Integer>, String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            KV<String, Integer> kv = c.element();
            c.output(kv.getKey() + ": " + kv.getValue());
        }
    }

    // ---------- DoFn: Log to console ----------
    static class LogOutputFn extends DoFn<String, String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String line = c.element();
            System.out.println(line);
            // Also output so it stays in the PCollection (optional)
            c.output(line);
        }
    }
}