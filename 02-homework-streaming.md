# Homework

Deadline: 16.12.2020, 23:59.

The homework consists of 3 problems.
Each subsequent problem is based the previous one
and assumes additional complexity.

The first problem is mandatory, others are optional.

## 1. Keyword detector

**Goal: Detect keywords in a stream of textual data**

Expected result:
- consume strings from the input Kafka topic
- split each string into words, discard punctuation
- normalize each detected keyword: to lower case
- extract keywords provided in a dictionary
- publish the normalized keywords to the output Kafka topic

Demo:
- the application is running in YARN
- input and output Kafka topics are created
- user enters phrases in the input topic via kafka-console-consumer
- user observes detected keywords in the output topic via kafka-console-producer
- optionally: configurable keyword dictionary (not hardcoded)

## 2. Keyword counter

**Goal: Determine how many keywords occur in every minute**

Expected result:
- instead of keywords (as in the previous problem)
  publish the running count for each detected keyword
- publish counts for all keywords
  - if a keyword has not been yet detected on the input, then publish zero
  - if there is no update for a keyword, then publish the previous count
- publish for every minute
  - in other words: sliding window with 1 minute width and 1 minute shift
- assume event timestamps equal to processing timestamps

## 3. Frequent keywords detector

**Goal: Detect Top N most frequent keywords in a stream of textual data**

Expected result:
- instead of all keyword counts (as in the previous problem)
  output only counts for N most frequently occurring keywords
- optionally: configurable N

## Useful tips

Fork the [Spark project template](https://gitlab.com/1c4/hadoop-2020/spark-project-template)
and use it as a starting point for your project.

Follow the build and deployment steps described in the project README.

To implement a submitter script for your new application,
copy the dummy submitter `dummy-spark-submit.sh`
and adjust it for your application.

For Kafka console utilities,
refer to [Kafka cheatsheet](https://gitlab.com/1c4/hadoop-2020/spark-project-template/-/blob/master/KAFKA-CHEATSHEET.md).

When creating Kafka topics, avoid name clashes with your colleagues.
Create topics with unique suffixes, for example:
`input-$USER`, `output-$USER`.

Don't forget to delete the Kafka topics that you are not using anymore.

## Documentation

- [Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Structured Streaming + Kafka Integration Guide](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)
- [Slides from lections](https://gitlab.com/1c4/hadoop-2020/basic-materials/-/blob/master/streaming/streaming-data-processing-with-apache-spark-and-apache-flink.pdf)
