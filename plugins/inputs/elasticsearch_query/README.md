# Elasticsearch query input plugin

This [elasticsearch](https://www.elastic.co/) query plugin queries endpoints to obtain metrics from data stored in an Elasticsearch cluster.

The following is supported:

* return number of hits for a search query
* calculate the avg/max/min for a numeric field, filtered by a query
* count number of terms for a particular field

## Motivation

It is an usual approach in Elasticsearch, when used as log management tool, to limit days of logs kept to maintain a stable storage consumption.

Sometimes there is relevant information in the data that may be necessary for a longer period, eg. if that information is used in a set of graphs or the information is used for anomaly detection and/or other monitoring needs. This plugin can be used to extract/calculate some useful metrics from events, logs and documents indexed in Elasticsearch.

## Configuration

TODO

## Sample queries

TODO