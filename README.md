# Introduction

[Kafka Streams](http://kafka.apache.org/documentation/streams/) is a streaming engine that is built
alongside [Apache Kafka](http://kafka.apache.org/). It provides realtime access to data that is stored
on a Kafka Cluster. This repository contains example applications built using Kafka Streams for 
Financial Services use cases.

Some additional examples are available [here](https://github.com/confluentinc/kafka-streams-examples)

## Compiling the application

The source code for the workshop is already downloaded to your cloud IDE.

```bash
cd kafka-streams-workshop
mvn clean package
```

## Use Cases

## Spending by category

This application is designed to aggregate transactions for a time period. In this case we are looking
to build a [Mint](https://mint.com) style interface that aggregates transactions for an account by 
category.


```bash
run-categorization
```


## Customer Dashboard

In many organizations the dashboard displayed when a customer logs into their account is rendered by calling 
services that are owned by different teams. This leads to a poor experience because the web page is 
waiting on multiple service calls to return data before it renders the page. 
With this use case we will focus on updating the data that feeds the dashboard as the data changes. 

```bash
run-producer 
```

```bash
run-dashboard
```


