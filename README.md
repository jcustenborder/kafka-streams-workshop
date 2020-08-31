# Introduction

[Kafka Streams](http://kafka.apache.org/documentation/streams/) is a streaming engine that is built
alongside [Apache Kafka](http://kafka.apache.org/). It provides realtime access to data that is stored
on a Kafka Cluster. This repository contains example applications built using Kafka Streams for 
Financial Services use cases.

Some additional examples are available [here](https://github.com/confluentinc/kafka-streams-examples)

# Use Cases

## Customer Dashboard

In many organizations the dashboard displayed when a customer logs into their account is rendered by calling 
services that are owned by different teams. This leads to a poor experience because the web page is 
waiting on multiple service calls to return data before it renders the page. 
With this use case we will focus on updating the data that feeds the dashboard as the data changes. 

## Spending by category

With this use case we will build a user dashboard similar to Mint where the data is categorized based on spending. 
The incoming transactions will be aggregated based on the account and category of the transaction. 

## Flagging transactions by category

Many companies issue corporate cards for spending on "business" expenses. Certain categories of expenses 
are not allowed to be placed on a corporate card. This example will allow the user to specify categories 
that are prohibited, and alert on spending in these categories.