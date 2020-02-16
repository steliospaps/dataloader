# dataloader
(under construction)

[![Build Status](https://travis-ci.org/steliospaps/dataloader.svg?branch=master)](https://travis-ci.org/steliospaps/dataloader)
[![Maven Central](https://img.shields.io/maven-central/v/io.github.steliospaps/dataloader.svg?maxAge=2592000)](https://search.maven.org/search?q=g:io.github.steliospaps%20a:dataloader)
[![Javadocs](https://img.shields.io/badge/javadoc-blue.svg)](https://www.javadoc.io/doc/io.github.steliospaps/dataloader)

A java version of the dataloader pattern.

This is my take of the facebook dataloader pattern.

Similar to the [java-dataloader](https://github.com/graphql-java/java-dataloader) but it is not graphql related.

Wrap a batch api, and allow multiple requests to be batched under the hood. This is a [reactor](https://projectreactor.io/)-based dataloader

# usage

## mvn dependency

```
<dependency>
  <groupId>io.github.steliospaps</groupId>
  <artifactId>dataloader</artifactId>
  <version>xxx</version>
</dependency>
```

## examples

### simple

```java
DataLoader<String,Integer> dataloader = ReactorDataLoader.create(list -> list.stream().map(i -> "result"+i).collect(Collectors.toList()));

CompletableFuture<String> result= dataloader.load(1);
// result.get() will yield "result1"
```

result in the example above will eventually complete.
