# about
(under construction)

dataloader

A java version of the dataloader pattern
This is my take of the facebook dataloader pattern.

Similar to the [java-dataloader](https://github.com/graphql-java/java-dataloader) but it is not graphql related.

wrap a batch api, and allow multiple requests to be batched under the hood. This is a [reactor](https://projectreactor.io/)-based dataloader

# usage

## mvn dependency

```
<dependency>
  <groupId>io.github.steliospaps</groupId>
  <artifactId>dataloader</artifactId>
  <version>0.2.0</version>
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