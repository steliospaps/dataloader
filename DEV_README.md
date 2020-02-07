#deploy to maven
see [maven](https://central.sonatype.org/pages/ossrh-guide.html)
## snapshot

```
mvn clean deploy
```

## release
```
mvn release:clean release:prepare

mvn release:perform
```


