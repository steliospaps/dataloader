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

it fails with an error but the artifacts can be found in [maven central](https://repo1.maven.org/maven2/io/github/steliospaps/dataloader/)

```
[INFO] [ERROR] Failed to execute goal org.sonatype.plugins:nexus-staging-maven-plugin:1.6.7:release (default-cli) on project dataloader: Execution def
ault-cli of goal org.sonatype.plugins:nexus-staging-maven-plugin:1.6.7:release failed: Internal Server Error : entity body dump follows: <nexus-error>
[INFO] [ERROR]   <errors>
[INFO] [ERROR]     <error>
[INFO] [ERROR]       <id>*</id>
[INFO] [ERROR]       <msg>Unhandled: Repository with ID=&quot;iogithubsteliospaps-1004&quot; not found</msg>
[INFO] [ERROR]     </error>
[INFO] [ERROR]   </errors>
[INFO] [ERROR] </nexus-error>
``` 
