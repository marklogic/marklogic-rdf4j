#marklogic-sesame-repository examples

This directory contains a sample project that demonstrates usage of marklogic-sesame-repository. 

For development branches, it points to the local user's Maven repository.

##Setup and Running

This example suite utilises the same marklogic-sesame environment/marklogic setup (referenced in gradle.properties) and it will need to be operational to run these examples. 
  
To run examples; 

Simple sesame example.
```
gradle runExample1
```

Use MarkLogic specific features (rulesets, perms, constraining query ...).
```
gradle runExample2
```

Return number of triples.
```
gradle runExample3
```

Example of Loading 100,000 triples.
```
gradle runExample4
```