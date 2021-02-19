to run the jar you can use this command: (i use EmailScheduler project as the base :sweat_smile:)
```
java -cp pubsubtest.jar com.indodana.vayu.emailscheduler.pubsubLimitTest <service_account.json path> <running time in millis> <output path> <threadcount>
```
example:
```
java -cp test.jar com.indodana.vayu.emailscheduler.pubsubLimitTest mjalbertus-pubsub-test.json 300000 result7.csv 128
```
