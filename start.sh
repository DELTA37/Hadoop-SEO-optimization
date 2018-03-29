./gradlew jar
hadoop fs -rm -f -r /user/g.kasparyants/results_hw2
rm -rf results_hw2
hadoop jar build/libs/hw2.jar hw2.SEO /data/hw2/clicks/ /user/g.kasparyants/results_hw2
hadoop fs -get /user/g.kasparyants/results_hw2
