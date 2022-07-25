FROM maven:3.6.1-jdk-11-slim
# create working directory and set
RUN mkdir /label-generator
ADD . /label-generator
WORKDIR /label-generator
RUN mvn clean install

# copy jar and set entrypoint
FROM openjdk:11-slim
COPY --from=0 /label-generator/target/smile_label_generator.jar /label-generator/smile_label_generator.jar
ENTRYPOINT ["java"]
