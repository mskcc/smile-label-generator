FROM maven:3.8.8
# create working directory and set
RUN mkdir /label-generator
ADD . /label-generator
WORKDIR /label-generator
RUN mvn clean install

# copy jar and set entrypoint
FROM ibm-semeru-runtimes:open-21.0.9_10-jdk-jammy
COPY --from=0 /label-generator/target/smile_label_generator.jar /label-generator/smile_label_generator.jar
ENTRYPOINT ["java"]
