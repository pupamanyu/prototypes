# Event Branching

This project is a simple Java application that demonstrates event processing using two roles: `producer` and `consumer`.
Depending on the command-line argument provided, the application will either run as an event producer or an event
consumer.

## Table of Contents

- [Project Structure](#project-structure)
- [Requirements](#requirements)
- [Setup](#setup)
- [Running the Application](#running-the-application)
- [Building the Uber JAR](#building-the-uber-jar)
- [Dependencies](#dependencies)
- [License](#license)

## Project Structure

```
event-branching
├── LICENSE
├── README.md
├── dependency-reduced-pom.xml
├── pom.xml
└── src
    ├── main
    │   ├── java
    │   │   └── com
    │   │       └── example
    │   │           ├── config
    │   │           │   └── KafkaConfig.java
    │   │           ├── consumer
    │   │           │   └── EventConsumer.java
    │   │           ├── generator
    │   │           │   └── EventGenerator.java
    │   │           ├── processor
    │   │           │   ├── EventProcessor.java
    │   │           │   ├── EventTypeAProcessor.java
    │   │           │   └── EventTypeBProcessor.java
    │   │           ├── producer
    │   │           │   └── EventProducer.java
    │   │           └── prototype
    │   │               └── Run.java
    │   └── resources
    └── test
        └── java
            └── com
                └── example
                    ├── consumer
                    │   └── EventConsumerBranchingTest.java
                    └── generator
                        └── EventGeneratorTest.java
```

- **`com.example.prototype.Run.java`**: The main entry point of the application. It takes a command-line argument to determine whether to run
  the `com.example.producer.EventProducer` or `com.example.consumer.EventConsumer`.
- **`com.example.producer.EventProducer.java`**: A class that simulates producing events.
- **`com.example.consumer.EventConsumer.java`**: A class that simulates consuming events.

## Requirements

- Java 11 (or higher)
- Maven 3.x (for building the project)

## Setup

Clone this repository to your local machine:

```bash
git clone https://github.com/pupamanyu/prototypes.git
cd event-branching
```

Make sure you have Java 11 and Maven installed:

```bash
java -version
mvn -version
```

## Running the Application

You can run the application as either an event producer or an event consumer using the `--run` flag.

### Run as Event Producer

```bash
mvn clean compile exec:java -Dexec.mainClass="com.example.prototype.Run" -Dexec.args="--run producer"
```

### Run as Event Consumer

```bash
mvn clean compile exec:java -Dexec.mainClass="com.example.prototype.Run" -Dexec.args="--run consumer"
```
## Run Tests

To Run tests, run:

```bash
mvn clean test
```

## Building the Uber JAR

To build an Uber JAR (fat jar) that includes all dependencies, run:

```bash
mvn clean package
```

This will generate an executable JAR in the `target` directory:

```bash
java -jar target/event-branching-1.0-SNAPSHOT.jar --run producer
java -jar target/event-branching-1.0-SNAPSHOT.jar --run consumer
```

## Dependencies

The project relies on the following dependencies:

- **Apache Kafka Clients**: For event streaming capabilities.
- **Apache Commons Lang**: Provides utility methods for string manipulation and other helper classes.
- **Google Guava**: Offers a wide range of utilities for collections, caching, primitives, concurrency, etc.

These dependencies are managed via Maven and are defined in the `pom.xml` file.

## License

This project is licensed under the Apache License. See the [LICENSE](LICENSE) file for details.
