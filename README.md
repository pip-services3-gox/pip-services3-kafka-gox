# <img src="https://uploads-ssl.webflow.com/5ea5d3315186cf5ec60c3ee4/5edf1c94ce4c859f2b188094_logo.svg" alt="Pip.Services Logo" width="200"> <br/> Kafka Messaging for Pip.Services in Go

This module is a part of the [Pip.Services](http://pipservices.org) polyglot microservices toolkit.

The Kafka module contains a set of components for messaging using the Kafka protocol. Contains the implementation of the components for working with messages: KafkaMessageQueue, KafkaConnectionResolver.

The module contains the following packages:
- **Build** - factory default implementation
- **Connect** - components for setting up the connection to the Kafka broker
- **Queues** - components of working with a message queue via the Kafka protocol

<a name="links"></a> Quick links:

* [Configuration](http://docs.pipservices.org/conceptual/configuration/component_configuration/)
* [API Reference](https://godoc.org/github.com/pip-services3-gox/pip-services3-kafka-gox/)
* [Change Log](CHANGELOG.md)
* [Get Help](http://docs.pipservices.org/get_help/)
* [Contribute](http://docs.pipservices.org/contribute/)


## Use

Get the package from the Github repository:
```bash
go get -u github.com/pip-services3-gox/pip-services3-kafka-gox@latest
```

## Develop

For development you shall install the following prerequisites:
* Golang v1.18+
* Visual Studio Code or another IDE of your choice
* Docker
* Git

Run automated tests:
```bash
go test -v ./test/...
```

Generate API documentation:
```bash
./docgen.ps1
```

Before committing changes run dockerized test as:
```bash
./test.ps1
./clear.ps1
```

## Contacts

The Golang version of Pip.Services is created and maintained by **Sergey Seroukhov** and **Levichev Dmitry**.

The documentation is written by:
- **Levichev Dmitry**