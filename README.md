# Charmed Apache Kafka Connect Operator

[![Release](https://github.com/canonical/kafka-connect-operator/actions/workflows/release.yaml/badge.svg)](https://github.com/canonical/kafka-connect-operator/actions/workflows/release.yaml)
[![Tests](https://github.com/canonical/kafka-connect-operator/actions/workflows/ci.yaml/badge.svg?branch=main)](https://github.com/canonical/kafka-connect-operator/actions/workflows/ci.yaml?query=branch%3Amain)
[![Docs](https://github.com/canonical/kafka-connect-operator/actions/workflows/sync_docs.yaml/badge.svg)](https://github.com/canonical/kafka-connect-operator/actions/workflows/sync_docs.yaml)

## Overview

The Charmed Apache Kafka Connect Operator delivers automated operations management from day 0 to day 2 on [Apache Kafka Connect](https://kafka.apache.org/documentation/#connect).

This operator can be found on [Charmhub](https://charmhub.io/kafka-connect) and it comes with production-ready features such as:

- Automated or manual connector plugins management.
- Fault-tolerance, replication and scalability out-of-the-box.
- Authenticaon on REST API enabled by default.
- TLS support both on the REST API and Kafka cluster relations.
- Seamless integration with Charmed Kafka set of operators
- Seamless integration with an ecosystem of of Integrator charms supporting common ETL tasks on different database technologies offered by [Canonical Data Platform](https://canonical.com/data).

The Apache Kafka Connect Operator uses the latest Apache Kafka release, made available using the [*charmed-kafka* snap](https://github.com/canonical/charmed-kafka-snap) distributed by Canonical.

As Apache Kafka Connect requires a running Kafka cluster, this operator makes use of the [Kafka Operator](https://github.com/canonical/kafka-operator) in order to function.

## Usage

### Deployment and Basic Usage

Before using Apache Kafka Connect, a Kafka cluster needs to be deployed. The Charmed Kafka operator can be deployed as follows:

```shell
$ juju deploy kafka --channel 3/edge -n 3 --config roles="broker,controller"
```

To deploy the Charmed Apache Kafka Connect operator and relate it with the Kafka cluster, use the following commands:

```shell
$ juju deploy kafka-connect --channel latest/edge
$ juju integrate kafka-connect kafka
```

To watch the process, `juju status` can be used. Once all the units show as `active|idle`, the Apache Kafka Connect cluster is ready to be used.

### Plugin Management

Apache Kafka Connect uses a pluggable architecture model, meaning that the user could add desired functionalities by means of **Plugins**, also known as **Connectors**. Simply put, plugins are bundles of JAR files adhering to Apache Kafka Connect Connector Interface. These connectors could be an implementation of a data source connector, data sink connector, a transformer or a converter. Apache Kafka Connect automatically discovers added plugins, and the user could use the exposed REST interface to define desired ETL tasks based on available plugins.

In the Charmed Apache Kafka Connect operator, adding a plugin is as simple as calling the `juju attach-resource` command. Make sure that you bundle all required JAR files into a single TAR archive (let's call that `my-plugin.tar`) and then use the following command:

```shell
juju attach-resource kafka-connect connect-plugin=./my-plugin.tar
```

This would trigger a restart of `kafka-connect` charm, once all units show as `active|idle`, your desired plugin is ready to use. 

There is no limit on the number of plugins that could be manually added. However, the recommended way for common use-cases of ETL tasks on Data Platform charmed operators is by using the [Template Connect Integrator](https://github.com/canonical/template-connect-integrator) charm.

## Relations

The Charmed Apache Kafka Connect Operator supports Juju [relations](https://juju.is/docs/olm/relations) for interfaces listed below.

#### `connect_client` interface:

The `connect_client` interface is used with any requirer/integrator charm adhering to the `connect-client` charm relation interface. Integrators will automatically handle connectors/tasks lifecycle on Apache Kafka Connect including plugin management, startup, cleanup, and scaling, and simplify common ETL operations on Data Platform line of products.

A curated set of integrators for common ETL use cases on Canonical Data Platform line of products are provided in the [Template Connect Integrator](https://github.com/canonical/template-connect-integrator) repository. These cover use-cases such as loading data from/to MySQL, PostgreSQL, Opensearch, S3-compliant storage services, and active/passive replication of Apache Kafka topics using MirrorMaker.

#### `tls-certificates` interface:

The `tls-certificates` interface could be used with any charm that adheres to [`tls-certifcates`](https://github.com/canonical/charm-relation-interfaces/tree/main/docs/json_schemas/tls_certificates/v1) **provider** charm relation interface. One example is the [`self-signed-certificates`](https://github.com/canonical/self-signed-certificates-operator) operator by Canonical.

Note that TLS could be enabled in three different modes:

- For Apache Kafka Connect REST interface only
- Only on the relation between Apache Kafka cluster and Apache Kafka Connect.
- Both.

To enable TLS on the Apache Kafka Connect REST interface, use following commands:

```shell
# deploy the TLS charm
juju deploy self-signed-certificates --channel=stable
# to enable TLS on Apache Kafka Connect, relate the applications
juju integrate self-signed-certificates kafka-connect
```

To enable TLS on the relation between Apache Kafka cluster and Apache Kafka Connect, use the following commands:

```
juju integrate self-signed-certificates kafka
```

To disable TLS on each interface, remove the respective relation.

```shell
juju remove-relation kafka-connect self-signed-certificates
juju remove-relation kafka self-signed-certificates
```

Note: The TLS settings here are for self-signed-certificates which are not recommended for production clusters, the `tls-certificates-operator` charm offers a variety of configurations, read more on the TLS charm [here](https://charmhub.io/tls-certificates-operator)

## Monitoring

The Charmed Apache Kafka Connect Operator comes with the [JMX exporter](https://github.com/prometheus/jmx_exporter/).
The metrics can be queried by accessing the `http://<unit-ip>:9100/metrics` endpoints.

Additionally, the charm provides integration with the [Canonical Observability Stack](https://charmhub.io/topics/canonical-observability-stack).

Deploy the `cos-lite` bundle in a Kubernetes environment. This can be done by following the
[deployment tutorial](https://charmhub.io/topics/canonical-observability-stack/tutorials/install-microk8s).
Since the Charmed Apache Kafka Operator is deployed on a machine environment, it is needed to offer the endpoints
of the COS relations. The [offers-overlay](https://github.com/canonical/cos-lite-bundle/blob/main/overlays/offers-overlay.yaml)
can be used, and this step is shown in the COS tutorial.

Next, deploy [grafana-agent](https://charmhub.io/grafana-agent) and follow the
[tutorial](https://discourse.charmhub.io/t/using-the-grafana-agent-machine-charm/8896)
to relate it to the COS Lite offers.

Now, relate `kafka-connect` with the grafana-agent:

```shell
juju integrate kafka-connect grafana-agent
```

After this is complete, Grafana will have the `Kafka Connect Cluster` dashboard available.

## Contributing

Please see the [Juju SDK docs](https://juju.is/docs/sdk) for guidelines on enhancements to this charm following best practice guidelines, and [CONTRIBUTING.md](https://github.com/canonical/kafka-connect-operator/blob/main/CONTRIBUTING.md) for developer guidance. 

### We are Hiring!

Also, if you truly enjoy working on open-source projects like this one and you would like to be part of the OSS revolution, please don't forget to check out the [open positions](https://canonical.com/careers/all) we have at [Canonical](https://canonical.com/). 

## License

The Charmed Karapace K8s Operator is free software, distributed under the Apache Software License, version 2.0. See [LICENSE](https://github.com/canonical/kafka-connect-operator/blob/main/LICENSE) for more information.
