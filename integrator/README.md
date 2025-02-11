# Template Charm for Kafka Connect Integrators

This simple template charm implements the requirer logic of the `connect_client` interface. Furthermore, in order to serve the connector plugins, this charm uses a FastAPI service exposed by default at port `8080` which serves the plugins by default at `api/v1/plugin`.

To build and use this charm, the `connect-plugin` resource should be provided either during deploy or build, with Charmhub being the preferred way.

## Build & Deploy Instructions

```bash
charmcraft pack
juju deploy *.charm --resource connect-plugin=/path/to/plugin.tar
```

## Integration with Kafka Connect

```bash
# Deploy Apache Kafka & Kafka Connect charms
juju deploy kafka --config roles="broker,controller" -n 3
juju deploy kafka-connect 
juju integrate kafka kafka-connect

# Integrate with the integrator source or sink interface
juju integrate kafka-connect kafka-connect-integrator:[source|sink]
```
