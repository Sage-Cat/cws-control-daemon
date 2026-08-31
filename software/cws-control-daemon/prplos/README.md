# prplOS telemetry adapter

This directory provides a read-only telemetry agent for a prplOS-compatible AP. It queries available TR-181 counters and posts them to the controller receiver. It does not alter radios, association, network security, or QoS configuration.

At installation, provide an endpoint, a non-identifying node ID, and a token through standard input:

```sh
printf 'TOKEN\n' | ./install-prpl-agent.sh http://CONTROLLER_HOST:8770/v1/telemetry prplos-node
```

The optional host relay and reboot verifier require `CWS_TARGET`, `CWS_TARGET_KNOWN_HOSTS`, and the relevant peer/token environment variables. They have no embedded deployment target.
