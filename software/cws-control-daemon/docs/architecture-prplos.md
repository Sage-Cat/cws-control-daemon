# Read-only prplOS context architecture

The controller accepts authenticated telemetry from a prplOS-compatible access point through a small push agent. The agent reads available TR-181/pWHM counters, attaches a configured non-identifying node ID, and sends them to the controller's token-protected receiver. The controller uses the result as read-only context when evaluating sensing rates.

The adapter does not claim native Wi-Fi CSI support, vendor-specific sensing APIs, or actuation inside the access point. Deployments must configure endpoint, token, radio paths, and node identity locally. Service startup order and interface handling are explicitly deployment-specific.
