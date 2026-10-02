# Troubleshooting Runbook
- **Incidents delayed**: check consumer lag `kafka-consumer-groups.sh --describe --group nexus-processor`; scale processor or check DLQ `sc.events.dlq`.
- **High API latency**: PerfMon (% Processor Time, .NET CLR Exceptions, Memory), Grafana p95 panel; run `sp_GetOpenIncidents` with actual execution plan.
- **Network issues (Kafka/SQL timeouts)**: Wireshark filter `tcp.port==9092 && tcp.analysis.retransmission`; `Test-NetConnection host -Port 1433`.
- **Auth failures (401/403)**: decode JWT, verify `tid`, `roles`, audience `nexuscontrol-api`.
- **Missing events in search**: Elasticsearch `GET sc-events/_count`; check Redis idempotency key `seen:<eventId>`.
