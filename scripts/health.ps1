# Quick production triage on Windows: API health, CPU/queue counters (PerfMon), Kafka lag hint
(Invoke-RestMethod http://localhost:5000/health/ready) | Out-Host
Get-Counter '\Processor(_Total)\% Processor Time','\Memory\Available MBytes','\.NET CLR Exceptions(_Global_)\# of Exceps Thrown / sec' -SampleInterval 2 -MaxSamples 3
