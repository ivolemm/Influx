# Influx
some tests with influx

import txt-file : /home/ubuntu/Documenten/data_sets/77-3h-grafana_data_export-test.txt
FT.77.14.01 value=148.3 1518504270000000000
FT.77.14.01 value=147.9 1518504275000000000
FT.77.14.01 value=148 1518504280000000000

ubuntu@ubuntu:~$ curl -i -XPOST 'http://localhost:8086/write?db=algist_db' --data-binary @/home/ubuntu/Documenten/data_sets/77-3h-grafana_data_export-test.txt
HTTP/1.1 204 No Content
Content-Type: application/json
Request-Id: 00c839d6-11a0-11e8-8012-000000000000
X-Influxdb-Build: OSS
X-Influxdb-Version: 1.4.2
X-Request-Id: 00c839d6-11a0-11e8-8012-000000000000
Date: Wed, 14 Feb 2018 15:59:14 GMT

