docker run -p 18123:8123 -p 19000:9000 --rm --name clickhouse-server --ulimit nofile=262144:262144 -e CLICKHOUSE_SKIP_USER_SETUP=1 clickhouse/clickhouse-server:26.6-alpine $args
