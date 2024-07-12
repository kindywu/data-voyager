# data voyager
- Arrow
- DataFusion
- Polars

# duckdb clickhouse pgcli

- duckdb
  * SELECT count(*)
    FROM 'data/user_stats.parquet'
    WHERE last_visited_at >= '2024-06-15';

- clickhouse local
  * SELECT count(*)
    FROM file('data/user_stats.parquet', Parquet)
    WHERE last_visited_at >= toDateTime('2024-06-15', 'Asia/Shanghai');    // 0.065 sec

- pgcli postgres://kindy:kindy@localhost:5432/stats
  * SELECT count(*) FROM user_stats WHERE last_visited_at >= '2024-06-15'; // 0.131 sec
# other
chmod +x /usr/bin/duckdb

# dump 5M rows to parquet file

- clickhouse
select * from postgresql('localhost:5432', 'stats', 'user_stats', 'kindy', 'kindy')
into outfile 'data/user_stats.parquet'
