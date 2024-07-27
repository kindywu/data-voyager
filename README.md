data voyager: A simple and practical data exploration tool based on dataFusion and reedline-repl-rs

![image](https://github.com/user-attachments/assets/6e238f64-d1d9-4598-8759-4c5e83c50221)

![image](https://github.com/user-attachments/assets/014da2c7-5eb4-46b0-b3d6-aaab188e6752)

![image](https://github.com/user-attachments/assets/f055327c-f003-4f4d-a6e2-e0a28c205bb4)


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

select * from postgresql('localhost:5432', 'stats', 'user_stats', 'kindy', 'kindy') limit 100
into outfile 'assets/user_stats.ndjson'
