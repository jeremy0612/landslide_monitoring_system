# The database was built in PostreSQL
The service go with db container when vps environment was started. 
## Psql command
To connect the CLI of psql in warehouse container
```
psql -U admin -d landslide_warehouse
```
To list all tables or users
```
\dt
\du
```
To switch to a specific database
```
\c <db_name>
```
Show all connections
```
SELECT * FROM pg_stat_activity;
```
