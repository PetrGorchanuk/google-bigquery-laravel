Install
------------------
Install via Composer.

```
composer require bigquery/laravel

```
After run publishes
```
php artisan vendor:publish --provider="BigQuery\Laravel\BigQueryServiceProvider"

```
#####config/google_big_query.json
Need add to .gitignore and replace on you version credentials.

To .env need add variable

```
GOOGLE_BIG_QUERY_DATABASE=you_database_name
```

For migrations use commands

migrate all tables from /database/migrations/google_big_query
```
big_query_migrate
```

rollback all migrations
```
big_query_migrate:rollback
```

run migrate data from mysql to bigquery
```
big_query_migrate:data
```