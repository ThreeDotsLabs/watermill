# Synchronizing Databases (MySQL to PostgreSQL)

This example shows how to use [SQL Pub/Sub](https://github.com/ThreeDotsLabs/watermill-sql) across two different databases.

See also [SQL Pub/Sub documentation](https://watermill.io/pubsubs/sql).

## Background

Synchronizing two databases can be a tough task, especially with different data formats.
This example shows how to migrate a MySQL table to PostgreSQL table using watermill.

The application will first transfer all existing rows and then keep listening for any new inserts,
copying them to the new table as soon, as they appear. Only new rows will be detected, there's no
support for updates or deletes.

The `main.go` file contains watermill-related setup, database connections and the handler translating events
from one format to another. In `mysql.go` and `postgres.go` you will find definitions of `SchemaAdapters` for 
each database.

## Requirements

To run this example you will need Docker and docker-compose installed. See installation guide at https://docs.docker.com/compose/install/

## Running

Run the command and observe standard output. It should print out incoming users.

```bash
docker-compose up
```

Check what's inside MySQL by running:

```
docker-compose exec mysql mysql -e 'select * from watermill.users;'
```

```
+----+------------------+------------+-----------+---------------------+
| id | user             | first_name | last_name | created_at          |
+----+------------------+------------+-----------+---------------------+
|  1 | Carroll8506      | Marc       | Murphy    | 2019-09-28 13:51:53 |
|  2 | Metz8415         | Briana     | Bauch     | 2019-09-28 13:51:54 |
|  3 | Lebsack6887      | Tomasa     | Steuber   | 2019-09-28 13:51:55 |
|  4 | Hauck4518        | Alexandra  | Halvorson | 2019-09-28 13:51:56 |
|  5 | Reynolds7156     | Ariane     | Lebsack   | 2019-09-28 13:51:57 |
+----+------------------+------------+-----------+---------------------+
```

And the same for PostgreSQL:

```
docker-compose exec postgres psql -U watermill -d watermill -c 'select * from users;'
```

```
 id  |     username     |      full_name       |     created_at
-----+------------------+----------------------+---------------------
   1 | Carroll8506      | Marc Murphy          | 2019-09-28 13:51:53
   2 | Metz8415         | Briana Bauch         | 2019-09-28 13:51:54
   3 | Lebsack6887      | Tomasa Steuber       | 2019-09-28 13:51:55
   4 | Hauck4518        | Alexandra Halvorson  | 2019-09-28 13:51:56
   5 | Reynolds7156     | Ariane Lebsack       | 2019-09-28 13:51:57
```
