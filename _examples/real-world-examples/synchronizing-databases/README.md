# Synchronizing Databases (MySQL to PostgreSQL)

This example shows how to use [SQL Pub/Sub](https://github.com/ThreeDotsLabs/watermill-sql) across two different databases.

## Background

Synchronizing two databases can be a tough task, especially with different data formats.

This example shows how to synchronize a MySQL table to a PostgreSQL table using watermill.

## Requirements

To run this example you will need Docker and docker-compose installed. See installation guide at https://docs.docker.com/compose/install/

## Running

```bash
docker-compose up
```

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
