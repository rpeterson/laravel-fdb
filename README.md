FoundationDB Support for Laravel ~4.2
=====================================

Create a fdbsql database connection:

```
'fdbsql' => [
  'driver'   => 'fdbsql',
  'host'     => '127.0.0.1',
  'database' => 'database',
  'username' => '',
  'password' => '',
  'charset'  => 'utf8',
  'prefix'   => '',
  'schema'   => 'database',
  'port'     => 15432
]
```

Add Service Provider 

```
FDB\ServiceProvider
```
