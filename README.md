FoundationDB Support for Laravel ~4.2
=====================================

Create a fdbsql database connection:

```
'fdbsql' => [
  'driver'   => 'fdbsql',
  'host'     => '127.0.0.1',
  'database' => 'klink_core',
  'username' => '',
  'password' => '',
  'charset'  => 'utf8',
  'prefix'   => '',
  'schema'   => 'klink_core',
  'port'     => 15432
]
```

Add Service Provider 

```
FDB\ServiceProvider
```
