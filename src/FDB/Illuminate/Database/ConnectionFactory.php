<?php namespace FDB\Illuminate\Database;

use Illuminate\Database\Connectors\ConnectionFactory as LaravelConnectionFactory;
use PDO;
use Illuminate\Database\Connectors\MySqlConnector;
use Illuminate\Database\Connectors\PostgresConnector;
use Illuminate\Database\Connectors\SQLiteConnector;
use Illuminate\Database\Connectors\SqlServerConnector;
use Illuminate\Database\MySqlConnection;
use Illuminate\Database\PostgresConnection;
use Illuminate\Database\SQLiteConnection;
use Illuminate\Database\SqlServerConnection;

class ConnectionFactory extends LaravelConnectionFactory {

  /**
   * Create a single database connection instance.
   *
   * @param  array  $config
   * @return \Illuminate\Database\Connection
   */
  protected function createSingleConnection(array $config)
  {
    $pdo = $this->createConnector($config)->connect($config);

    switch ($config['driver_class']) {
      case 'FDB\SQL\DBAL\PDOFoundationDBSQLDriver':
        return $this->createConnection('fdbsql', $pdo, $config['database'], $config['prefix'], $config);
        break;
    }
    return $this->createConnection($config['driver_class'], $pdo, $config['database'], $config['prefix'], $config);
  }


  /**
   * Create a connector instance based on the configuration.
   *
   * @param  array  $config
   * @return \Illuminate\Database\Connectors\ConnectorInterface
   *
   * @throws \InvalidArgumentException
   */
  public function createConnector(array $config)
  {


    switch ($config['driver_class']) {
      case 'FDB\SQL\DBAL\PDOFoundationDBSQLDriver':
        return new FoundationConnector;
        break;
    }

    if ( ! isset($config['driver']))
    {
      throw new \InvalidArgumentException("A driver must be specified.");
    }

    if ($this->container->bound($key = "db.connector.{$config['driver']}"))
    {
      return $this->container->make($key);
    }

    switch ($config['driver'])
    {
      case 'mysql':
        return new MySqlConnector;

      case 'pgsql':
        return new PostgresConnector;

      case 'sqlite':
        return new SQLiteConnector;

      case 'sqlsrv':
        return new SqlServerConnector;

      case 'fdbsql':
        return new FoundationConnector;
    }

    throw new \InvalidArgumentException("Unsupported driver [{$config['driver']}]");
  }

  /**
   * Create a new connection instance.
   *
   * @param  string   $driver
   * @param  \PDO     $connection
   * @param  string   $database
   * @param  string   $prefix
   * @param  array    $config
   * @return \Illuminate\Database\Connection
   *
   * @throws \InvalidArgumentException
   */
  protected function createConnection($driver, PDO $connection, $database, $prefix = '', array $config = array())
  {
    if ($this->container->bound($key = "db.connection.{$driver}"))
    {
      return $this->container->make($key, array($connection, $database, $prefix, $config));
    }

    switch ($config['driver_class']) {
      case 'FDB\SQL\DBAL\PDOFoundationDBSQLDriver':
        return new FoundationConnection($connection, $database, $prefix, $config);
        break;
    }

    switch ($driver)
    {
      case 'mysql':
        return new MySqlConnection($connection, $database, $prefix, $config);

      case 'pgsql':
        return new PostgresConnection($connection, $database, $prefix, $config);

      case 'sqlite':
        return new SQLiteConnection($connection, $database, $prefix, $config);

      case 'sqlsrv':
        return new SqlServerConnection($connection, $database, $prefix, $config);

      case 'fdbsql':
        return new FoundationConnection($connection, $database, $prefix, $config);
    }

    throw new \InvalidArgumentException("Unsupported driver [$driver]");
  }
}
