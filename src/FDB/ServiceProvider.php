<?php namespace FDB;

use Illuminate\Support\ServiceProvider as BaseServiceProvider;
use FDB\Illuminate\Database\FoundationConnector;
use FDB\Illuminate\Database\FoundationConnection;
use FDB\Illuminate\Database\DatabaseManager;
use FDB\Illuminate\Database\ConnectionFactory;

class ServiceProvider extends BaseServiceProvider 
{
  /**
   * Register the service provider.
   *
   * @return void
   */
  public function register()
  {
    $this->app->singleton('db.connector.fdbsql', function ($app, $parameters)
    {
      return new FoundationConnector;
    });

    $this->app->singleton('db.connection.fdbsql', function ($app, $parameters)
    {
      list($connection, $database, $prefix, $config) = $parameters;
      return new FoundationConnection($connection, $database, $prefix, $config);
    });

    $this->app->singleton('fdb', function ($app, $parameters)
    {
      require_once('fdb.php');
      $fdb = new \stdClass();
      $fdb->api = \FDB\API::api_version(200);
      $fdb->db  = $fdb->api->open();
      return $fdb;
    });

    $this->app->bindShared('db.factory', function($app)
    {
      return new ConnectionFactory($app);
    });

    $this->app->bindShared('db', function($app)
    {
      return new DatabaseManager($app, $app['db.factory']);
    });

  }
}