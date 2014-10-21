<?php namespace FDB;

use Illuminate\Support\ServiceProvider as BaseServiceProvider;
use FDB\Illuminate\Database\FoundationConnector;
use FDB\Illuminate\Database\FoundationConnection;

class ServiceProvider extends BaseServiceProvider 
{
  /**
   * Register the service provider.
   *
   * @return void
   */
  public function register()
  {
    $this->app->singleton('db.connector.fdbsql', function ($app, $parameters) {
      return new FoundationConnector;
    });

    $this->app->singleton('db.connection.fdbsql', function ($app, $parameters) {
      list($connection, $database, $prefix, $config) = $parameters;
      return new FoundationConnection($connection, $database, $prefix, $config);
    });

    $this->app->singleton('fdb.api', function ($app, $parameters) {
      require_once('fdb.php');
      $api = \FDB\API::api_version(200);
      return $api->open();
    });

  }
}