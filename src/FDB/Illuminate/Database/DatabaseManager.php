<?php namespace FDB\Illuminate\Database;

use Illuminate\Database\DatabaseManager as LaravelDatabaseManager;

class DatabaseManager extends LaravelDatabaseManager {

  public function __construct($app, ConnectionFactory $factory)
  {
    parent::__construct($app, $factory);
  }


  /**
	 * Make the database connection instance.
	 *
	 * @param  string  $name
	 * @return \Illuminate\Database\Connection
	 */
	protected function makeConnection($name)
	{
		$config = $this->getConfig($name);

		// First we will check by the connection name to see if an extension has been
		// registered specifically for that connection. If it has we will call the
		// Closure and pass it the config allowing it to resolve the connection.
		if (isset($this->extensions[$name]))
		{
			return call_user_func($this->extensions[$name], $config, $name);
		}

    if (isset($config['driver'])) {
      $driver = $config['driver'];

      // Next we will check to see if an extension has been registered for a driver
      // and will call the Closure if so, which allows us to have a more generic
      // resolver for the drivers themselves which applies to all connections.
      if (isset($this->extensions[$driver]))
      {
        return call_user_func($this->extensions[$driver], $config, $name);
      }
    }

		return $this->factory->make($config, $name);
	}

}