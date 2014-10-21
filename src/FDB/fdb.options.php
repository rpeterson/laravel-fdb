<?php
namespace FDB;

class ClusterOptions {
	private $base;

	function __construct( $base ) { $this->base = $base; }

}

class DatabaseOptions {
	private $base;

	function __construct( $base ) { $this->base = $base; }

	public function set_location_cache_size( $param ) {
		$this->base->set_option_impl( 10, bytes_for_int( $param ) );
	}
	public function set_max_watches( $param ) {
		$this->base->set_option_impl( 20, bytes_for_int( $param ) );
	}
	public function set_machine_id( $param ) {
		$this->base->set_option_impl( 21, $param );
	}
	public function set_datacenter_id( $param ) {
		$this->base->set_option_impl( 22, $param );
	}
}

class TransactionOptions {
	private $base;

	function __construct( $base ) { $this->base = $base; }

	public function set_causal_write_risky() {
		$this->base->set_option_impl( 10, null );
	}
	public function set_causal_read_risky() {
		$this->base->set_option_impl( 20, null );
	}
	public function set_causal_read_disable() {
		$this->base->set_option_impl( 21, null );
	}
	public function set_next_write_no_write_conflict_range() {
		$this->base->set_option_impl( 30, null );
	}
	public function set_check_writes_enable() {
		$this->base->set_option_impl( 50, null );
	}
	public function set_read_your_writes_disable() {
		$this->base->set_option_impl( 51, null );
	}
	public function set_read_ahead_disable() {
		$this->base->set_option_impl( 52, null );
	}
	public function set_durability_datacenter() {
		$this->base->set_option_impl( 110, null );
	}
	public function set_durability_risky() {
		$this->base->set_option_impl( 120, null );
	}
	public function set_durability_dev_null_is_web_scale() {
		$this->base->set_option_impl( 130, null );
	}
	public function set_priority_system_immediate() {
		$this->base->set_option_impl( 200, null );
	}
	public function set_priority_batch() {
		$this->base->set_option_impl( 201, null );
	}
	public function set_initialize_new_database() {
		$this->base->set_option_impl( 300, null );
	}
	public function set_access_system_keys() {
		$this->base->set_option_impl( 301, null );
	}
	public function set_debug_dump() {
		$this->base->set_option_impl( 400, null );
	}
	public function set_timeout( $param ) {
		$this->base->set_option_impl( 500, bytes_for_int( $param ) );
	}
	public function set_retry_limit( $param ) {
		$this->base->set_option_impl( 501, bytes_for_int( $param ) );
	}
}

class StreamingMode {
	const WANT_ALL = -2;
	const ITERATOR = -1;
	const EXACT = 0;
	const SMALL = 1;
	const MEDIUM = 2;
	const LARGE = 3;
	const SERIAL = 4;
}

class AtomicallyOperational {
	public function add( $key, $value ) {
		$this->mutate( 2, $key, $value );
	}
	public function bit_and( $key, $value ) {
		$this->mutate( 6, $key, $value );
	}
	public function bit_or( $key, $value ) {
		$this->mutate( 7, $key, $value );
	}
	public function bit_xor( $key, $value ) {
		$this->mutate( 8, $key, $value );
	}
}

class ConflictRangeType {
	const READ = 0;
	const WRITE = 1;
}

?>
