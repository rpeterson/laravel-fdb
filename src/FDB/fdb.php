<?php
/* Copyright (c) 2013 FoundationDB, LLC
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

namespace FDB;

require_once("fdb.directory.php");

# returns a 0-padded string serialization of a 64-bit integer, in little endian order
function bytes_for_int( $in ) {
	static $lowMap = 0x00000000ffffffff;
	if( !is_int( $in ) ) { throw new \Exception( 'parameter must be an integer' ); }
	$higher = $in >> 32;
	$lower = $in & $lowMap;
	return \pack('VV', $lower, $higher);
}

function printable( $in ) {
	if( !is_string($in) ) {
		throw new \Exception("printable() must be passed a string (got " . gettype($in) . ")");
	}

	$out = "";
	for($i = 0; $i < strlen( $in ); $i++) {
		$b = ord( $in[$i] );
		if ($b >= 32 && $b < 127 && $b != ord("\\")) $out .= chr($b);
		else if ($b == ord("\\")) $out .= "\\\\";
		else $out .= sprintf("\\x%02x", $b);
	}
	return $out;
}

require_once("fdb.options.php");

function key_to_bytes( $k ) {
	if( method_exists( $k, "as_foundationdb_key" ) )
		$k = $k->as_foundationdb_key();
	if( !is_string( $k ) )
		throw new \Exception("Key must be of type string");
	return $k;
}

function value_to_bytes( $v ) {
	if( method_exists( $v, "as_foundationdb_value" ) )
		$v = $v->as_foundationdb_value();
	if( !is_string( $v ) )
		throw new \Exception("Value must be of type string");
	return $v;
}

function strinc( $in ) {
	$s = rtrim($in, "\xFF");
	$l = strlen( $s );
	if($l == 0) {
		throw new \Exception("No key beyond this prefix");
	}
	return substr( $s, 0, $l - 1 ) . chr(ord( $s[ $l - 1 ] ) + 1);
}

// class FDBException is defined in module code so it can be thrown as an exception from the extension

class KeySelector {
	public $key;
	public $offset;
	public $or_equal;

	public function __construct( $key, $or_equal, $offset ) {
		$this->key = $key;
		$this->or_equal = $or_equal;
		$this->offset = $offset;
	}

	public function add( $offset ) {
		return new KeySelector($this->key, $this->or_equal, $this->offset + $offset);
	}

	public function sub( $offset ) {
		return new KeySelector($this->key, $this->or_equal, $this->offset - $offset);
	}

	public static function last_less_than($key) {
		return new KeySelector($key, false, 0);
	}

	public static function last_less_or_equal($key) {
		return new KeySelector($key, true, 0);
	}

	public static function first_greater_than($key) {
		return new KeySelector($key, true, 1);
	}

	public static function first_greater_or_equal($key) {
		return new KeySelector($key, false, 1);
	}

	public function __toString() {
		return sprintf("KeySelector(%s, %r, %d)", $this->key, $this->or_equal, $this->offset);
	}
}

function to_selector( $key_or_selector ) {
	if( !($key_or_selector instanceof KeySelector) ) {
		# This constructor does not check types - this check will happen later
		#  before the call into the extension.
		$key_or_selector = KeySelector::first_greater_or_equal( $key_or_selector );
	}
	return $key_or_selector;
}

class Future {
	protected $fpointer = null;

	function __construct( $fpointer ) {
		if( $fpointer == null )
			throw new \Exception('Future pointer must not be null');
		$this->fpointer = $fpointer;
	}

	function __destruct() {
		if( $this->fpointer != null ) {
			// Should we lock this operation?
			fdb_php_future_destroy( $this->fpointer );
			unset( $this->fpointer );
		}
	}

	public function wait() {
		throw new \Exception("Function not implemented.");
	}

	public function is_ready() {
		return fdb_php_future_is_ready( $this->fpointer );
	}

	public function block_until_ready() {
		fdb_php_future_block_until_ready( $this->fpointer );
	}

	public function cancel() {
		fdb_php_future_cancel( $this->fpointer );
	}

	public function get_error() {
		fdb_php_future_get_error( $this->fpointer );
	}
}

class FutureVoid extends Future {
	function __construct( $fpointer ) {
		parent::__construct( $fpointer );
	}

	public function wait() {
		$this->block_until_ready();
		$this->get_error();
	}
}

class FutureVersion extends Future {
	function __construct( $fpointer ) {
		parent::__construct( $fpointer );
	}

	public function wait() {
		$this->block_until_ready();
		return fdb_php_future_get_version( $this->fpointer );
	}
}

class FutureValue extends Future {
	private $value;

	function __construct( $fpointer ) {
		parent::__construct( $fpointer );
	}

	public function wait() {
		if( isset( $this->value ) )
			return $this->value;
		$this->block_until_ready();
		$this->value = fdb_php_future_get_value( $this->fpointer );
		fdb_php_future_release_memory( $this->fpointer );
		return $this->value;
	}

	public function __toString() {
		$val = $this->wait();
		if( $val != null )
			return $this->value;
		return "<nil>";
	}
}

class FutureKey extends Future {
	private $value;

	function __construct( $fpointer ) {
		parent::__construct( $fpointer );
	}

	public function wait() {
		if( isset( $this->value ) )
			return $this->value;
		$this->block_until_ready();
		$this->value = fdb_php_future_get_key( $this->fpointer );
		fdb_php_future_release_memory( $this->fpointer );
		return $this->value;
	}

	public function __toString() {
		return $this->wait();
	}
}

class FutureKeyValues extends Future {
	private $value;

	function __construct( $fpointer ) {
		parent::__construct( $fpointer );
	}

	public function wait() {
		if( isset( $this->value ) )
			return $this->value;
		$this->block_until_ready();
		$this->value = fdb_php_future_get_key_values( $this->fpointer );
		fdb_php_future_release_memory( $this->fpointer );
		return $this->value;
	}
}

class FutureStrings extends Future {
	private $value;

	function __construct( $fpointer ) {
		parent::__construct( $fpointer );
	}

	public function wait() {
		if( isset( $this->value ) )
			return $this->value;
		$this->block_until_ready();
		$this->value = fdb_php_future_get_string_array( $this->fpointer );
		# there is no freeing a "string array" at this time
		return $this->value;
	}
}

class Cluster {
	private $cpointer;
	public $options;

	function __construct( $fpointer ) {
		$f = new Future( $fpointer );
		$f->block_until_ready();
		$this->cpointer = fdb_php_future_get_cluster( $fpointer );
		$this->options = new ClusterOptions( $this );
	}

	function __destruct() {
		if( isset( $cpointer ) ) {
			fdb_php_cluster_destroy( $this->cpointer );
			unset( $dpointer );
		}
	}

	public function create_database( $database_name = null ) {
		return new Database( fdb_php_cluster_create_database( $this->cpointer, $database_name ), $this );
	}

	public function set_option_impl( $code, $param ) {
		fdb_php_cluster_set_option( $this->cpointer, $code, $param );
	}
}

class Database extends AtomicallyOperational implements \ArrayAccess {
	private $dpointer;
	private $cluster;
	public $options;

	function __construct( $fpointer, $cluster ) {
		$f = new Future( $fpointer );
		$f->block_until_ready();
		$this->dpointer = fdb_php_future_get_database( $fpointer );
		$this->options = new DatabaseOptions( $this );
		$this->cluster = $cluster;
	}

	function __destruct() {
		if( isset( $dpointer ) ) {
			fdb_php_database_destroy( $this->dpointer );
			unset( $dpointer );
		}
	}

	public function get( $key ) {
		return $this->transact( function( $tr ) use ( $key ) {
			return $tr->get( $key )->wait();
		} );
	}

	public function get_key( $selector ) {
		return $this->transact( function( $tr ) use ( $selector ) {
			return $tr->get_key( $selector )->wait();
		} );
	}

	public function get_range( $begin, $end, $limit=0, $reverse=false, $streaming_mode=StreamingMode::WANT_ALL ) {
		return $this->transact( function( $tr ) use( $begin, $end, $limit, $reverse, $streaming_mode ) {
			return $tr->get_range( $begin, $end, $limit, $reverse, $streaming_mode )->to_array();
		} );
	}

	public function get_range_startswith( $prefix, $limit=0, $reverse=false, $streaming_mode=StreamingMode::WANT_ALL ) {
		return $this->transact( function( $tr ) use( $prefix, $limit, $reverse, $streaming_mode ) {
			return $tr->get_range_startswith( $prefix, $limit, $reverse, $streaming_mode )->to_array();
		} );
	}

	public function set( $key, $value ) {
		$this->transact( function( $tr ) use( $key, $value ) {
			$tr->set( $key, $value );
		} );
	}

	protected function mutate( $code, $key, $value ) {
		$this->transact( function( $tr ) use ( $code, $key, $value ) { 
			$tr->mutate( $code, $key, $value );
		} );
	}

	public function clear( $key ) {
		$this->transact( function( $tr ) use( $key ) {
			$tr->clear( $key );
		} );
	}

	public function clear_range( $begin, $end ) {
		$this->transact( function( $tr ) use( $begin, $end ) {
			$tr->clear_range( $begin, $end );
		} );
	}

	public function clear_range_startswith( $prefix ) {
		$this->transact( function( $tr ) use( $prefix ) {
			$tr->clear_range_startswith( $prefix );
		} );
	}

	public function get_and_watch( $key ) {
		return $this->transact( function( $tr ) use( $key ) {
			$val = $tr->get( $key )->wait();
			return array( $val, $tr->watch( $key ) );
		} );
	}

	public function set_and_watch( $key, $value ) {
		return $this->transact( function( $tr ) use( $key, $value ) {
			$tr->set( $key, $value );
			return $tr->watch( $key );
		} );
	}

	public function clear_and_watch( $key ) {
		return $this->transact( function( $tr ) use( $key ) {
			$tr->clear( $key );
			return $tr->watch( $key );
		} );
	}

	###############################
	# Implementation of ArrayAccess
	###############################
	public function offsetExists( $offset ) {
		return $this->get( $offset ) != null;
	}

	public function offsetGet( $offset ) {
		if( is_array( $offset ) ) {
			$count = count( $offset );
			if( $count < 2 || $count > 3 )
				throw new \Exception("Range get array must have begin, end, and optionally -1 step");
			return $this->get_range( $offset[0], $offset[1], 0, $count == 3 ? $offset[2] == -1 : false );
		}
		return $this->get( $offset );
	}

	public function offsetSet( $offset, $value ) {
		$this->set( $offset, $value );
	}

	public function offsetUnset( $offset ) {
		if( is_array( $offset ) ) {
			if( count( $offset ) != 2 )
				throw new \Exception("Range unset array must have begin and end only");
			$this->clear_range( $offset[0], $offset[1] );
		} else {
			$this->clear( $offset );
		}
	}

	public function create_transaction() {
		return new Transaction( fdb_php_database_create_transaction( $this->dpointer ), $this );
	}

	public function set_option_impl( $code, $param ) {
		fdb_php_database_set_option( $this->dpointer, $code, $param );
	}

	public function transact( $function ) {
		$tr = $this->create_transaction();
		while(true) {
			try {
				$output = $function( $tr );
				$tr->commit()->wait();
				return $output;
			} catch( \Exception $exc ) {
				$tr->on_error( $exc )->wait();
			}
		}
	}
}

class FDBAggregate implements \IteratorAggregate {
	# these are the inputs. $begin, $end, and $limit will be modified during iteration
	private $tr;
	private $begin;
	private $end;
	private $limit;
	private $reverse;
	private $streaming_mode;

	# the first block will start fetching immediatly
	private $first_future;

	function __construct( $tr, $begin, $end, $limit, $reverse, $streaming_mode ) {
		$this->tr = $tr;
		$this->begin = $begin;
		$this->end = $end;
		$this->limit = $limit;
		$this->reverse = $reverse;
		$this->streaming_mode = $streaming_mode;

		$this->first_future = $tr->get_range_impl(
			$begin, $end, $limit, $reverse, $streaming_mode, 1);
	}

	public function getIterator() {
		return new FDBRange( $this->tr, $this->begin, $this->end, $this->limit, 
				$this->reverse, $this->streaming_mode, $this->first_future );
	}

	# FIXME: this should cache the $first_future, perhaps?
	public function to_array() {
		$streaming_mode = $this->limit != 0 ? StreamingMode::EXACT : StreamingMode::WANT_ALL;
		if( $streaming_mode != $this->streaming_mode )
			$first_future = $this->tr->get_range_impl( 
					$this->begin, $this->end, $this->limit, $this->reverse, $streaming_mode, 1 );
		else
			$first_future = $this->first_future;
		return iterator_to_array( new FDBRange( $this->tr, $this->begin, $this->end, $this->limit,
				$this->reverse, $streaming_mode, $first_future ) );
	}
}

# This implementation keeps track of the current position of the array $kvs with
#  $index externally. This is not ideal since state known internally to the array
#  is duplicated outside the array and must be kept in tight synchronization with
#  it. This is done so that next() and valid() do not need to extract keys and
#  values from $kvs for determing internal state. 
class FDBRange implements \Iterator {
	# these are the inputs. $begin, $end, and $limit will be modified during iteration
	private $tr;
	private $begin;
	private $end;
	private $limit;
	private $reverse;
	private $streaming_mode;

	# non-null when a request is outstanding
	private $first_future;
	private $future;

	# these will be set each time a future completes
	private $kvs;
	private $size;
	private $more;
	private $index;

	# for tracking the number of requests
	private $iteration;

	function __construct( $tr, $begin, $end, $limit, $reverse, $streaming_mode, $first_future ) {
		$this->tr = $tr;
		$this->begin = $begin;
		$this->end = $end;
		$this->limit = $limit;
		$this->reverse = $reverse;
		$this->streaming_mode = $streaming_mode;
		$this->iteration = 1; # the first request was already made
		$this->future = $this->first_future = $first_future;
	}

	function wait_and_assign() {
		if( $this->future == null )
			return;

		$query = $this->future->wait();
		$this->kvs = $query["key_values"];
		$this->size = $query["size"];
		$this->more = $query["more"];
		$this->future = null;

		# since this will be cached at some point, and we may have already
		#  partially iterated on the values, we need to be rewind this to
		#  make sure that the value of $index is synced with the state of $kvs.
		reset( $this->kvs );
		$this->index = 0;
	}

	# Rewind just puts the first future back in place. Further calls to
	#  wait_and_assign() will then block if needed. In practice this is
	#  called at the start of all iterations so it is important that this
	#  code path does not do extra work or issue extra requests.
	function rewind() {
		$this->future = $this->first_future;
		$this->iteration = 1;
	}

	function current() {
		$this->wait_and_assign();
		return current( $this->kvs );
	}

	function key() {
		$this->wait_and_assign();
		return key( $this->kvs );
	}

	function next() {
		# if future is waited on that means that next() has been called 
		#  with no intervening call to current()/key()/valid(). It seems that
		#  we can support this access pattern, but it may not be useful.
		$this->wait_and_assign();

		next( $this->kvs );
		$this->index++;
		if( $this->index < $this->size )
			return;

		# These tests let is know if there is more data. If they fail
		#  any future calls to valid() after this will return false.
		if( $this->size == 0 or $this->more == false )
			return;

		# if there seems like there will be some more data, start new request
		$this->iteration++;
		if( $this->limit > 0 ) {
			$this->limit -= $this->size;
			if( $this->limit <= 0 ) {
				# we have exhausted the row limit and no more requests should be made
				return;
			}
		}
		# the above check makes sure that $size is non-zero, so setting $kvs to the end
		#  and getting that key will return a non-null FDB key on which to start the next
		#  query.
		end( $this->kvs );
		$last_key = key( $this->kvs );
		if( $this->reverse )
			$this->end = KeySelector::first_greater_or_equal( $last_key );
		else
			$this->begin = KeySelector::first_greater_than( $last_key );
		$this->future = $this->tr->get_range_impl(
			$this->begin, $this->end, $this->limit, $this->reverse, $this->streaming_mode, $this->iteration);
	}

	function valid() {
		$this->wait_and_assign();
		return $this->size != 0 and $this->index < $this->size;;
	}
}

class TransactionRead extends AtomicallyOperational implements \ArrayAccess {
	protected $tpointer;
	private $snapshot;
	public $db;

	function __construct( $tpointer, $db, $snapshot ) {
		$this->tpointer = $tpointer;
		$this->db = $db;
		$this->snapshot = $snapshot;
	}

	public function get_read_version() {
		return new FutureVersion( fdb_php_transaction_get_read_version( $this->tpointer ) );
	}

	public function get( $key ) {
		$key = key_to_bytes( $key );
		return new FutureValue( fdb_php_transaction_get( $this->tpointer, $key, $this->snapshot ) );
	}

	public function get_key( $key_selector ) {
		$key = key_to_bytes( $key_selector->key );
		return new FutureKey( fdb_php_transaction_get_key( $this->tpointer, $key, $key_selector->or_equal, $key_selector->offset, $this->snapshot ) );
	}

	public function get_range_impl( $begin, $end, $limit, $reverse, $streaming_mode, $iteration ) {
		$begin_key = key_to_bytes( $begin->key );
		$end_key = key_to_bytes( $end->key );
		return new FutureKeyValues(
			fdb_php_transaction_get_range(
				$this->tpointer, $begin_key, $begin->or_equal, $begin->offset,
				$end_key, $end->or_equal, $end->offset, $limit, 0,
				$streaming_mode, $iteration, $this->snapshot, $reverse ) );
	}

	public function get_range( $begin, $end, $limit=0, $reverse=false, $streaming_mode=StreamingMode::ITERATOR ) {
		if( $begin === null )
			$begin = "";
		if( $end === null )
			$end = "\xFF";
		$begin = to_selector( $begin );
		$end = to_selector( $end );
		return new FDBAggregate( $this, $begin, $end, $limit, $reverse, $streaming_mode );
	}

	public function get_range_startswith( $prefix, $limit=0, $reverse=false, $streaming_mode=StreamingMode::ITERATOR ) {
		$prefix = key_to_bytes( $prefix );
		return $this->get_range( $prefix, strinc( $prefix ), $limit, $reverse, $streaming_mode );
	}

	###############################
	# Implementation of ArrayAccess
	###############################
	public function offsetExists( $offset ) {
		# This function, unlike offsetGet() will have to be blocking since
		#  there is no other way to make this determination.
		return $this->get( $offset )->wait() != null;
	}

	public function offsetGet( $offset ) {
		if( is_array( $offset ) ) {
			$count = count( $offset );
			if( $count < 2 || $count > 3 )
				throw new \Exception("Range get array must have begin, end, and optionally -1 step");
			return $this->get_range( $offset[0], $offset[1], 0, $count == 3 ? $offset[2] == -1 : false );
		}
		return $this->get( $offset );
	}

	public function offsetSet( $offset, $value ) {
		throw new \Exception("Cannot modify database in a read-only context");
	}

	public function offsetUnset( $offset ) {
		throw new \Exception("Cannot modify database in a read-only context");
	}
}

class Transaction extends TransactionRead {
	public $snapshot;
	public $options;

	function __construct( $tpointer, $db ) {
		parent::__construct( $tpointer, $db, false );
		$this->snapshot = new TransactionRead( $tpointer, $db, true );
		$this->options = new TransactionOptions( $this );
	}

	function __destruct() {
		if( isset( $this->tpointer ) ) {
			// Should we lock this operation?
			fdb_php_transaction_destroy( $this->tpointer );
			unset( $this->tpointer );
		}
	}

	# not be to called externally
	public function set_option_impl( $code, $param ) {
		fdb_php_transaction_set_option( $this->tpointer, $code, $param );
	}

	public function set_read_version( $version ) {
		fdb_php_transaction_set_read_version( $this->tpointer, $version );
	}

	public function get_committed_version() {
		return fdb_php_transaction_get_committed_version( $this->tpointer );
	}

	public function watch( $key ) {
		$key = key_to_bytes( $key );
		return new FutureVoid( fdb_php_transaction_watch( $this->tpointer, $key ) );
	}

	public function commit() {
		return new FutureVoid( fdb_php_transaction_commit( $this->tpointer ) );
	}

	public function reset() {
		fdb_php_transaction_reset( $this->tpointer );
	}

	public function cancel() {
		fdb_php_transaction_cancel( $this->tpointer );
	}

	public function set( $key, $value ) {
		$key = key_to_bytes( $key );
		$value = value_to_bytes( $value );
		fdb_php_transaction_set( $this->tpointer, $key, $value );
	}

	public function clear( $key ) {
		$key = key_to_bytes( $key );
		fdb_php_transaction_clear( $this->tpointer, $key );
	}

	public function clear_range( $key_begin, $key_end ) {
		$key_begin = key_to_bytes( $key_begin );
		$key_end = key_to_bytes( $key_end );
		fdb_php_transaction_clear_range( $this->tpointer, $key_begin, $key_end );
	}

	public function clear_range_startswith( $prefix ) {
		$prefix = key_to_bytes( $prefix );
		return $this->clear_range( $prefix, strinc( $prefix ) );
	}

	public function on_error( $exc ) {
		if( $exc instanceof FDBException ) {
			return new FutureVoid( fdb_php_transaction_on_error( $this->tpointer, $exc->getCode() ) );
		}
		if( $exc instanceof \Exception )
			throw $exc;
		throw new \Exception("on_error() must be called with an Exception");
	}

	public function mutate( $code, $key, $value ) {
		$key = key_to_bytes( $key );
		$value = value_to_bytes( $value );
		fdb_php_transaction_mutate( $this->tpointer, $code, $key, $value );
	}

	public function add_read_conflict_range( $begin, $end ) {
		$begin = key_to_bytes( $begin );
		$end = key_to_bytes( $end );
		fdb_php_transaction_add_conflict_range( $this->tpointer, $begin, $end, ConflictRangeType::READ );
	}

	public function add_read_conflict_key( $key ) {
		$key = key_to_bytes( $key );
		$this->add_read_conflict_range( $key, $key . "\x00" );
	}

	public function add_write_conflict_range( $begin, $end ) {
		$begin = key_to_bytes( $begin );
		$end = key_to_bytes( $end );
		fdb_php_transaction_add_conflict_range( $this->tpointer, $begin, $end, ConflictRangeType::WRITE );
	}

	public function add_write_conflict_key( $key ) {
		$key = key_to_bytes( $key );
		$this->add_write_conflict_range( $key, $key . "\x00" );
	}

	public function get_addresses_for_key( $key ) {
		$key = key_to_bytes( $key );
		return new FutureStrings( fdb_php_transaction_get_addresses_for_key( $this->tpointer, $key ) );
	}

	###################################
	# Write-access parts of ArrayAccess
	###################################
	public function offsetSet( $offset, $value ) {
		$this->set( $offset, $value );
	}

	public function offsetUnset( $offset ) {
		if( is_array( $offset ) ) {
			if( count( $offset ) != 2 )
				throw new \Exception("Range unset array must have begin and end only");
			$this->clear_range( $offset[0], $offset[1] );
		} else {
			$this->clear( $offset );
		}
	}

	public function transact( $function ) {
		return $function( $this );
	}
}

class API {
	private $api_version;
	public $directory;

	public static function api_version( $version ) {
		if( !extension_loaded('foundationdb') ) {
			if( !dl('foundationdb.so') ) {
				throw new \Exception("Could not locate or load foundationdb.so module");
			}
		}
		fdb_php_api_version( $version );
		# The above call can only ever succeed with a single value for any one process.
		#  Although there could be some optimization for having a single API object,
		#  there seemed to be no pressing need.
		$api = new API( $version );
		$api->directory = \FDB\Directory\DirectoryLayer::$Directory;
		return $api;
	}

	public function start_network() {
		fdb_php_setup_network();
		# in the php extension (at this time) this call does not block
		fdb_php_run_network();
	}

	public function create_cluster( $cluster_file = null ) {
		return new Cluster( fdb_php_create_cluster( $cluster_file ) );
	}

	public function open( $cluster_file = null, $database_name = null ) {
		$this->start_network();
		return $this->create_cluster( $cluster_file )->create_database( $database_name );
	}

	private function __construct( $version ) {
		$this->version = $version;
	}
}

?>
