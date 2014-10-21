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

require_once( "fdb.php" );

function get_addresses_for_key( $tr, $key ) {
	return $tr->get_addresses_for_key( $key );
}

function get_boundary_keys( $db_or_tr, $begin, $end ) {
	if( $db_or_tr instanceof \FDB\Transaction ) {
		$tr = $db_or_tr->db->create_transaction();
		$tr->set_read_version( $db_or_tr->get_read_version()->wait() );
	} elseif( $db_or_tr instanceof \FDB\Database ) {
		$tr = $db_or_tr->create_transaction();
	} else {
		throw new \Exception("get_boundary_keys requires a Database or Transaction as the first parameter");
	}

	return new BoundaryIterable($tr, $begin, $end);
}

class BoundaryIterable implements \IteratorAggregate {
	private $tr;
	private $begin;
	private $end;
	private $query;

	function __construct($tr, $begin, $end) {
		$this->tr = $tr;
		$this->begin = $begin;
		$this->end = $end;
		$tr->options->set_access_system_keys();
		$this->query = $tr->snapshot->get_range("\xFF/keyServers/" . $begin, "\xFF/keyServers/" . $end)->getIterator();
	}

	public function getIterator() {
		return new BoundaryIterator($this->tr, $this->begin, $this->end, $this->query);
	}
}

class BoundaryIterator implements \Iterator {
	private $tr;
	private $begin;
	private $last_begin;
	private $saved_begin;
	private $end;
	private $query;

	function __construct($tr, $begin, $end, $query) {
		$this->tr = $tr;
		$this->begin = $this->saved_begin = $this->last_begin = $begin;
		$this->end = $end;
		$this->query = $query;
	}

	function restart_or_throw( $e ) {
		if( $e->getCode() == 1007 and $this->begin != $this->last_begin ) {
			$this->tr = $this->tr->db->create_transaction();
		} else {
			$this->tr->on_error( $e )->wait();
		}

		$this->tr->options->set_access_system_keys();
		$this->query = $this->tr->snapshot->get_range(
			"\xFF/keyServers/" . $this->begin, "\xFF/keyServers/" . $this->end)->getIterator();
		$this->last_begin = $this->begin;
	}

	function rewind() {
		if( $this->begin != $this->saved_begin ) {
			$this->begin = $this->last_begin = $this->saved_begin;
			$this->query = $this->tr->snapshot->get_range(
				"\xFF/keyServers/" . $this->begin, "\xFF/keyServers/" . $this->end)->getIterator();
		}
	}

	function current() {
		while(true) {
			try {
				if( strlen( $this->query->key() ) == 13 )
					$k = "";
				else
					$k = substr( $this->query->key(), 13 );

				$this->begin = $k . "\x00";
				return $k;
			} catch( \FDB\FDBException $e ) {
				$this->restart_or_throw( $e );
			}
		}
	}

	function key() {
		return null;
	}

	function next() {
		try {
			$this->query->next();
		} catch( \FDB\FDBException $e ) {
			$this->restart_or_throw( $e );
		}
	}

	function valid() {
		while( true ) {
			try {
				return $this->query->valid();
			} catch( \FDB\FDBException $e ) {
				$this->restart_or_throw( $e );
			}
		}
	}
}
?>
