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

/* The general layout of the classes is as follows:
 *
 *      Subspace   Directory (i/f)
 *           \     /       \
 *            \   /         \
 *     DirectorySubspace    DirectoryLayer
 *              |
 *              |
 *     DirectoryPartition
 */
namespace FDB\Directory;

require_once("fdb.subspace.php");

class HighContentionAllocator {
	private $counters;
	private $recent;

	public function __construct($subspace) {
		$this->counters = $subspace[0];
		$this->recent = $subspace[1];
	}

	public function allocate( $db_or_tr ) {
		return $db_or_tr->transact( function($tr) {
			$range = $tr->snapshot->get_range(
				$this->counters->range()[0],
				$this->counters->range()[1], 1, True)->to_array();
			if(count($range) == 0) {
				$start = 0;
				$count = 0;
			} else {
				// We know that there will only be one entry, is there another way to get key and value?
				foreach ($range as $k => $v) {
					$start = $this->counters->unpack($k)[0];
					$count = \unpack("V2int", $v)["int1"];
				}
			}

			$window = $this->window_size($start);
			if(($count + 1) * 2 >= $window) {
				# advance the window
				unset( $tr[ array( $this->counters, $this->counters[$start]->key() . "\x00" ) ] );
				$start += $window;
				unset( $tr[ array( $this->recent, $this->recent[$start] )] );
				$window = $this->window_size($start);
			}

			# Increment the allocation count for the current window
			$tr->add($this->counters[$start], \pack("V", 1) . "\x00\x00\x00\x00");

			while(True) {
				# As of the snapshot being read from, the window is less than half
				# full, so this should be expected to take 2 tries.  Under high
				# contention (and when the window advances), there is an additional
				# subsequent risk of conflict for this transaction.
				$candidate = \rand($start, ($start + $window) - 1);
				if( $tr[ $this->recent[ $candidate ] ]->wait() === null ) {
					$tr[ $this->recent[ $candidate ] ] = "";
					return \FDB\Tuple\pack( array($candidate) );
				}
			}
		});
	}

	private function window_size($start) {
		if($start < 255)
			return 64;
		if($start < 65535)
			return 1024;
		return 8192;
	}
}

function starts_with( $a, $b ) {
	if( is_array($a) || is_array($b) ) {
		if( !is_array($a) || !is_array($b) )
			throw new \Exception("Array-based invocations must both be arrays");
		if( count($a) < count($b) )
			return False;
		for( $i = 0; $i < count($b); $i++) {
			if( $a[$i] != $b[$i] )
				return False;
		}
		return True;
	}
	return substr($a, 0, strlen($b)) === $b;
}

interface Directory {
	public function create_or_open($db_or_tr, $path, $layer=null);

	public function open($db_or_tr, $path, $layer=null);

	public function create($db_or_tr, $path, $layer=null, $prefix=null);

	public function list_children($db_or_tr, $path=array());

	public function move($db_or_tr, $old_path, $new_path);

	public function move_to($db_or_tr, $new_absolute_path);

	public function remove($db_or_tr, $path=array());

	public function remove_if_exists($db_or_tr, $path=array());

	public function exists($db_or_tr, $path=array());

	public function get_layer();

	public function get_path();
}

function tuplify_path($path) {
	if(!is_array($path))
		$path = array($path);
	return $path;
}

function to_unicode_path( $path ) {
	if( $path instanceof \FDB\Tuple\UnicodeString )
		return array( $path );
	if( is_string($path) )
		return array( new \FDB\Tuple\UnicodeString($path) );

	if( is_array($path) ) {
		$fixed = array();
		foreach( $path as $i => $v ) {
			if( $v instanceof \FDB\Tuple\UnicodeString )
				$fixed[] = $v;
			elseif( is_string($v) )
				$fixed[] = new \FDB\Tuple\UnicodeString($v);
			else
				throw new \Exception("Path elements must be strings (" . get_class( $v ) . " at " . $i . ")");
		}
		return $fixed;
	}

	throw new \Exception("Invalid path: must be a string or an array of strings");
}

class DirectoryLayer implements Directory {
	private $content_subspace;
	private $node_subspace;
	private $root_node;
	private $allocator;
	private $path;

	# Transactional methods
	private $create_or_open;
	private $move;
	private $remove;
	private $remove_if_exists;
	private $list;
	private $exists;

	private static $SUBDIRS = 0;
	private static $VERSION = array(1,0,0);

	public function __construct( $node_subspace = null, $content_subspace = null, $allow_manual_prefixes = false ) {
		if( $node_subspace === null )
			$node_subspace = new \FDB\Subspace\Subspace( array(), "\xfe");
		if( $content_subspace === null )
			$content_subspace = new \FDB\Subspace\Subspace();

		# If specified, new automatically allocated prefixes will all fall within content_subspace
		$this->content_subspace = $content_subspace;
		$this->node_subspace = $node_subspace;
		$this->allow_manual_prefixes = $allow_manual_prefixes;

		# The root node is the one whose contents are the node subspace
		$this->root_node = $node_subspace[ $node_subspace->key() ];
		$this->allocator = new HighContentionAllocator( $this->root_node[ 'hca' ] );
		$this->layer = '';
		$this->path = array();
	}

	public function create_or_open( $db_or_tr, $path, $layer = null ) {
		return $this->create_or_open_internal( $db_or_tr, $path, $layer, null, True, True );
	}

	public function open( $db_or_tr, $path, $layer = null ) {
		return $this->create_or_open_internal($db_or_tr, $path, $layer, null, False, True);	
	}

	public function create( $db_or_tr, $path, $layer = null, $prefix = null ) {
		return $this->create_or_open_internal($db_or_tr, $path, $layer, $prefix, True, False);	
	}

	public function move_to( $db_or_tr, $new_absolute_path ) {
        throw new \Exception('The root directory cannot be moved.');
    }

   	public function move( $db_or_tr, $old_path, $new_path ) {
		return $db_or_tr->transact( function( $tr ) use( $old_path, $new_path ) {
			$this->check_version( $tr );

			$old_path = to_unicode_path( $old_path );
			$new_path = to_unicode_path( $new_path );

			if( starts_with( $new_path, $old_path ) )
				throw new \Exception("The destination directory cannot be a subdirectory of the source directory.");

			$old_node = $this->find($tr, $old_path)->prefetch_metadata( $tr );
			$new_node = $this->find($tr, $new_path)->prefetch_metadata( $tr );

			if( !$old_node->exists() )
				throw new \Exception("The source directory does not exist.");

			$old_in_part = $old_node->is_in_partition();
			$new_in_part = $new_node->is_in_partition();
			if( $old_in_part or $new_in_part ) {
				if( !$old_in_part or !$new_in_part or $old_node->path != $new_node->path )
					throw new \Exception("Cannot move between partitions.");

				return $new_node->get_contents($this)->move($tr, 
					$old_node->get_partition_subpath(), $new_node->get_partition_subpath());
			}

			if( $new_node->exists() )
				throw new \Exception("The destination directory already exists. It must be removed.");
			
			$parent_node = $this->find($tr, array_slice($new_path, 0, -1) );
			if( !$parent_node->exists() )
				throw new \Exception("The parent of the destination directory does not exist. Create it first.");
			$tr[ $parent_node->subspace[ \FDB\Directory\DirectoryLayer::$SUBDIRS ][$new_path[ count( $new_path ) - 1 ]]] = 
				$this->node_subspace->unpack($old_node->subspace->key())[0];
			$this->remove_from_parent($tr, $old_path);
			return $this->contents_of_node( $old_node->subspace, $new_path, $old_node->layer());
		});
	}

	public function remove( $db_or_tr, $path = array() ) {
		return $db_or_tr->transact( function( $tr ) use( $path ) {
			return $this->remove_internal( $tr, $path, True );
		});
	}

	public function remove_if_exists( $db_or_tr, $path = array() ) {
		return $db_or_tr->transact( function( $tr ) use( $path ) {
			return $this->remove_internal( $tr, $path, False );
		});
	}

	public function list_children( $db_or_tr, $path = array() ) {
		return $db_or_tr->transact(  function( $tr ) use( $path ) {
			$this->check_version( $tr, False );

			$path = to_unicode_path( $path );
			$node = $this->find($tr, $path)->prefetch_metadata($tr);

			if( !$node->exists() )
				throw new \Exception("The given directory does not exist.");

			if( $node->is_in_partition( $tr, True ) )
				return $node->get_contents($this)->list_children($tr, $node->get_partition_subpath());

			$children = array();
			foreach($this->subdir_names_and_nodes($tr, $node->subspace) as $name => $cnode) {
				$children[] = $name;
			}
			return $children;
		});
	}

	public function exists( $db_or_tr, $path = array() ) {
		return $db_or_tr->transact( function( $tr ) use( $path ) {
			$this->check_version( $tr, False );

			$path = to_unicode_path( $path );
			$node = $this->find($tr, $path)->prefetch_metadata($tr);

			if( !$node->exists() )
				return False;

			if( $node->is_in_partition() )
				return $node->get_contents($this)->exists($tr, $node->get_partition_subpath());

			return True;
		});
	}

	public function get_layer() {
		return $this->layer;
	}

	public function get_path() {
		return $this->path;
	}

	public function set_path( $path ) {
		if(!is_array($path))
			throw new \Exception("path can only be set to an array");

		$this->path = $path;
	}

	########################################
	## Private methods for implementation ##
	########################################

	private function create_or_open_internal( $db_or_tr, $path, $layer = null, $prefix = null, $allow_create = True, $allow_open = True ) {
		return $db_or_tr->transact( function( $tr ) use( $path, $layer, $prefix, $allow_create, $allow_open ) {
			$this->check_version( $tr, False );

			if($prefix !== null and !$this->allow_manual_prefixes) {
				if(count($path) === 0)
					throw new \Exception("Cannot specify a prefix unless manual prefixes are enabled.");
				else
					throw new \Exception("Cannot specify a prefix in a partition.");
			}

			$path = to_unicode_path( $path );

			if($path === null || count( $path ) == 0)
				throw new \Exception("The root directory may not be opened");

			$existing_node = $this->find( $tr, $path )->prefetch_metadata( $tr );

			if($existing_node->exists()) {
				if($existing_node->is_in_partition()) {
					$subpath = $existing_node->get_partition_subpath();
					$contents = $existing_node->get_contents($this);
					$sublayer = $contents->get_dir_layer();
					return $sublayer->create_or_open_internal(
						$tr, $subpath, $layer, $prefix, $allow_create, $allow_open);
				}

				if( !$allow_open )
					throw new \Exception("The directory already exists");

				if( $layer !== null && strlen($layer) > 0 && $existing_node->layer() != $layer )
					throw new \Exception("The directory exists but was created with an incompatible layer.");

				return $existing_node->get_contents($this);
			}

			if( !$allow_create )
				throw new \Exception("The directory does not exist.");

			$this->check_version($tr);

			if( $prefix === null ) {
				$prefix = $this->content_subspace->key() . $this->allocator->allocate( $tr );

				if( count( $tr->get_range_startswith($prefix, 1)->to_array() ) > 0) {
					throw new \Exception("The database has keys stored at the prefix chosen by the automatic prefix allocator: " .
											\FDB\printable($prefix) . ".");
				}

				if( !$this->is_prefix_free($tr->snapshot, $prefix) ) {
					throw new \Exception("The directory layer has manually allocated prefixes that conflict with the " .
											"automatic prefix allocator.");
				}
			}
			else if( !$this->is_prefix_free( $tr, $prefix ) )
				throw new \Exception("The given prefix is already in use.");

			if( count($path) > 1 )
				$parent_node = $this->node_with_prefix( $tr, $this->create_or_open_internal($tr, array_slice($path, 0, -1))->key() );
			else
				$parent_node = $this->root_node;

			if( $parent_node === null )
				throw new \Exception("The parent directory doesn't exist.");

			$node = $this->node_with_prefix($tr, $prefix);
			$leaf = array_slice($path, -1);
			$key = $parent_node[ \FDB\Directory\DirectoryLayer::$SUBDIRS ][ $leaf ];
			$tr[ $key ] = $prefix;
			if( $layer === null )
				$layer = '';

			$tr[$node['layer']] = $layer;

			return $this->contents_of_node( $node, $path, $layer);
		});
	}

	# This will be called only from inside a "transactional" function
	private function remove_internal( $tr, $path, $fail_on_nonexistent ) {
		$this->check_version( $tr );

		$path = to_unicode_path($path);

		if( $path === null || count( $path ) == 0 )
			throw new \Exception("The root directory may not be removed.");

		$node = $this->find($tr, $path)->prefetch_metadata($tr);

		if( !$node->exists() ) {
			if( $fail_on_nonexistent )
				throw new \Exception("The directory doesn't exist.");
			return False;
		}

		if( $node->is_in_partition() ) {
			$contents = $node->get_contents($this);
			$sublayer = $contents->get_dir_layer();
			$subpath = $node->get_partition_subpath();
			return $sublayer->remove_internal($tr, $subpath, $fail_on_nonexistent);
		}

		$this->remove_recursive($tr, $node->subspace);
		$this->remove_from_parent($tr, $path);
		return True;
	}

	private function check_version( $tr, $write_access = True ) {
		$version = $tr[ $this->root_node[ 'version' ]];

		if( $version->wait() === null ) {
			if( $write_access ) {
				$this->initialize_directory( $tr );
			}

			return;
		}

		$version = \unpack( "V3int", $version->wait() );
		if( $version['int1'] > \FDB\Directory\DirectoryLayer::$VERSION[0] )
			throw new \Exception("Cannot load directory with version");// %d.%d.%d using directory layer %d.%d.%d" % (version + self.VERSION));
		if( $version['int2'] > \FDB\Directory\DirectoryLayer::$VERSION[1] && $write_access )
			throw new \Exception("Directory with later version is read only");
	}

	private function initialize_directory( $tr ) {
		$v = \FDB\Directory\DirectoryLayer::$VERSION;
		$tr[ $this->root_node[ 'version' ]] = \pack( "VVV", $v[0], $v[1], $v[2] );
	}

	private function node_containing_key( $tr, $key ) {
		# Right now this is only used for _is_prefix_free(), but if we add
		# parent pointers to directory nodes, it could also be used to find a
		# path based on a key.
		if( starts_with( $key, $this->node_subspace->key() ) ) {
			return $this->root_node;
		}

		$range = $tr->get_range(
			$this->node_subspace->range( array() )[0],
			$this->node_subspace->pack( array( $key ) ) . "\x00",
			1, True);
		foreach( $range as $k => $value ) {
			$prev_prefix = $this->node_subspace->unpack( $k )[0];
			if( starts_with( $key, $prev_prefix ) )
				return new \FDB\Subspace\Subspace( array(), $k );
		}
		return null;
	}

	private function node_with_prefix( $tr, $prefix ) {
		if( $prefix === null )
			return null;
		return $this->node_subspace[ $prefix ];
	}

	function contents_of_node( $node, $path, $layer = "" ) {
		$prefix = $this->node_subspace->unpack( $node->key() )[0];

		if( $layer == 'partition' ) {
			return new DirectoryPartition( array_merge($this->path, $path), $prefix, $this );
		}
		return new DirectorySubspace( array_merge($this->path, $path), $prefix, $this, $layer );
	}

	private function find( $tr, $path ) {
		$n = new Node( $this->root_node, array(), $path );
		foreach( $path as $i => $name ) {
			$n = new Node(
				$this->node_with_prefix( $tr, $tr[ $n->subspace[\FDB\Directory\DirectoryLayer::$SUBDIRS][ $name ]]->wait() ), 
				\array_slice( $path, 0, $i + 1 ), $path );
			if( !$n->exists() or $n->layer( $tr ) == 'partition' )
				return $n;
		}
		return $n;
	}

	private function subdir_names_and_nodes( $tr, $node ) {
		$sd = $node[ \FDB\Directory\DirectoryLayer::$SUBDIRS ];
		$out = array();
		foreach( $tr[ $sd->range( array() )] as $k => $v ) {
			$off = $sd->unpack( $k )[0];
			$out[ $off->__toString() ] = $this->node_with_prefix( $tr, $v );
		}
		return $out;
	}

	private function remove_from_parent( $tr, $path ) {
		$parent = $this->find( $tr, array_slice( $path, 0, -1 ) );
		unset( $tr[ $parent->subspace[ \FDB\Directory\DirectoryLayer::$SUBDIRS ][ array_slice( $path, -1 )[0] ]] );
	}

	private function remove_recursive( $tr, $node ) {
		foreach( $this->subdir_names_and_nodes( $tr, $node ) as $name => $sn ) {
			$this->remove_recursive( $tr, $sn );
		}
		$tr->clear_range_startswith( $this->node_subspace->unpack( $node->key() )[0] );
		unset( $tr[ $node->range( array() )] );
	}

	private function is_prefix_free( $tr, $prefix ) {
		# Returns true if the given prefix does not "intersect" any currently
		# allocated prefix (including the root node). This means that it neither
		# contains any other prefix nor is contained by any other prefix.
		if( $prefix === null || strlen( $prefix ) == 0 )
			return False;
		if( $this->node_containing_key( $tr, $prefix ) !== null )
			return False;
		$nodes_with_prefix = $tr->get_range( $this->node_subspace->pack( array( $prefix ) ),
											 $this->node_subspace->pack( array( \FDB\strinc( $prefix ) ) ),
								   			1 )->to_array();
		return count( $nodes_with_prefix ) == 0;
	}

	public static $Directory;
}

DirectoryLayer::$Directory = new DirectoryLayer();


class DirectorySubspace extends \FDB\Subspace\Subspace implements Directory {
	protected $directory_layer; // Accessed by DirectoryPartition
	private $path;
	private $layer;

	public function __construct( $path, $prefix, $directory_layer = DirectoryLayer::Directory, $layer = null) {
		parent::__construct( array(), $prefix );
		$this->path = $path;
		$this->directory_layer = $directory_layer;
		$this->layer = $layer;
	}

	public function get_layer() {
		return $this->layer;
	}

	public function get_path() {
		return $this->path;
	}

	public function get_dir_layer() {
		return $this->directory_layer;
	}

	public function create_or_open($db_or_tr, $path, $layer=null) {
		$path = tuplify_path($path);
		$subpath = $this->partition_subpath($path);
		return $this->directory_layer->create_or_open($db_or_tr, $subpath, $layer);
	}

	public function open($db_or_tr, $path, $layer=null) {
		$path = tuplify_path($path);
		$subpath = $this->partition_subpath($path);
		return $this->directory_layer->open($db_or_tr, $subpath, $layer);
	}

	public function create($db_or_tr, $path, $layer=null, $prefix=null) {
		$path = tuplify_path($path);
		$subpath = $this->partition_subpath($path);
		return $this->directory_layer->create($db_or_tr, $subpath, $layer, $prefix);
	}

	public function list_children($db_or_tr, $path=array()) {
		$path = tuplify_path($path);
		$subpath = $this->partition_subpath($path);
		return $this->directory_layer->list_children($db_or_tr, $subpath);
	}

	public function move($db_or_tr, $old_path, $new_path) {
		$oldpath = tuplify_path($old_path);
		$oldsubpath = $this->partition_subpath($oldpath); 
		$newpath = tuplify_path($new_path);
		$newsubpath = $this->partition_subpath($newpath);
		return $this->directory_layer->move($db_or_tr, $oldsubpath, $newsubpath);
	}

	public function move_to($db_or_tr, $new_absolute_path) {
        $directory_layer = $this->get_layer_for_path(array());
        $new_absolute_path = to_unicode_path($new_absolute_path);
        $partition_len = count($directory_layer->get_path());
        $partition_path = array_slice($new_absolute_path, 0, $partition_len);
        if( $partition_path != $directory_layer->get_path() )
            throw new \Exception("Cannot move between partitions.");

        return $directory_layer->move($db_or_tr, 
        	array_slice($this->path, $partition_len),
        	array_slice($new_absolute_path, $partition_len));
	}

	public function remove($db_or_tr, $path=array()) {
		$path = tuplify_path($path);
		$directory_layer = $this->get_layer_for_path($path);
		$subpath = $this->partition_subpath($path, $directory_layer);
		return $directory_layer->remove($db_or_tr, $subpath);
	}

	public function remove_if_exists($db_or_tr, $path=array()) {
		$path = tuplify_path($path);
		$directory_layer = $this->get_layer_for_path($path);
		$subpath = $this->partition_subpath($path, $directory_layer);
		return $directory_layer->remove_if_exists($db_or_tr, $subpath);
	}

	public function exists($db_or_tr, $path=array()) {
		$path = tuplify_path($path);
		$directory_layer = $this->get_layer_for_path($path);
		$subpath = $this->partition_subpath($path, $directory_layer);
		return $directory_layer->exists($db_or_tr, $subpath);
	}

	private function partition_subpath($path, $directory_layer = null) {
		if($directory_layer === null)
			$directory_layer = $this->directory_layer;

		$slice = array_slice($this->path, count($directory_layer->get_path()));
		$result = array_merge($slice, $path);
		return $result;
	}

    # Called by all functions that could operate on this subspace directly (move_to, remove, remove_if_exists, exists)
    # Subclasses can choose to return a different directory layer to use for the operation if path is in fact empty
    protected function get_layer_for_path($path) {
        return $this->directory_layer;
    }
}

class DirectoryPartition extends DirectorySubspace {
	private $parent_directory_layer;

	public function __construct($path, $prefix, $parent_directory_layer) {
		$directory_layer = new DirectoryLayer(
			new \FDB\Subspace\Subspace(array(), $prefix . "\xfe"), 
			new \FDB\Subspace\Subspace(array(), $prefix));
		$directory_layer->set_path( $path );
		parent::__construct( $path, $prefix, $directory_layer, 'partition' );

		$this->parent_directory_layer = $parent_directory_layer;
	}

	public function key() {
		throw new \Exception("Cannot get key for the root of a directory partition.");
	}

	public function pack( $t = array() ) {
		throw new \Exception("Cannot pack keys using the root of a directory partition.");
	}

	public function unpack( $key ) {
		throw new \Exception("Cannot unpack keys using the root of a directory partition.");
	}

	public function range( $t = array() ) {
		throw new \Exception("Cannot get range for the root of a directory partition.");
	}

	public function contains( $key ) { 
		throw new \Exception("Cannot check whether a key belongs to the root of a directory partition.");
	}

	public function as_foundationdb_key() {
		throw new \Exception("Cannot use the root of a directory partition as a key.");
	}

	public function subspace($tuple) {
		throw new \Exception("Cannot open subspace in the root of a directory partition.");
	}

	protected function get_layer_for_path($path) {
		if( count( $path ) == 0 )
			return $this->parent_directory_layer;
		return $this->directory_layer;
	}
}

class Node {
	public $path;
	public $subspace;
	public $target_path;
	public $layer;

	public function __construct( $subspace, $path, $target_path ) {
		$this->subspace = $subspace;
		$this->path = $path;
		$this->target_path = $target_path;
		$this->layer = -1;
	}

	public function exists() {
		return $this->subspace !== null;
	}

	public function prefetch_metadata( $tr ) {
		if( $this->exists() )
			$this->layer( $tr );

		return $this;
	}

	public function layer( $tr = null ) {
		if( $tr !== null ) {
			if( !($tr instanceof \FDB\ReadTransaction) && !($tr instanceof \FDB\Transaction)) {
				throw new \Exception("Parameter tr must be a Transaction!");
			}

			$val = $tr[ $this->subspace['layer'] ];
			$this->layer = $val->wait();
		}
		else if( $this->layer == -1 ) {
			throw new \Exception("Layer has not been read");
		}

		return $this->layer;
	}

	public function is_in_partition( $tr = null, $include_empty_subpath = False ) {
		return $this->exists() &&
			$this->layer( $tr ) == 'partition' &&
			($include_empty_subpath || count( $this->target_path ) > count( $this->path ));
	}

	public function get_partition_subpath() {
		return array_slice( $this->target_path, count( $this->path ) );
	}

	public function get_contents( $directory_layer, $tr = null ) {
		return $directory_layer->contents_of_node( $this->subspace, $this->path, $this->layer( $tr ));
	}
}

?>
