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

namespace FDB\Tuple;

class UnicodeString {
	public $value;
	public function __construct($value) {
		$this->value = $value;
	}

	public function __toString() {
		return $this->value;
	}
}

function pack_int( $value ) {
	static $lowMap = 0x00000000ffffffff;
	$higher = $value >> 32;
	$lower = $value & $lowMap;
	return \pack('NN', $higher, $lower);
}

function find_terminator( $in, $pos ) {
	# Finds the start of the next terminator [\x00]![\xff] or the end of $in
	while( true ) {
		$pos = strpos( $in, "\x00", $pos );
		# since $pos could be 0, triple '=' needed here:
		if( $pos === FALSE )
			return strlen( $in );
		if( $pos+1 == strlen( $in ) || $in[ $pos+1 ] != "\xFF" )
			return $pos;
		$pos += 2;
	}
}

function get_size_limits() {
	static $size_limits;
	if( !isset( $size_limits ) ) {
		$size_limits = array();
		$size_limits[0] = 1;
		$i = 1;
		for(; $i < 8; $i++) {
			$size_limits[$i] = $size_limits[$i-1] * 256;
			$size_limits[$i-1] -= 1;
		}
		$size_limits[$i-1] -= 1;
	}
	return $size_limits;
}

function decode( $v, $pos ) {
	# These are used below and can be cached!
	static $two_31_less_1;
	static $two_32;
	if( !isset( $two_31_less_1 ) ) {
		$two_31_less_1 = pow(2, 31) - 1;
		$two_32 = pow(2, 32);
	}

	$code = ord( $v[$pos] );
	if( $code == 0 )
		return array( null, $pos+1 );
	if( $code == 1 or $code == 2 ) {
		$end = find_terminator($v, $pos+1);
		if( $pos+1 == strlen($v) )
			$cut = "";
		else
			$cut = substr( $v, $pos+1, $end - ($pos+1) );
		$cleaned = str_replace( "\x00\xFF", "\x00", $cut );
		if( $code == 2 )
			$cleaned = new UnicodeString( $cleaned );
		return array( $cleaned, $end+1 );
	}
	if( $code >= 12 and $code <= 28 ) {
		$n = $code - 20;
		if( $code < 20 )
			$n = 20 - $code;
		$end = $pos + 1 + $n;
		$cut = substr( $v, $pos+1, $end - ($pos+1) );
		$padded = str_pad( $cut, 8, "\x00", STR_PAD_LEFT );
		list($higher, $lower) = array_values(\unpack('N2', $padded));
		if( $code == 28 && ord($padded[0]) > 127 ) {
			throw new \Exception("Cannot unpack integer (out of range)");
		}
		if( $code == 12 ) {
			$first_byte = ord($padded[0]);
			if( $first_byte < 127 )
				throw new \Exception("Cannot unpack integer (out of range)");
			if( $first_byte == 127 && $padded != "\x7f\xff\xff\xff\xff\xff\xff\xff" )
				throw new \Exception("Cannot unpack integer (out of range)");
		}
		$val = $higher << 32 | $lower;
		if( $code < 20 ) {
			if( $n == 8 ) {
				if( $higher == $two_31_less_1 ) {
					$val = -PHP_INT_MAX - 1;
				} else {
					$val = $lower + 1 + ($two_32 * ($higher - $two_32));
				}
			} else {
				$size_limits = get_size_limits();
				$val -= $size_limits[$n];
			}
		}
		return array( $val, $end );
	}
	throw new \Exception("Unknown data type in DB: " . $code);
}

function encode( $value ) {
	static $php_int_min;
	if( !isset( $php_int_min ) )
		$php_int_min = -PHP_INT_MAX - 1;

	# returns [code][data] (code != 0xFF)
	# encoded values are self-terminating

	if( $value === null )
		return "\x00";

	if( is_string( $value ) )
		return "\x01" . str_replace("\x00", "\x00\xFF", $value) . "\x00";

	if( $value instanceof UnicodeString ) {
		# It could be that at some later time UnicodeString can have an encoding other
		#  than UTF-8. This would mean that there would have to be some conversion here.
		return "\x02" . str_replace("\x00", "\x00\xFF", $value->value) . "\x00";
	}

	if( is_int( $value ) ) {
		$size_limits = get_size_limits();

		if( $value == 0 )
			return "\x14";
		
		$pos_item = abs( $value );
		$i = 0;
		for($i = 0; $i < count( $size_limits ) + 1; $i++) {
			if($i == count( $size_limits ))
				break;
			if($pos_item <= $size_limits[$i])
				break;
		}
		$length = $i;

		if( $value > 0 ) {
			$b = pack_int( $value, false );
			return chr(20 + $length) . substr( $b, -$length );
		}
		if( $value == $php_int_min )
			# This is a special case since the negative of int_min overflows and becomes
			#  a float. The other code actually works, but it's unclear why, so we'll
			#  just do the safe thing, since behavior could change for something undefined.
			$not_neg = PHP_INT_MAX;
		else
			$not_neg =  ~ (-$value);
		return chr(20 - $length) . substr( pack_int( $not_neg ), -$length );
	}
	throw new \Exception("Unsupported data type: " . gettype($value));
}

function unpack( $in ) {
	if( !is_string( $in ) ) { 
		throw new \Exception( 'parameter must be a string' );
	}
	$pos = 0;
	$arr = array();

	$len = strlen( $in );
	while($pos < $len) {
		$old_pos = $pos;
		list($res, $pos) = decode($in, $pos);
		$arr[] = $res;
	}
	return $arr;
}

function pack( $values ) {
	if( !is_array( $values ) ) {
		throw new \Exception( "parameter must be array of values" );
	}
	$output = array();
	foreach( $values as $value ) {
		$output[] = encode( $value );
	}
	return implode( $output );
}

function range( $values ) {
	$p = pack( $values );	
	return array( $p . "\x00", $p . "\xFF" );
}

?>
