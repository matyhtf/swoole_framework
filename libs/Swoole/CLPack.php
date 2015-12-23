<?php
/**
 * Created by PhpStorm.
 * User: yanchunhao
 * Date: 2015/12/2
 * Time: 18:18
 */
namespace Swoole;

class CLPack {
	const MIN_LEN = 4, MAX_LEN = 8193, LEN_BYTE = 4;

	private $data = '', $count = 0, $len = 0;
	public $last_err = '';

	function unpack($data) {
		$this->count += strlen($data);
		$this->data .= $data;
		if ($this->count > self::LEN_BYTE && $this->len == 0) {
			$r = unpack('L', substr($this->data, 0, self::LEN_BYTE));
			$this->len = $r[1];
			if ($this->len < self::MIN_LEN || $this->len > self::MAX_LEN) {
				//包的大小不在正常范围内
				$this->last_err = "包大小超出限制	" . $this->len . "\n";
				return false;
			}
		}
		if ($this->len && $this->count >= $this->len) {
			$r = @json_decode(substr($this->data, self::LEN_BYTE), 1);
			if (json_last_error() == JSON_ERROR_NONE) {
				$this->reset(1);
				return $r;
			}
			$this->last_err = "json解包失败	{$this->data}\n";
			return false;
		}
		return [];
	}

	static function pack($data) {
		$data = json_encode($data);
		return pack('L', strlen($data) + self::LEN_BYTE) . $data;
	}

	function reset($next = false) {
		if (!$next) {
			$this->count = 0;
			$this->len = 0;
			$this->data = '';
		} else {
			$this->data = substr($this->data, $this->len);
			$this->count = $this->count - $this->len;
			$this->len = 0;
		}
	}
}