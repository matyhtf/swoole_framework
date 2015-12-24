<?php
namespace Swoole\Async;

class MySQL {
	public $cl_db_name;
	/**
	 * max connections for mysql client
	 * @var int $pool_size
	 */
	protected $pool_size;
	/**
	 * number of current connection
	 * @var int $connection_num
	 */
	protected $connection_num;
	/**
	 * idle connection
	 * @var array $idle_pool
	 */
	protected $idle_pool = array();
	/**
	 * work connetion
	 * @var array $work_pool
	 */
	protected $work_pool = array();
	/**
	 * database configuration
	 * @var array $config
	 */
	protected $config = array();
	/**
	 * wait connection
	 * @var arrayecho
	 */
	protected $wait_queue = array();

	/**
	 * @param array $config
	 * @param int $pool_size
	 * @throws \Exception
	 */
	public function __construct(array $config, $pool_size = 100) {
		if (empty($config['host']) ||
			empty($config['database']) ||
			empty($config['user']) ||
			empty($config['password'])
		) {
			throw new \Exception("require host, database, user, password config.");
		}
		if (!function_exists('swoole_get_mysqli_sock')) {
			throw new \Exception("require swoole_get_mysqli_sock function.");
		}
		if (empty($config['port'])) {
			$config['port'] = 3306;
		}
		$this->config = $config;
		$this->pool_size = $pool_size;
	}

	public function setPoolSize($pool_size) {
		if ($this->pool_size == $pool_size) {
			return 0;
		}
		$this->pool_size = $pool_size;
		while ($this->pool_size < $this->connection_num) {
			$conn = array_pop($this->idle_pool);
			if (!$conn) {
				break;
			}
			$this->removeConnection($conn);
		}
		return 1;
	}

	/**
	 * create mysql connection
	 */
	protected function createConnection() {
		$config = $this->config;
		$db = new \mysqli;
		$db->connect($config['host'], $config['user'], $config['password'], $config['database'], $config['port']);
		if ($db->connect_error) {
			return [
				$db->connect_errno,
				$db->connect_error
			];
		}
		if (!empty($config['charset'])) {
			$db->set_charset($config['charset']);
		}
		$db_sock = swoole_get_mysqli_sock($db);
		swoole_event_add($db_sock, array(
			$this,
			'onSQLReady'
		));
		$this->idle_pool[$db_sock] = array(
			'object' => $db,
			'socket' => $db_sock,
		);
		$this->connection_num++;
		return 0;
	}

	/**
	 * remove mysql connection
	 * @param $db_sock
	 */
	protected function removeConnection($db) {
		if (isset($this->work_pool[$db['socket']])) {
			#不能删除正在工作的连接
			return false;
		}
		swoole_event_del($db['socket']);
		$db['object']->close();
		if (isset($this->idle_pool[$db['socket']])) {
			unset($this->idle_pool[$db['socket']]);
		}
		$this->connection_num--;
	}

	/**
	 * @param $db_sock
	 * @return bool
	 */
	public function onSQLReady($db_sock) {
		$task = empty($this->work_pool[$db_sock]) ? null : $this->work_pool[$db_sock];
		if (empty($task)) {
			#echo "MySQLi Warning: Maybe SQLReady receive a Close event , such as Mysql server close the socket !\n";
			if (isset($this->idle_pool[$db_sock])) {
				$this->removeConnection($this->idle_pool[$db_sock]);
			}
			return false;
		}
		/**
		 * @var \mysqli $mysqli
		 */
		$mysqli = $task['mysql']['object'];
		$callback = $task['callback'];
		if ($result = $mysqli->reap_async_query()) {
			call_user_func($callback, $mysqli, $result);
			if (is_object($result)) {
				mysqli_free_result($result);
			}
		} else {
			call_user_func($callback, $mysqli, $result);
			#echo "MySQLi Error: " . mysqli_error($mysqli) . "\n";
		}
		//release mysqli object
		unset($this->work_pool[$db_sock]);
		if ($this->pool_size < $this->connection_num) {
			//减少连接数
			$this->removeConnection($task['mysql']);
		} else {
			$this->idle_pool[$task['mysql']['socket']] = $task['mysql'];
			//fetch a request from wait queue.
			if (count($this->wait_queue) > 0) {
				$idle_n = count($this->idle_pool);
				for ($i = 0; $i < $idle_n; $i++) {
					$new_task = array_shift($this->wait_queue);
					$this->doQuery($new_task['sql'], $new_task['callback']);
				}
			}
		}
	}

	/**
	 * @param string $sql
	 * @param callable $callback
	 */
	public function query($sql, callable $callback) {
		//no idle connection
		if (count($this->idle_pool) == 0) {
			if ($this->connection_num < $this->pool_size) {
				$r = $this->createConnection();
				if ($r) {
					return $r;
				}
				$this->doQuery($sql, $callback);
			} else {
				$this->wait_queue[] = array(
					'sql' => $sql,
					'callback' => $callback,
				);
			}
		} else {
			$this->doQuery($sql, $callback);
		}
		return 0;
	}

	/**
	 * @param string $sql
	 * @param callable $callback
	 */
	protected function doQuery($sql, callable $callback) {
		//remove from idle pool
		$db = array_pop($this->idle_pool);
		/**
		 * @var \mysqli $mysqli
		 */
		$mysqli = $db['object'];
		for ($i = 0; $i < 2; $i++) {
			$result = $mysqli->query($sql, MYSQLI_ASYNC);
			if ($result === false) {
				if ($mysqli->errno == 2013 or $mysqli->errno == 2006) {
					$mysqli->close();
					$r = $mysqli->connect();
					if ($r === true) {
						continue;
					}
				} else {
					#echo "server exception. \n";
					$this->connection_num--;
					$this->wait_queue[] = array(
						'sql' => $sql,
						'callback' => $callback,
					);
					return;
				}
			}
			break;
		}
		$task['sql'] = $sql;
		$task['callback'] = $callback;
		$task['mysql'] = $db;
		//join to work pool
		$this->work_pool[$db['socket']] = $task;
	}

	function getKey() {
		return $this->config['host'] . ':' . $this->config['port'];
	}

	function isFree() {
		return (!$this->work_pool && !$this->wait_queue) ? true : false;
	}

	function close() {
		#echo "destruct\n";
		foreach ($this->idle_pool as $conn) {
			$this->removeConnection($conn);
		}
	}

	function __destruct() {
		$this->close();
	}


}