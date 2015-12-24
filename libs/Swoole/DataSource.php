<?php
namespace Swoole;

/**
 * 数据源系统基类
 * 通过一个数据描述符，来获取数据
 * 可以从数据库、文本、缓存系统、字典等数据源得到数据
 * @author Tianfeng.Han
 * @package SwooleSystem
 * @subpackage database
 *
 */
class DataSource
{
	public $swoole;
	public $ds_uri;
	
	function __construct($swoole,$dsn)
	{
		$this->swoole = $swoole;
		$this->ds_uri = $dsn;
	}
	
	function fetch()
	{
		$dsn = self::parseDsn($this->ds_uri);
		$method = 'fetch_'.$dsn['dtype'];
		return $this->$method($dsn['dname'],$dsn['params']);
	}
	
	function fetch_model($dname,$params)
	{
		$model = $this->swoole->createModel($dname);
		if(array_key_exists('func',$params))
		{
			$func=$params['func'];
			unset($params['func']);
		}
		else $func = 'getList';
		return call_user_func_array(array($model,$func),$params);
	}
	
	function fetch_select($dname,$params)
	{
		$select = new SelectDB($this->swoole->db);
		$select->from($dname);
		$select->put($params);
		return $select->getall();		
	}
	
	static function parseDsn($dns)
	{
		$result = array();
		$dns_array = explode(':',trim($dns));
		
		$result['dtype'] = $dns_array[0];
		
		$params_array = explode(' ',trim($dns_array[1]));
		$result['dname'] = $params_array[0];
		
		$count = count($params_array);
		for($i=1;$i<$count;$i++)
		{
			$p = explode('=',trim($params_array[$i]));
			$result['params'][$p[0]] = $p[1];
		}
		return $result;
	}
}
