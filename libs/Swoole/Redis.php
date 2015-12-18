<?php
namespace Swoole;
use Swoole;

class Redis
{
    public $_redis;
    public $config;
    public static $prefix = "autoinc_key:";
    static function getIncreaseId($appKey, $init_id = 1000)
    {
        if (empty($appKey))
        {
            return false;
        }
        $main_key = self::$prefix . $appKey;
        //已存在 就加1
        if (\Swoole::$php->redis->exists($main_key))
        {
            $inc = \Swoole::$php->redis->incr($main_key);
            if (empty($inc))
            {
                \Swoole::$php->log->put("redis::incr() failed. Error: ".\Swoole::$php->redis->getLastError());
                return false;
            }
            return $inc;
        }
        //上面的if条件返回false,可能是有错误，或者key不存在，这里要判断下
        else if(\Swoole::$php->redis->getLastError())
        {
            return false;
        }
        //这才是说明key不存在，需要初始化
        else
        {
            $init = \Swoole::$php->redis->set($main_key, $init_id);
            if ($init == false)
            {
                \Swoole::$php->log->put("redis::set() failed. Error: ".\Swoole::$php->redis->getLastError());
                return false;
            }
            else
            {
                return $init_id;
            }
        }
    }

    function __construct($config)
    {
        $this->_redis = new \Redis();
        $this->config = $config;
        $this->connect();

        if (!empty($this->config['password']))
        {
            $this->_redis->auth($this->config['password']);
        }
        if (!empty($this->config['database']))
        {
            $this->_redis->select($this->config['database']);
        }
    }

    function connect()
    {
        try {
            if($this->config['pconnect'])
            {
                return $this->_redis->pconnect($this->config['host'], $this->config['port'], $this->config['timeout']);
            }
            else
            {
                return $this->_redis->connect($this->config['host'], $this->config['port'], $this->config['timeout']);
            }
        } catch(\RedisException $e) {
            \Swoole::$php->log->error(__CLASS__." Swoole Redis Exception".var_export($e,1));
            return false;
        }
    }

    function __call($method, $args = array())
    {
        $result = false;
        for ($i = 0; $i < 2; $i++)
        {
            try {
                $result = call_user_func_array(array($this->_redis,$method),$args);
            } catch (\RedisException $e) {
                \Swoole::$php->log->error(__CLASS__." [".posix_getpid()."] Swoole Redis Exception {$i} time ".var_export($e,1));
                $result = -1;
            }
            if ($result === -1)//异常重试一次
            {
                $r = $this->checkConnection();
                if ($r === true)
                {
                    continue;
                }
            }
            break;
        }
        if ($result === -1)
            return false;
        return $result;
    }

    protected function checkConnection()
    {
        if (!@$this->_redis->ping())
        {
            $this->_redis->close();
            return $this->connect();
        }
        return true;
    }
}