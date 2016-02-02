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
        $this->config = $config;
        $this->connect();
    }

    function connect()
    {
        try
        {
            if ($this->_redis)
            {
                unset($this->_redis);
            }
            $this->_redis = new \Redis();
            if ($this->config['pconnect'])
            {
                $this->_redis->pconnect($this->config['host'], $this->config['port'], $this->config['timeout']);
            }
            else
            {
                $this->_redis->connect($this->config['host'], $this->config['port'], $this->config['timeout']);
            }
            
            if (!empty($this->config['password']))
            {
                $this->_redis->auth($this->config['password']);
            }
            if (!empty($this->config['database']))
            {
                $this->_redis->select($this->config['database']);
            }
        }
        catch (\RedisException $e)
        {
            \Swoole::$php->log->error(__CLASS__ . " Swoole Redis Exception" . var_export($e, 1));
            return false;
        }
    }

    function __call($method, $args = array())
    {
        $reConnect = false;
        while (1)
        {
            try
            {
                $result = call_user_func_array(array($this->_redis, $method), $args);
            }
            catch (\RedisException $e)
            {
                //已重连过，仍然报错
                if ($reConnect)
                {
                    throw $e;
                }

                \Swoole::$php->log->error(__CLASS__ . " [" . posix_getpid() . "] Swoole Redis[{$this->config['host']}:{$this->config['port']}]
                 Exception(Msg=" . $e->getMessage() . ", Code=" . $e->getCode() . "), Redis->{$method}, Params=" . var_export($args, 1));
                $this->_redis->close();
                $this->connect();
                $reConnect = true;
                continue;
            }
            return $result;
        }
        //不可能到这里
        return false;
    }
}
