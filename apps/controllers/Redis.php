<?php
namespace App\Controller;
use Swoole;

class Redis extends Swoole\Controller
{
    function test()
    {
        $this->http->header('Content-Type', 'text/html; charset=UTF-8');
        $keys = $this->redis->keys('*');
        var_dump($keys);
        return $this->showTrace(true);
    }
}