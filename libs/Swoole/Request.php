<?php
namespace Swoole;

class Request
{
    /**
     * 文件描述符
     * @var int
     */
    public $fd;
    public $id;

    /**
     * 请求时间
     * @var int
     */
    public $time;

    /**
     * 客户端IP
     * @var
     */
    public $remote_ip;

    /**
     * 客户端PORT
     * @var
     */
    public $remote_port;

    public $get     = array();
    public $post    = array();
    public $file    = array();
    public $cookie  = array();
    public $session = array();
    public $server  = array();

    /**
     * @var \StdClass
     */
    public $attrs;

    public $head = array();
    public $body;
    public $meta = array();

    public $finish = false;
    public $ext_name;
    public $status;

    /**
     * 将原始请求信息转换到PHP超全局变量中
     */
    public function setGlobal()
    {
        if ($this->get) {
            $_GET = $this->get;
        }
        if ($this->post) {
            $_POST = $this->post;
        }
        if ($this->file) {
            $_FILES = $this->file;
        }
        if ($this->cookie) {
            $_COOKIE = $this->cookie;
        }
        if ($this->server) {
            $_SERVER = $this->server;
        }
        $_REQUEST = array_merge($this->get, $this->post, $this->cookie);

        $_SERVER['REQUEST_URI']     = $this->meta['uri'];
        $_SERVER['REMOTE_ADDR']     = $this->remote_ip;
        $_SERVER['REMOTE_PORT']     = $this->remote_port;
        $_SERVER['REQUEST_METHOD']  = $this->meta['method'];
        $_SERVER['REQUEST_TIME']    = $this->time;
        $_SERVER['SERVER_PROTOCOL'] = $this->meta['protocol'];
        $_SERVER['QUERY_STRING']    = $this->meta['query'];
        $_SERVER['DOCUMENT_ROOT']   = $this->meta['document_root'];
    }

    public function unsetGlobal()
    {
        $_REQUEST = $_SESSION = $_COOKIE = $_FILES = $_POST = $_SERVER = $_GET = array();
    }

    public function isWebSocket()
    {
        return isset($this->head['Upgrade']) && strtolower($this->head['Upgrade']) == 'websocket';
    }

    public function formatHeaderKeys()
    {
        // 处理http头里面的key,不同客户端传入的可能不标准
        foreach ($this->head as $key => $value) {
            $_keys = explode('-', $key);
            foreach ($_keys as $n => $_key) {
                $_keys[$n] = ucfirst(strtolower($_key));
            }
            $newKey              = implode('-', $_keys);
            $this->head[$newKey] = $value;

            $_serverKey                = 'HTTP_' . strtoupper(str_replace('-', '_', $key));
            $this->server[$_serverKey] = $value;
        }
    }
}
