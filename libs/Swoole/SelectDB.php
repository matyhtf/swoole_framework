<?php
namespace Swoole;

/**
 * 查询数据库的封装类，基于底层数据库封装类，实现SQL生成器
 * 注：仅支持MySQL，不兼容其他数据库的SQL语法
 * @author Tianfeng.Han
 * @package SwooleSystem
 * @subpackage Database
 */
class SelectDB
{
    static $error_call = '';
    static $allow_regx = '#^([a-z0-9\(\)\._=\-\+\*\`\s\'\",]+)$#i';

    public $table      = '';
    public $primary    = 'id';
    public $select     = '*';
    public $sql        = '';
    public $limit      = '';
    public $where      = '';
    public $order      = '';
    public $group      = '';
    public $use_index  = '';
    public $having     = '';
    public $join       = '';
    public $union      = '';
    public $for_update = '';

    /**
     * @var \Swoole\RecordSet
     */
    protected $result;

    //Union联合查询
    private $if_union     = false;
    private $union_select = '';

    //Join连接表
    private $if_join          = false;
    private $if_add_tablename = false;

    //Count计算
    private $count_fields = '*';

    public $page_size = 10;
    public $num       = 0;
    public $pages     = 0;
    public $page      = 0;
    public $pager     = null;

    public $auto_cache = false;
    public $cache_lifetime;
    public $cache_prefix = 'swoole_selectdb_';

    public $RecordSet;

    public $is_execute = 0;

    public $result_filter = array();

    public $call_by = 'func';

    /**
     * @var \Swoole\Database
     */
    public $db;

    public function __construct($db)
    {
        $this->db = $db;
    }

    /**
     * 初始化，select的值，参数$where可以指定初始化哪一项
     * @param $what
     */
    public function init($what = '')
    {
        if ($what == '') {
            $this->table     = '';
            $this->primary   = 'id';
            $this->select    = '*';
            $this->sql       = '';
            $this->limit     = '';
            $this->where     = '';
            $this->order     = '';
            $this->group     = '';
            $this->use_index = '';
            $this->join      = '';
            $this->union     = '';
        } else {
            $this->$what = '';
        }

    }

    /**
     * 字段等于某个值，支持子查询，$where可以是对象
     * @param $field
     * @param $_where
     */
    public function equal($field, $_where)
    {
        if ($_where instanceof SelectDB) {
            $where = $field . '=(' . $_where->getsql() . ')';
        } else {
            $where = "`$field`='$_where'";
        }
        $this->where($where);
    }

    /**
     * 指定表名，可以使用table1,table2
     * @param $table
     */
    public function from($table)
    {
        if (strpos($table, "`") === false) {
            $this->table = "`" . $table . "`";
        } else {
            $this->table = $table;
        }
    }

    /**
     * 指定查询的字段，select * from table
     * 可多次使用，连接多个字段
     * @param $select
     * @param $force
     * @return null
     */
    public function select($select, $force = false)
    {
        if ($this->select == "*" or $force) {
            $this->select = $select;
        } else {
            $this->select = $this->select . ',' . $select;
        }
    }

    /**
     * where参数，查询的条件
     * @param $where
     * @return null
     */
    public function where($where)
    {
        //$where = str_replace(' or ','',$where);
        if ($this->where == "") {
            $this->where = "WHERE " . $where;
        } else {
            $this->where = $this->where . " AND " . $where;
        }
    }

    /**
     * 指定查询所使用的索引字段
     * @param $field
     * @return null
     */
    public function useIndex($field)
    {
        self::sqlSafe($field);
        $this->use_index = "USE INDEX($field)";
    }

    /**
     * 相似查询like
     * @param $field
     * @param $like
     * @return null
     */
    public function like($field, $like)
    {
        self::sqlSafe($field);
        $this->where("`{$field}` LIKE '{$like}'");
    }

    /**
     * 使用or连接的条件
     * @param $where
     * @return null
     */
    public function orwhere($where)
    {
        if ($this->where == "") {
            $this->where = "WHERE " . $where;
        } else {
            $this->where = $this->where . " OR " . $where;
        }
    }

    /**
     * 查询的条数
     * @param $limit
     * @return null
     */
    public function limit($limit)
    {
        if (!empty($limit)) {
            $_limit = explode(',', $limit, 2);
            if (count($_limit) == 2) {
                $this->limit = 'LIMIT ' . (int) $_limit[0] . ',' . (int) $_limit[1];
            } else {
                $this->limit = "LIMIT " . (int) $limit;
            }
        } else {
            $this->limit = '';
        }
    }

    /**
     * 指定排序方式
     * @param $order
     * @return null
     */
    public function order($order)
    {
        if (!empty($order)) {
            self::sqlSafe($order);
            $this->order = "ORDER BY $order";
        } else {
            $this->order = '';
        }
    }

    /**
     * 组合方式
     * @param $group
     * @return null
     */
    public function group($group)
    {
        if (!empty($group)) {
            self::sqlSafe($group);
            $this->group = "GROUP BY $group";
        } else {
            $this->group = '';
        }

    }

    /**
     * group后条件
     * @param $having
     * @return null
     */
    public function having($having)
    {
        if (!empty($having)) {
            self::sqlSafe($having);
            $this->having = "HAVING $having";
        } else {
            $this->having = '';
        }
    }

    /**
     * IN条件
     * @param $field
     * @param $ins
     * @return null
     */
    public function in($field, $ins)
    {
        $ins = trim($ins, ','); //去掉2面的分号
        $this->where("`$field` IN ({$ins})");
    }

    /**
     * NOT IN条件
     * @param $field
     * @param $ins
     * @return null
     */
    public function notin($field, $ins)
    {
        $this->where("`$field` NOT IN ({$ins})");
    }

    /**
     * INNER连接
     * @param $table_name
     * @param $on
     * @return null
     */
    public function join($table_name, $on)
    {
        $this->join .= "INNER JOIN `{$table_name}` ON ({$on})";
    }

    /**
     * 左连接
     * @param $table_name
     * @param $on
     * @return null
     */
    public function leftjoin($table_name, $on)
    {
        $this->join .= "LEFT JOIN `{$table_name}` ON ({$on})";
    }

    /**
     * 右连接
     * @param $table_name
     * @param $on
     * @return null
     */
    public function rightjoin($table_name, $on)
    {
        $this->join .= "RIGHT JOIN `{$table_name}` ON ({$on})";
    }

    /**
     * 分页参数,指定每页数量
     * @param $pagesize
     * @return null
     */
    public function pagesize($pagesize)
    {
        $this->page_size = (int) $pagesize;
    }

    /**
     * 分页参数,指定当前页数
     * @param $page
     * @return null
     */
    public function page($page)
    {
        $this->page = (int) $page;
    }

    /**
     * 主键查询条件
     * @param $id
     * @return null
     */
    public function id($id)
    {
        $this->where("`{$this->primary}` = '$id'");
    }

    /**
     * 启用缓存
     * @param int $lifetime
     */
    public function cache($lifetime = 300)
    {
        $this->cache_lifetime = $lifetime;
    }

    /**
     * 产生分页
     * @return null
     */
    public function paging()
    {
        $this->num = $this->count();
        $offset    = ($this->page - 1) * $this->page_size;
        if ($offset < 0) {
            $offset = 0;
        }
        if ($this->num % $this->page_size > 0) {
            $this->pages = intval($this->num / $this->page_size) + 1;
        } else {
            $this->pages = $this->num / $this->page_size;
        }
        $this->limit($offset . ',' . $this->page_size);
        $this->pager = new Pager(array('total' => $this->num,
            'perpage'                              => $this->page_size,
            'nowindex'                             => $this->page,
        ));
    }

    /**
     * 使SQL元素安全
     * @param $sql_sub
     * @return null
     */
    public static function sqlSafe($sql_sub)
    {
        if (!preg_match(self::$allow_regx, $sql_sub)) {
            echo $sql_sub;
            if (self::$error_call === '') {
                die('sql block is not safe!');
            } else {
                call_user_func(self::$error_call);
            }

        }
    }
    /**
     * 获取组合成的SQL语句字符串
     * @param $ifreturn
     * @return string | null
     */
    public function getsql($ifreturn = true)
    {
        $this->sql = "SELECT {$this->select} FROM {$this->table}";
        $this->sql .= implode(' ',
            array(
                $this->join,
                $this->use_index,
                $this->where,
                $this->union,
                $this->group,
                $this->having,
                $this->order,
                $this->limit,
                $this->for_update,
            ));

        if ($this->if_union) {
            $this->sql = str_replace('{#union_select#}', $this->union_select, $this->sql);
        }
        if ($ifreturn) {
            return $this->sql;
        }
    }

    public function rawPut($params)
    {
        foreach ($params as $array) {
            if (isset($array[0]) and isset($array[1]) and count($array) == 2) {
                $this->_call($array[0], $array[1]);
            } else {
                $this->rawPut($array);
            }
        }
    }

    /**
     * 锁定行或表
     * @return null
     */
    public function lock()
    {
        $this->for_update = 'for update';
    }

    /**
     * 执行生成的SQL语句
     * @param $sql
     * @return null
     */
    public function exeucte($sql = '')
    {
        if ($sql == '') {
            $this->getsql(false);
        } else {
            $this->sql = $sql;
        }
        $this->result = $this->db->query($this->sql);
        $this->is_execute++;
    }

    /**
     * SQL联合
     * @param $sql
     * @return null
     */
    public function union($sql)
    {
        $this->if_union = true;
        if ($sql instanceof SelectDB) {
            $this->union_select = $sql->select;
            $sql->select        = '{#union_select#}';
            $this->union        = 'UNION (' . $sql->getsql(true) . ')';
        } else {
            $this->union = 'UNION (' . $sql . ')';
        }

    }
    /**
     * 将数组作为指令调用
     * @param $params
     * @return null
     */
    public function put($params)
    {
        if (isset($params['put'])) {
            Error::info('SelectDB Error!', 'Params put() cannot call put()!');
        }
        //处理where条件
        if (isset($params['where'])) {
            $wheres = $params['where'];
            if (is_array($wheres)) {
                foreach ($wheres as $where) {
                    $this->where($where);
                }
            } else {
                $this->where($wheres);
            }

            unset($params['where']);
        }
        //处理orwhere条件
        if (isset($params['orwhere'])) {
            $orwheres = $params['orwhere'];
            if (is_array($orwheres)) {
                foreach ($orwheres as $orwhere) {
                    $this->orwhere($orwhere);
                }
            } else {
                $this->$orwheres($orwheres);
            }

            unset($params['orwhere']);
        }
        //处理walk调用
        if (isset($params['walk'])) {
            foreach ($params['walk'] as $call) {
                list($key, $value) = each($call);
                $this->_call($key, $value);
            }
            unset($params['walk']);
        }
        //处理其他参数
        foreach ($params as $key => $value) {
            $this->_call($key, $value);
        }
    }
    private function _call($method, $param)
    {
        $method = strtolower($method);

        if ($method == 'update' or $method == 'delete' or $method == 'insert') {
            return false;
        }

        if (strpos($method, '_') !== 0) {
            if (method_exists($this, $method)) {
                if (is_array($param)) {
                    call_user_func_array(array($this, $method), $param);
                } else {
                    $this->$method($param);
                }

            } else {
                $param = $this->db->quote($param);
                if ($this->call_by == 'func') {
                    $this->where($method . '="' . $param . '"');
                } elseif ($this->call_by == 'smarty') {
                    if (strpos($param, '$') === false) {
                        $this->where($method . "='" . $param . "'");
                    } else {
                        $this->where($method . "='{" . $param . "}'");
                    }

                } else {
                    Error::info('Error: SelectDB 错误的参数', "<pre>参数$method=$param</pre>");
                }

            }
        }
    }
    /**
     * 获取记录
     * @param $field
     * @param $cache_id
     * @return unknown_type
     */
    public function getone($field = '')
    {
        $this->limit('1');
        if ($this->auto_cache or !empty($cache_id)) {
            $cache_key = empty($cache_id) ? $this->cache_prefix . '_one_' . md5($this->sql) : $this->cache_prefix . '_all_' . $cache_id;
            global $php;
            $record = $php->cache->get($cache_key);
            if (empty($data)) {
                if ($this->is_execute == 0) {
                    $this->exeucte();
                }
                $record = $this->result->fetch();
                $php->cache->set($cache_key, $record, $this->cache_life);
            }
        } else {
            if ($this->is_execute == 0) {
                $this->exeucte();
            }
            $record = $this->result->fetch();
        }
        if ($field === '') {
            return $record;
        }

        return $record[$field];
    }

    protected function _execute()
    {
        if ($this->is_execute == 0) {
            $this->exeucte();
        }
        if ($this->result) {
            return $this->result->fetchall();
        } else {
            return false;
        }
    }

    /**
     * 获取所有记录
     * @return array | bool
     */
    public function getall()
    {
        //启用了Cache
        if ($this->cache_lifetime) {
            $this->getsql(false);
            $cache_key = $this->cache_prefix . '_all_' . md5($this->sql);
            $data      = \Swoole::$php->cache->get($cache_key);
            //Cache数据为空，从DB中拉取
            if (empty($data)) {
                $data = $this->_execute();
                \Swoole::$php->cache->set($cache_key, $data, $this->cache_lifetime);
                return $data;
            } else {
                return $data;
            }
        } else {
            return $this->_execute();
        }
    }

    /**
     * 获取当前条件下的记录数
     * @return int
     */
    public function count()
    {
        $sql = "SELECT COUNT({$this->count_fields}) AS c FROM {$this->table} {$this->join} {$this->where} {$this->union} {$this->group}";

        if ($this->cache_lifetime) {
            $this->getsql(false);
            $cache_key = $this->cache_prefix . '_count_' . md5($this->sql);
            $data      = \Swoole::$php->cache->get($cache_key);
            if ($data) {
                return $data;
            }
        }

        if ($this->if_union) {
            $sql = str_replace('{#union_select#}', "COUNT({$this->count_fields}) AS c", $sql);
            $c   = $this->db->query($sql)->fetchall();
            $cc  = 0;
            foreach ($c as $_c) {
                $cc += $_c['c'];
            }
            $count = intval($cc);
        } else {
            $_c = $this->db->query($sql);
            if ($_c === false) {
                return false;
            } else {
                $c = $_c->fetch();
            }
            $count = intval($c['c']);
        }

        if ($this->cache_lifetime and $count !== false) {
            \Swoole::$php->cache->set($cache_key, $count, $this->cache_lifetime);
        }
        return $count;
    }

    /**
     * 执行插入操作
     * 修改为PDO::prepare方式，原来的quote后再拼接sql会造成双引号问题，sql语法错误
     * @param $data
     * @return bool
     */
    public function insert($data)
    {
        $field  = '';
        $values = [];
        $params = [];
        foreach ($data as $key => $value) {
            $field .= "`{$key}`,";
            $values[]          = ":{$key}";
            $params[":{$key}"] = $value;
        }
        $field  = substr($field, 0, -1);
        $values = implode(',', $values);
        $stmp   = $this->db->prepare("INSERT INTO {$this->table} ($field) VALUES($values)");

        return $stmp->execute($params);
           
    }

    /**
     * 对符合当前条件的记录执行update
     * 修改为PDO::prepare方式，原来的quote后再拼接sql会造成双引号问题，sql语法错误
     * @param $data
     * @return bool
     */
    public function update($data)
    {
        $update = '';
        $params = [];
        foreach ($data as $key => $value) {
            if ($value != '' && $value{0} == '`') {
                $update = $update . "`$key`=$value,";
            } else {
                $update            = $update . "`$key`=:$key,";
                $params[":{$key}"] = $value;
            }
        }
        $update = substr($update, 0, -1);
        $stmp   = $this->db->prepare("UPDATE {$this->table} SET $update {$this->where} {$this->limit}");
        return $stmp->execute($params);
    }

    /**
     * 删除当前条件下的记录
     * @return bool
     */
    public function delete()
    {
        return $this->db->query("DELETE FROM {$this->table} {$this->where} {$this->limit}");
    }

    /**
     * 获取受影响的行数
     * @return int
     */
    public function rowCount()
    {
        return $this->db->getAffectedRows();
    }
}
