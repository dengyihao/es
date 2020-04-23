<?php

namespace CrCms\ElasticSearch;

use Elasticsearch\Client;
use Illuminate\Support\Collection;
use RuntimeException;
use stdClass;

/**
 * Class Builder
 *
 * @package CrCms\ElasticSearch
 * @author simon
 */
class Builder
{
    /**
     * @var array
     */
    public $wheres = [];

    /**
     * @var array
     */
    public $columns = [];

    /**
     * @var null
     */
    public $offset = null;

    /**
     * @var null
     */
    public $limit = null;

    /**
     * @var array
     */
    public $orders = [];

    /**
     * @var array
     */
    public $aggs = [];

    public $buildAggs = [];

    /**
     * @var string
     */
    public $index = '';

    /**
     * @var string
     */
    public $type = '';

    /**
     * @var string
     */
    public $scroll = '';

    /**
     * @var array
     */
    public $operators = [
        '=' => 'eq',
        '>' => 'gt',
        '>=' => 'gte',
        '<' => 'lt',
        '<=' => 'lte',
    ];

    /**
     * @var Grammar|null
     */
    protected $grammar = null;

    /**
     * @var \Elasticsearch\Client|null
     */
    protected $elastisearch = null;

    /**
     * @var array
     */
    protected $queryLogs = [];

    /**
     * @var bool
     */
    protected $enableQueryLog = false;

    /**
     * @var array
     */
    protected $config = [];

    /**
     * Builder constructor.
     */
    public function __construct(array $config, Grammar $grammar, Client $client)
    {
        $this->config = $config;
        $this->setGrammar($grammar);
        $this->setElasticSearch($client);
        $this->setDefault();
    }

    /**
     * @return void
     */
    protected function setDefault()
    {
        if (!empty($this->config['index'])) {
            $this->index = $this->config['index'];
        }

        if (!empty($this->config['type'])) {
            $this->type = $this->config['type'];
        }
    }

    /**
     * @param $index
     * @return Builder
     */
    public function index($index)
    {
        $this->index = $index;

        return $this;
    }

    /**
     * @param $type
     * @return Builder
     */
    public function type($type)
    {
        $this->type = $type;

        return $this;
    }

    /**
     * @param int $value
     * @return Builder
     */
    public function limit(int $value)
    {
        $this->limit = $value;

        return $this;
    }

    /**
     * @param int $value
     * @return Builder
     */
    public function take(int $value)
    {
        return $this->limit($value);
    }

    /**
     * @param int $value
     * @return Builder
     */
    public function offset(int $value)
    {
        $this->offset = $value;

        return $this;
    }

    /**
     * @param int $value
     * @return Builder
     */
    public function skip(int $value)
    {
        return $this->offset($value);
    }

    /**
     * @param string $field
     * @param $sort
     * @return Builder
     */
    public function orderBy($field, $sort)
    {
        $this->orders[$field] = $sort;

        return $this;
    }

    /**
     * @param $field
     * @param $type
     * @return Builder
     */
    public function aggBy($field, $type, $alias = '')
    {
        if (is_array($field)) {
            $this->aggs[] = $field;
        } else if (!empty($alias)) {
            $this->aggs[] = ['alias' => $alias, 'type' => $type, 'field' => $field];
        } else {
            $this->aggs[] = ['field' => $field, 'type' => $type];
        }

        return $this;
    }

    /**
     * @param string $scroll
     * @return Builder
     */
    public function scroll(string $scroll)
    {
        $this->scroll = $scroll;

        return $this;
    }

    /**
     * @param $columns
     * @return Builder
     */
    public function select($columns)
    {
        $this->columns = is_array($columns) ? $columns : func_get_args();

        return $this;
    }

    /**
     * @param $field
     * @param $value
     * @param string $boolean
     * @return Builder
     */
    public function whereMatch($field, $value, $boolean = 'and')
    {
        return $this->where($field, '=', $value, 'match', $boolean);
    }

    /**
     * @param $field
     * @param $value
     * @param string $boolean
     * @return Builder
     */
    public function orWhereMatch($field, $value, $boolean = 'and')
    {
        return $this->whereMatch($field, $value, $boolean);
    }


    /**
     * @param $field
     * @param $value
     * @param string $boolean
     * @return Builder
     */
    public function whereTerm($field, $value, $boolean = 'and')
    {
        return $this->where($field, '=', $value, 'term', $boolean);
    }

    /**
     * @param $field
     * @param array $value
     * @return Builder
     */
    public function whereIn($field, array $value)
    {
        return $this->where(function (Builder $query) use ($field, $value) {
            array_map(function ($item) use ($query, $field) {
                $query->orWhereTerm($field, $item);
            }, $value);
        });
    }

    /**
     * @param $field
     * @param array $value
     * @return Builder
     */
    public function orWhereIn($field, array $value)
    {
        return $this->orWhere(function (Builder $query) use ($field, $value) {
            array_map(function ($item) use ($query, $field) {
                $query->orWhereTerm($field, $item);
            }, $value);
        });
    }

    /**
     * @param $field
     * @param $value
     * @param string $boolean
     * @return Builder
     */
    public function orWhereTerm($field, $value, $boolean = 'or')
    {
        return $this->whereTerm($field, $value, $boolean);
    }

    /**
     * @param $field
     * @param null $operator
     * @param null $value
     * @param string $boolean
     * @return Builder
     */
    public function whereRange($field, $operator = null, $value = null, $boolean = 'and')
    {
        return $this->where($field, $operator, $value, 'range', $boolean);
    }

    /**
     * @param $field
     * @param null $operator
     * @param null $value
     * @return Builder
     */
    public function orWhereRange($field, $operator = null, $value = null)
    {
        return $this->where($field, $operator, $value, 'or');
    }

    /**
     * @param $field
     * @param array $values
     * @param string $boolean
     * @return Builder
     */
    public function whereBetween($field, array $values, $boolean = 'and')
    {
        return $this->where($field, null, $values, 'range', $boolean);
    }

    /**
     * @param $field
     * @param array $values
     * @return Builder
     */
    public function orWhereBetween($field, array $values)
    {
        return $this->whereBetween($field, $values, 'or');
    }

    /**
     * @param $column
     * @param null $operator
     * @param null $value
     * @param string $leaf
     * @param string $boolean
     * @return Builder
     */
    public function where($column, $operator = null, $value = null, $leaf = 'term', $boolean = 'and')
    {
        if ($column instanceof \Closure) {
            return $this->whereNested($column, $boolean);
        }

        if (func_num_args() === 2) {
            list($value, $operator) = [$operator, '='];
        }

        if (is_array($operator)) {
            list($value, $operator) = [$operator, null];
        }

        if ($operator !== '=') {
            $leaf = 'range';
        }

        if (is_array($value) && $leaf === 'range') {
            $value = [
                $this->operators['>='] => $value[0],
                $this->operators['<='] => $value[1],
            ];
        }

        $type = 'Basic';

        $operator = $operator ? $this->operators[$operator] : $operator;

        $this->wheres[] = compact(
            'type', 'column', 'leaf', 'value', 'boolean', 'operator'
        );

        return $this;
    }

    /**
     * @param $field
     * @param null $operator
     * @param null $value
     * @param string $leaf
     * @return Builder
     */
    public function orWhere($field, $operator = null, $value = null, $leaf = 'term')
    {
        if (func_num_args() === 2) {
            list($value, $operator) = [$operator, '='];
        }

        return $this->where($field, $operator, $value, $leaf, 'or');
    }

    /**
     * @param \Closure $callback
     * @param $boolean
     * @return Builder
     */
    public function whereNested(\Closure $callback, $boolean)
    {
        $query = $this->newQuery();

        call_user_func($callback, $query);

        return $this->addNestedWhereQuery($query, $boolean);
    }

    /**
     * @return static
     */
    public function newQuery()
    {
        return new static($this->config, $this->grammar, $this->elastisearch);
    }

    /**
     * @return stdClass|null
     */
    public function first()
    {
        $this->limit = 1;

        $results = $this->runQuery($this->grammar->compileSelect($this));

        return $this->metaData($results)->first();
    }

    /**
     * @return Collection
     */
    public function get()
    {
        $results = $this->runQuery($this->grammar->compileSelect($this));
        return $this->metaData($results);
    }

    /**
     * @date 2018/11/13 2:09 PM
     * @author ChenRenhuan
     * @introduction 获取原始返回数据
     * @return mixed
     */
    public function getRaw()
    {
        return $this->runQuery($this->grammar->compileSelect($this));
    }

    /**
     * @date 2018/11/6 3:16 PM
     * @author ChenRenhuan
     * @introduction 返回聚合查询结果
     * @return Collection
     */
    public function getAggs()
    {
        $results = $this->runQuery($this->grammar->compileSelect($this, true));
        $isAgg = array_key_exists('aggregations', $results);
        if ($isAgg) {
            $ret = $results['aggregations'];
            return collect($ret);
        }
        return collect([]);
    }

    /**
     * @param int $page
     * @param int $perPage
     * @return Collection
     */
    public function paginate(int $page, $perPage = 15)
    {
        $from = (($page * $perPage) - $perPage);

        if (empty($this->offset)) {
            $this->offset = $from;
        }

        if (empty($this->limit)) {
            $this->limit = $perPage;
        }

        $results = $this->runQuery($this->grammar->compileSelect($this));

        $data = collect($results['hits']['hits'])->map(function ($hit) {
            return (object)array_merge($hit['_source'], ['_id' => $hit['_id']]);
        });

        $maxPage = intval(ceil($results['hits']['total'] / $perPage));
        return collect([
            'total' => $results['hits']['total'],
            'per_page' => $perPage,
            'current_page' => $page,
            'next_page' => $page < $maxPage ? $page + 1 : $maxPage,
            //'last_page' => $maxPage,
            'total_pages' => $maxPage,
            'from' => $from,
            'to' => $from + $perPage,
            'data' => $data
        ]);
    }

    /**
     * @param $id
     * @return null|object
     */
    public function byId($id)
    {
        //$query = $this->newQuery();

        $result = $this->runQuery(
            $this->whereTerm('_id', $id)->getGrammar()->compileSelect($this)
        );

        return isset($result['hits']['hits'][0]) ?
            $this->sourceToObject($result['hits']['hits'][0]) :
            null;
    }

    /**
     * @param $id
     * @return stdClass
     */
    public function byIdOrFail($id)
    {
        $result = $this->byId($id);

        if (empty($result)) {
            throw new RuntimeException('Resource not found');
        }

        return $result;
    }

    /**
     * @param callable $callback
     * @param int $limit
     * @param string $scroll
     * @return bool
     */
    public function chunk(callable $callback, $limit = 2000, $scroll = '10m')
    {
        if (empty($this->scroll)) {
            $this->scroll = $scroll;
        }

        if (empty($this->limit)) {
            $this->limit = $limit;
        }

        $results = $this->runQuery($this->grammar->compileSelect($this), 'search');

        if ($results['hits']['total'] === 0) {
            return null;
        }

        $total = $this->limit;
        $whileNum = intval(floor($results['hits']['total'] / $this->limit));

        do {
            if (call_user_func($callback, $this->metaData($results)) === false) {
                return false;
            }

            $results = $this->runQuery(['scroll_id' => $results['_scroll_id'], 'scroll' => $this->scroll], 'scroll');

            $total += count($results['hits']['hits']);
        } while ($whileNum--);
    }

    /**
     * @param array $data
     * @param null $id
     * @param string $key
     * @return stdClass
     */
    public function create(array $data, $id = null, $key = 'id')
    {
        $id = $id ? $id : isset($data[$key]) ? $data[$key] : uniqid();

        $result = $this->runQuery(
            $this->grammar->compileCreate($this, $id, $data),
            'create'
        );

        if (!isset($result['result']) || $result['result'] !== 'created') {
            throw new RunTimeException('Create params: ' . json_encode($this->getLastQueryLog()));
        }

        $data['_id'] = $id;
        return (object)$data;
    }

    /**
     * @param $id
     * @param array $data
     * @return bool
     */
    public function update($id, array $data)
    {
        $result = $this->runQuery($this->grammar->compileUpdate($this, $id, $data), 'update');

        if (!isset($result['result']) || $result['result'] !== 'updated') {
            throw new RunTimeException('Update error params: ' . json_encode($this->getLastQueryLog()));
        }

        return true;
    }

    /**
     * @param $id
     * @return bool
     */
    public function delete($id)
    {
        $result = $this->runQuery($this->grammar->compileDelete($this, $id), 'delete');

        if (!isset($result['result']) || $result['result'] !== 'deleted') {
            throw new RunTimeException('Delete error params:' . json_encode($this->getLastQueryLog()));
        }

        return true;
    }

    /**
     * @date 2018/11/7 8:25 PM
     * @author ChenRenhuan 更具条件删除
     * @introduction
     * @return bool
     */
    public function remove()
    {
        $results = $this->runQuery($this->grammar->compileDeleteQuery($this), 'deleteByQuery');
        if (isset($results['failures']) && !empty($results['failures'])) {
            return false;
        }
        return true;
    }

    public function save($data)
    {
        $results = $this->runQuery($this->grammar->compileUpdateQuery($this, $data), 'updateByQuery');
        return $results;
        if (isset($results['failures']) && !empty($results['failures'])) {
            return false;
        }
        return true;
    }

    /**
     * @return int
     */
    public function count()
    {
        $result = $this->runQuery($this->grammar->compileSelect($this), 'count');
        return $result['count'];
    }

    /**
     * @return Grammar|null
     */
    public function getGrammar()
    {
        return $this->grammar;
    }

    /**
     * @param Grammar $grammar
     * @return $this
     */
    public function setGrammar(Grammar $grammar)
    {
        $this->grammar = $grammar;

        return $this;
    }

    /**
     * @param Client $client
     * @return $this
     */
    public function setElasticSearch(Client $client)
    {
        $this->elastisearch = $client;

        return $this;
    }

    /**
     * @return Client|null
     */
    public function getElasticSearch()
    {
        return $this->elastisearch;
    }

    /**
     * @return Builder
     */
    public function enableQueryLog()
    {
        $this->enableQueryLog = true;

        return $this;
    }

    /**
     * @return Builder
     */
    public function disableQueryLog()
    {
        $this->enableQueryLog = false;

        return $this;
    }

    /**
     * @return array
     */
    public function getQueryLog()
    {
        return $this->queryLogs;
    }

    /**
     * @return array
     */
    public function getLastQueryLog()
    {
        return empty($this->queryLogs) ? '' : end($this->queryLogs);
    }

    /**
     * @return \Elasticsearch\Client
     */
    public function search()
    {
        return $this->elastisearch;
    }

    /**
     * @param array $params
     * @param string $method
     * @return mixed
     */
    protected function runQuery(array $params, $method = 'search')
    {
        if ($this->enableQueryLog) {
            $this->queryLogs[] = $params;
        }
        return call_user_func([$this->elastisearch, $method], $params);
    }

    /**
     * @param array $results
     * @return Collection
     */
    protected function metaData(array $results)
    {
        return collect($results['hits']['hits'])->map(function ($hit) {
            return $this->sourceToObject($hit);
        });
    }

    /**
     * @param array $result
     * @return object
     */
    protected function sourceToObject(array $result)
    {
        return (object)array_merge($result['_source'], ['_id' => $result['_id']]);
    }

    /**
     * @param $query
     * @param string $boolean
     * @return Builder
     */
    protected function addNestedWhereQuery($query, $boolean = 'and')
    {
        if (count($query->wheres)) {
            $type = 'Nested';
            $this->wheres[] = compact('type', 'query', 'boolean');
        }

        return $this;
    }

    /**
     * @date 2018/11/13 2:09 PM
     * @author ChenRenhuan
     * @introduction 短语查询
     * @param $field
     * @param $value
     * @param string $boolean
     * @return Builder
     */
    public function whereMatchPhrase($field, $value, $boolean = 'and')
    {
        return $this->where($field, '=', $value, 'match_phrase', $boolean);
    }

    /**
     * @date 2018/11/13 4:47 PM
     * @author ChenRenhuan
     * @introduction 不等于
     * @param $field
     * @param $value
     * @param string $boolean
     * @return $this|Builder
     */
    public function whereNot($field, $value, $boolean = 'and')
    {
        if ($boolean == 'or') {
            return $this->where($field, '=', $value, 'must_not', $boolean);
        } else {
            $type = 'must_not';
            $leaf = 'term';
            $column = $field;
            $this->wheres[] = compact('type', 'column', 'value', 'boolean', 'leaf');
            return $this;
        }
    }

    /**
     * @date 2018/11/13 3:18 PM
     * @author ChenRenhuan
     * @introduction 模糊匹配搜索
     * @param $field
     * @param $value
     * @param string $boolean
     * @return Builder
     */
    public function whereWildcard($field, $value, $boolean = 'and')
    {
        return $this->where($field, '=', '*' . $value . '*', 'wildcard', $boolean);
    }

    /**
     * @date 2018/11/14 10:23 AM
     * @author ChenRenhuan
     * @introduction 模糊搜索 例如: 搜索标题 为 '直播' 或者 '省考'
     * @param $field
     * @param $value
     * @param string $boolean
     * @return $this
     * @throws \Exception
     */
    public function whereWildcardIn($field, $value, $boolean = 'and')
    {
        if (!is_array($value)) {
            throw new \Exception('value 必须是数组');
        }
        $type = 'should';
        $leaf = 'wildcard';
        $column = $field;
        $value = array_map(function ($item) {
            return '*' . $item . '*';
        }, $value);
        $this->wheres[] = compact('type', 'column', 'value', 'boolean', 'leaf');
        return $this;
    }

    /**
     * @date 2018/11/14 10:36 AM
     * @author ChenRenhuan
     * @introduction 模糊匹配(不包含)
     * @param $field
     * @param $value
     * @param string $boolean
     * @return $this
     */
    public function whereNotWidcard($field, $value, $boolean = 'and')
    {
        $type = 'must_not';
        $leaf = 'wildcard';
        $column = $field;
        $value = '*' . $value . '*';
        $this->wheres[] = compact('type', 'column', 'value', 'boolean', 'leaf');
        return $this;
    }

    public function addAggs($field, $params)
    {
        $this->buildAggs[$field] = $params;
        return $this;
    }

    /**
     * @date 2018/11/14 4:46 PM
     * @author ChenRenhuan
     * @introduction 由于容器单例问题 reset重新实例化一个, 尽量少用这个
     * @return Builder
     */
    public function reset()
    {
        $es = (new self($this->config, $this->grammar, $this->elastisearch))->index($this->index)->type($this->type);
        return $es;
    }

    /**
     * @date 2019/1/17 11:47 AM
     * @author ChenRenhuan
     * @introduction 自定义查询, 高亮可选, 带分页
     * @param $params
     * @param bool $withHighlight
     * @return array
     */
    public function queryRaw($params, $withHighlight = false)
    {
        $results = $this->runQuery([
            'body' => $params,
            'index' => $this->index,
            'type' => $this->type,
			'preference' => '_primary'
        ]);
        $res = $this->metaData($results);
        $total = $results['hits']['total'];
        $highlights = [];
        if ($withHighlight) {
            foreach ($results['hits']['hits'] as $item) {
				 if (isset( $item['highlight'])) {
                    $highlights[$item['_source']['id']] = $item['highlight'];
                }
            }
        } else {
            return ['data' => $res, 'total' => $total];
        }
        $data = $res->map(function ($item) use ($highlights){
            if (array_key_exists($item->id, $highlights)) {
                $item->highlight = $highlights[$item->id];
            } else {
                $item->highlight = [];
            }
            return $item;
        });
        return ['total' => $total, 'data' => $data];
    }
}