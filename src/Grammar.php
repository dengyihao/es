<?php

namespace CrCms\ElasticSearch;

/**
 * Class Grammar
 *
 * @package CrCms\ElasticSearch
 * @author simon
 */
class Grammar
{
    /**
     * @var array
     */
    protected $selectComponents = [
        '_source' => 'columns',
        'query' => 'wheres',
        'aggs',
        'sort' => 'orders',
        'size' => 'limit',
        'from' => 'offset',
        'index' => 'index',
        'type' => 'type',
        'scroll' => 'scroll',
    ];

    /**
     * @param Builder $builder
     * @return int
     */
    public function compileOffset(Builder $builder)
    {
        return $builder->offset;
    }

    /**
     * @param Builder $builder
     * @return int
     */
    public function compileLimit(Builder $builder)
    {
        return $builder->limit;
    }

    /**
     * @param Builder $builder
     * @return string
     */
    public function compileScroll(Builder $builder)
    {
        return $builder->scroll;
    }

    /**
     * @param Builder $builder
     * @return array
     */
    public function compileSelect(Builder $builder, $agg = false)
    {
        $body = $this->compileComponents($builder);
        $index = array_pull($body, 'index');
        $type = array_pull($body, 'type');
        $scroll = array_pull($body, 'scroll');
        if ($agg) {
            $body['size'] = 0;
        }
        $params = ['body' => $body, 'index' => $index, 'type' => $type];
        if ($scroll) {
            $params['scroll'] = $scroll;
        }
//        echo json_encode($params) . "\n";
        return $params;
    }

    /**
     * @param Builder $builder
     * @param $id
     * @param array $data
     * @return array
     */
    public function compileCreate(Builder $builder, $id, array $data)
    {
        return array_merge([
            'id' => $id,
            'body' => $data
        ], $this->compileComponents($builder));
    }

    /**
     * @param Builder $builder
     * @param $id
     * @return array
     */
    public function compileDelete(Builder $builder, $id)
    {
        return array_merge([
            'id' => $id,
        ], $this->compileComponents($builder));
    }

    public function compileDeleteQuery(Builder $builder)
    {
        $body = $this->compileComponents($builder);
        $index = array_pull($body, 'index');
        $type = array_pull($body, 'type');
        $params = ['body' => $body, 'index' => $index, 'type' => $type];
        return $params;
    }

    /**
     * @param Builder $builder
     * @param $id
     * @param array $data
     * @return array
     */
    public function compileUpdate(Builder $builder, $id, array $data)
    {
        return array_merge([
            'id' => $id,
            'body' => ['doc' => $data, 'detect_noop' => false]
        ], $this->compileComponents($builder));
    }

    public function compileUpdateQuery(Builder $builder, $data)
    {

    }

    /**
     * @param Builder $builder
     * @return array
     */
    public function compileAggs(Builder $builder)
    {
        $aggs = [];

        foreach ($builder->aggs as $field => $aggItem) {
            if (is_array($aggItem)) {
                if (array_key_exists('alias', $aggItem)) {
                    $aggs[$aggItem['alias']] = [$aggItem['type'] => ['field' => $aggItem['field']]];
                } else if (array_key_exists('field', $aggItem)) {
                    $aggs[$aggItem['field'] . '_' . $aggItem['type']] = [$aggItem['type'] => ['field' => $aggItem['field']]];
                } else {
                    $aggs[] = $aggItem;
                }
            } else {
                $aggs[$field . '_' . $aggItem] = [$aggItem => ['field' => $field]];
            }
        }

        foreach ($builder->buildAggs as $field => $item) {
            $aggs[$field] = $item;
        }
        return $aggs;
    }

    /**
     * @param Builder $builder
     * @return array
     */
    public function compileColumns(Builder $builder)
    {
        return $builder->columns;
    }

    /**
     * @param Builder $builder
     * @return string
     */
    public function compileIndex(Builder $builder)
    {
        return is_array($builder->index) ? implode(',', $builder->index) : $builder->index;
    }

    /**
     * @param Builder $builder
     * @return string
     */
    public function compileType(Builder $builder)
    {
        return $builder->type;
    }

    /**
     * @param Builder $builder
     * @return array
     */
    public function compileOrders(Builder $builder)
    {
        $orders = [];

        foreach ($builder->orders as $field => $orderItem) {
            $orders[$field] = is_array($orderItem) ? $orderItem : ['order' => $orderItem];
        }

        return $orders;
    }

    /**
     * @param Builder $builder
     * @return array
     */
    protected function compileWheres(Builder $builder)
    {
        $whereGroups = $this->wherePriorityGroup($builder->wheres);

        $operation = count($whereGroups) === 1 ? 'filter' : 'should';

        $bool = [];
        foreach ($whereGroups as $wheres) {
            $must = [];
            $mustNot = [];
            $should = [];
            foreach ($wheres as $where) {
                if ($where['type'] === 'Nested') {
                    $must[] = $this->compileWheres($where['query']);
                } else if ($where['type'] === 'must_not') {
                    $mustNot[] = [$where['leaf'] => [$where['column'] => $where['value']]];
                } else if ($where['type'] === 'should') {
                    if (!empty($where['value'])) {
                        $shouldItem = [];
                        foreach ($where['value'] as $item) {
                            $shouldItem[] = $where;
                        }
                    }
                    $should[] = $shouldItem;
                } else {
                    $must[] = $this->whereLeaf($where['leaf'], $where['column'], $where['operator'], $where['value']);
                }
            }

            $filterBool = [];
            if (!empty($mustNot)) {
                $filterBool['must_not'] = $mustNot;
            }
            if (!empty($should)) {
                foreach ($should as $item) {
                    $ret = [];
                    foreach ($item as $subItem) {
                        $ret = array_map(function ($valueItem) use ($subItem) {
                            return [$subItem['leaf'] => [$subItem['column'] => $valueItem]];
                        }, $subItem['value']);
                    }
                    $must[] = ['bool' => ['should' => $ret]];
                }
            }
            if (!empty($must)) {
                $filterBool['filter'] = $must;
//                $bool['bool'][$operation][] = count($must) === 1 ? array_shift($must) : ['bool' => ['filter' => $must]];
            }
            if (!empty($filterBool)) {
                $bool['bool'][$operation][] = ['bool' => $filterBool];
            }
        }
        return $bool;
    }

    /**
     * @param string $leaf
     * @param string $column
     * @param string|null $operator
     * @param $value
     * @return array
     */
    protected function whereLeaf($leaf, $column, $operator = null, $value)
    {
        if (in_array($leaf, ['term', 'match', 'match_phrase', 'wildcard', 'must_not'], true)) {
            return [$leaf => [$column => $value]];
        } elseif ($leaf === 'range') {
            return [$leaf => [
                $column => is_array($value) ? $value : [$operator => $value]
            ]];
        }
    }

    /**
     * @param array $wheres
     * @return array
     */
    protected function wherePriorityGroup(array $wheres)
    {
        //get "or" index from array
        $orIndex = (array)array_keys(array_map(function ($where) {
            return $where['boolean'];
        }, $wheres), 'or');

        $lastIndex = $initIndex = 0;
        $group = [];
        foreach ($orIndex as $index) {
            $group[] = array_slice($wheres, $initIndex, $index - $initIndex);
            $initIndex = $index;
            $lastIndex = $index;
        }

        $group[] = array_slice($wheres, $lastIndex);

        return $group;
    }

    /**
     * @param Builder $query
     * @return array
     */
    protected function compileComponents(Builder $query)
    {
        $body = [];

        foreach ($this->selectComponents as $key => $component) {
            if ($key == 'aggs') {
                $bool = !empty($query->aggs) || !empty($query->buildAggs);
            } else {
                $bool = !empty($query->$component);
            }
            if ($bool) {
                $method = 'compile' . ucfirst($component);

                $body[is_numeric($key) ? $component : $key] = $this->$method($query, $query->$component);
            }
        }
        return $body;
    }
}