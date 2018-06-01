<?php
namespace BigQuery\Laravel;

trait BigQuerySqlTrait
{
    protected $query;
    protected $sql;
    
    /**
     * @param $builder
     * @return null|string|string[]
     */
    public function getSql($builder)
    {
        $this->builder = $builder;
        $this->query = $builder->getQuery();
        $this->sql = $builder->toSql();
        
        //Add db for main table
        $this->replaceTableSql($this->query->from);
        
        //Add db for joins table
        $this->addDbToJoins();
        
        //Replace where values
        $this->replaceWhereValues();
        
        return $this->sql;
    }
    
    protected function addDbToJoins()
    {
        if ($this->query->joins) {
            foreach ($this->query->joins as $join) {
                $this->replaceTableSql(explode(' ', $join->table)[0]);
            }
        }
    }
    
    protected function replaceWhereValues()
    {
        foreach ($this->builder->getBindings() as $binding) {
            $this->sql = preg_replace(
                '/\?/',
                is_numeric($binding) ? $binding : "'" . $binding . "'",
                $this->sql,
                1
            );
        }
    }
    
    protected function replaceTableSql($table)
    {
        $this->sql = str_replace(
            '`' . $table . '`',
            '`' . $this->defaultDataset . '.' . $table . '`',
            $this->sql
        );
    }
}