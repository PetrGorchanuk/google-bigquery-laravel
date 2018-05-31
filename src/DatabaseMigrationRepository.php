<?php

namespace BigQuery\Laravel;

trait DatabaseMigrationRepository
{
    /**
     * The database connection resolver instance.
     *
     * @var \Illuminate\Database\ConnectionResolverInterface
     */
    protected $resolver;
    
    /**
     * The name of the migration table.
     *
     * @var string
     */
    protected $table;
    
    /**
     * The name of the database connection to use.
     *
     * @var string
     */
    protected $connection;
    
    /**
     * Get the completed migrations.
     *
     * @return array
     */
    public function getRan()
    {
        $query = resolve('BigQuery');
        $getMigrations = $query->get(
            'SELECT migration, batch
             FROM '.$query->defaultDataset.'.migrations
             ORDER BY migration ASC, batch ASC'
        );
    
        $migrations = [];
        foreach ($getMigrations as $k => $migrate) {
            $migrations[$k] = $migrate['migration'];
        }
    
        return $migrations;
    }
    
    /**
     * Get list of migrations.
     *
     * @param  int  $steps
     * @return array
     */
    public function getMigrations($steps)
    {
//        $query = resolve('BigQuery');
//        $getMigrations = $query->get(
//            'SELECT migration, batch
//             FROM '.$query->defaultDataset.'.migrations
//             ORDER BY migration ASC, batch ASC'
//        );
        
        $query = $this->table()->where('batch', '>=', '1')
            ->orderBy('batch', 'desc')
            ->orderBy('migration', 'desc')
            ->take($steps)->get()->all();
        
        dd($query);
    }
    
    /**
     * Get the last migration batch.
     *
     * @return array
     */
    public function getLast()
    {
        $query = resolve('BigQuery');
        $getMigrations = $query->get(
            'SELECT migration, batch
             FROM '.$query->defaultDataset.'.migrations
             WHERE batch = '.$this->getLastBatchNumber().'
             ORDER BY migration DESC'
        );
        
        return array_values((array)$getMigrations)[0];
    }
    
    /**
     * Get the completed migrations with their batch numbers.
     *
     * @return array
     */
    public function getMigrationBatches()
    {
        return $this->table()
            ->orderBy('batch', 'asc')
            ->orderBy('migration', 'asc')
            ->pluck('batch', 'migration')->all();
    }
    
    /**
     * Log that a migration was run.
     *
     * @param  string  $name
     * @param  int  $batch
     * @return void
     */
    public function log($name, $batch)
    {
        resolve('BigQuery')
            ->insert('migrations', [
                [
                    'data' => [
                        'migration' => $name,
                        'batch' => $batch,
                    ],
                ],
            ]);
    }
    
    /**
     * Remove a migration from the log.
     *
     * @param  object  $migration
     * @return void
     */
    public function delete($migration)
    {
        $query = resolve('BigQuery');
        
        $query->delete(
            'DELETE FROM '.$query->defaultDataset.'.migrations WHERE migration = "'.$migration['migration'].'"'
        );
    }
    
    /**
     * Get the next migration batch number.
     *
     * @return int
     */
    public function getNextBatchNumber()
    {
        return $this->getLastBatchNumber() + 1;
    }
    
    /**
     * Get the last migration batch number.
     *
     * @return int
     */
    public function getLastBatchNumber()
    {
        $query = resolve('BigQuery');
        $getMigrations = $query->get(
            'SELECT MAX(batch) as batch
             FROM '.$query->defaultDataset.'.migrations'
        );
        
        return $getMigrations[0]['batch'];
    }
    
    /**
     * Create the migration repository data store.
     *
     * @return void
     */
    public function createRepository()
    {
        $schema = $this->getConnection()->getSchemaBuilder();
        
        $schema->create($this->table, function ($table) {
            // The migrations table is responsible for keeping track of which of the
            // migrations have actually run for the application. We'll create the
            // table to hold the migration file's path as well as the batch ID.
            $table->increments('id');
            $table->string('migration');
            $table->integer('batch');
        });
    }
    
    /**
     * Determine if the migration repository exists.
     *
     * @return bool
     */
    public function repositoryExists()
    {
        $schema = $this->getConnection()->getSchemaBuilder();
        
        return $schema->hasTable($this->table);
    }
    
    /**
     * Get a query builder for the migration table.
     *
     * @return \Illuminate\Database\Query\Builder
     */
    protected function table()
    {
        return 'migrations';
    }
    
    /**
     * Get the connection resolver instance.
     *
     * @return \Illuminate\Database\ConnectionResolverInterface
     */
    public function getConnectionResolver()
    {
        return $this->resolver;
    }
    
    /**
     * Resolve the database connection instance.
     *
     * @return \Illuminate\Database\Connection
     */
    public function getConnection()
    {
        return $this->resolver->connection($this->connection);
    }
    
    /**
     * Set the information source to gather data.
     *
     * @param  string  $name
     * @return void
     */
    public function setSource($name)
    {
        $this->connection = $name;
    }
}
