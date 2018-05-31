<?php

namespace BigQuery\Laravel;

use Illuminate\Support\Arr;
use Illuminate\Database\Migrations\Migrator as Base;

class Migrator extends Base
{
    use DatabaseMigrationRepository;
    
    /**
     * @param array $paths
     * @param array $options
     * @return array
     */
    public function run($paths = [], array $options = [])
    {
        // Once we grab all of the migration files for the path, we will compare them
        // against the migrations that have already been run for this package then
        // run each of the outstanding migrations against a database connection.
        $migrations = $this->pendingMigrations(
            $this->getMigrationFiles($paths),
            $this->getRan()
        );
        
        $this->requireFiles($migrations);
        
        // Once we have all these migrations that are outstanding we are ready to run
        // we will go ahead and run them "up". This will execute each migration as
        // an operation against a database. Then we'll return this list of them.
        $this->runPending($migrations, $options);
        
        return $migrations;
    }
    
    /**
     * @param string $file
     * @param int $batch
     * @param bool $pretend
     */
    protected function runUp($file, $batch, $pretend)
    {
        // First we will resolve a "real" instance of the migration class from this
        // migration file name. Once we have the instances we can run the actual
        // command such as "up" or "down", or we can just simulate the action.
        $migration = $this->resolve(
            $name = $this->getMigrationName($file)
        );

        if ($pretend) {
            return $this->pretendToRun($migration, 'up');
        }

        $this->note("<comment>Migrating:</comment> {$name}");

        $this->runMigration($migration, 'up');

        //This condition is added for ignore record about migration
        if (isset($migration->log) ? $migration->log : true) {
            // Once we have run a migrations class, we will log that it was run in this
            // repository so that we don't try to run it next time we do a migration
            // in the application. A migration repository keeps the migrate order.
            $this->log($name, $batch);
        }

        $this->note("<info>Migrated:</info>  {$name}");
    }
    
    /**
     * Rollback the last migration operation.
     *
     * @param  array|string $paths
     * @param  array  $options
     * @return array
     */
    public function rollback($paths = [], array $options = [])
    {
        $this->notes = [];
        
        // We want to pull in the last batch of migrations that ran on the previous
        // migration operation. We'll then reverse those migrations and run each
        // of them "down" to reverse the last migration "operation" which ran.
        $migrations = $this->getMigrationsForRollback($options);
        
        //dd($migrations); die;
        
        if (count($migrations) === 0) {
            $this->note('<info>Nothing to rollback.</info>');
            
            return [];
        }
        
        return $this->rollbackMigrations($migrations, $paths, $options);
    }
    
    /**
     * Get the migrations for a rollback operation.
     *
     * @param  array  $options
     * @return array
     */
    protected function getMigrationsForRollback(array $options)
    {
        if (($steps = $options['step'] ?? 0) > 0) {
            return $this->getMigrations($steps);
        }

        return $this->getLast();
    }
    
    /**
     * Rollback the given migrations.
     *
     * @param  array  $migrations
     * @param  array|string  $paths
     * @param  array  $options
     * @return array
     */
    protected function rollbackMigrations(array $migrations, $paths, array $options)
    {
        $rolledBack = [];
        
        $this->requireFiles($files = $this->getMigrationFiles($paths));
        
        // Next we will run through all of the migrations and call the "down" method
        // which will reverse each migration in order. This getLast method on the
        // repository already returns these migration's names in reverse order.
        foreach ($migrations as $migration) {
            if (! $file = Arr::get($files, $migration['migration'])) {
                $this->note("<fg=red>Migration not found:</> {$migration['migration']}");
                
                continue;
            }
            
            $rolledBack[] = $file;
            
            $this->runDown(
                $file,
                $migration,
                $options['pretend'] ?? false
            );
        }
        
        return $rolledBack;
    }
    
    /**
     * Run "down" a migration instance.
     *
     * @param  string  $file
     * @param  object  $migration
     * @param  bool    $pretend
     * @return void
     */
    protected function runDown($file, $migration, $pretend)
    {
        // First we will get the file name of the migration so we can resolve out an
        // instance of the migration. Once we get an instance we can either run a
        // pretend execution of the migration or we can run the real migration.
        $instance = $this->resolve(
            $name = $this->getMigrationName($file)
        );
        
        $this->note("<comment>Rolling back:</comment> {$name}");
        
        if ($pretend) {
            return $this->pretendToRun($instance, 'down');
        }
        
        $this->runMigration($instance, 'down');
        
        // Once we have successfully run the migration "down" we will remove it from
        // the migration repository so it will be considered to have not been run
        // by the application then will be able to fire by any later operation.
        $this->delete($migration);
        
        $this->note("<info>Rolled back:</info>  {$name}");
    }
}
