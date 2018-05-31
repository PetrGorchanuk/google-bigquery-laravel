<?php

namespace BigQuery\Laravel;

use Illuminate\Support\ServiceProvider;
use BigQuery\Laravel\Console\Commands\Migrations\MigrateCommand;
use BigQuery\Laravel\Console\Commands\Migrations\RollbackCommand;

class BigQueryServiceProvider extends ServiceProvider
{
    protected static $drivers = [
        'eloquent' => 'Eloquent',
        'big_query' => 'BigQuery'
    ];
    
    protected static $instances;
    
    public function boot()
    {
        $this->publishes([__DIR__ . '/../config/' => config_path('/'), 'config']);
        $this->publishes([__DIR__ . '/../migrations/' => database_path('/migrations'), 'migrations']);
        
        if ($this->app->runningInConsole()) {
            $this->commands([
                MigrateCommand::class,
                RollbackCommand::class,
            ]);
        }
    }
    
    public function register()
    {
        $this->app->singleton('BigQuery', function ($app) {
            return new BigQuery();
        });
        
        $this->app->singleton('BigQueryMigrator', function ($app) {
            return new Migrator($app['migration.repository'], $app['db'], $app['files']);
        });
    }
}
