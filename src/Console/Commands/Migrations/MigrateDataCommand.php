<?php

namespace BigQuery\Laravel\Console\Commands\Migrations;

use Illuminate\Console\ConfirmableTrait;
use Illuminate\Console\Command;

class MigrateDataCommand extends Command
{
    use ConfirmableTrait, BaseCommand;
    
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'big_query_migrate:data';
    
    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Run the big query data migrations';
    
    /**
     * The migrator instance.
     *
     * @var \Illuminate\Database\Migrations\Migrator
     */
    protected $migrator;
    
    /**
     * Create a new migration command instance.
     *
     * @return void
     */
    public function __construct()
    {
        parent::__construct();
        
        $this->migrator = resolve('BigQueryMigrator');
    }
    
    /**
     * Execute the console command.
     *
     * @return void
     */
    public function handle()
    {
        $limit = 10000;
        $tables = array_map('reset', \DB::select('SHOW TABLES'));
        $googleTables = resolve('BigQuery')->getTables();

        foreach ($tables as $table) {
            $count = $data = \DB::table($table)->count();
            if (in_array($table, $googleTables) && $count > 0) {
                if ($count > $limit) {
                    $this->addByChunks($table, $count, $limit);
                } else {
                    $data = resolve('BigQuery')->insert($table, \DB::table($table)->get()->toArray());
                    var_dump($data);
                }
            }
        }
    }
    
    protected function addByChunks($table, $count, $limit)
    {
        $offset = 0;
        $steps = (int)round($count / $limit);
        for ($i = 1; $i <= $steps; $i++) {
            $data = resolve('BigQuery')
                ->insert($table, \DB::table($table)->limit($limit, $offset)->get()->toArray());
            var_dump($data, $offset);
        
            $offset = $offset + $limit;
        }
    }
}
