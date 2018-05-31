<?php

namespace BigQuery\Laravel\Console\Commands\Migrations;

use Illuminate\Console\ConfirmableTrait;
use Illuminate\Console\Command;

class MigrateCommand extends Command
{
    use ConfirmableTrait, BaseCommand;
    
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'big_query_migrate
                {--force : Force the operation to run when in production.}
                {--path= : The path to the migrations files to be executed.}
                {--realpath : Indicate any provided migration file paths are pre-resolved absolute paths.}
                {--pretend : Dump the SQL queries that would be run.}
                {--seed : Indicates if the seed task should be re-run.}
                {--step : Force the migrations to be run so they can be rolled back individually.}';
    
    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Run the big query migrations';
    
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
        $bigQuery = resolve('BigQuery');
        
//        if (! $this->confirmToProceed()) {
//            return;
//        }
        
        $this->prepareDatabase($bigQuery);
        $this->prepareMigrationTable($bigQuery);
    
        // Next, we will check to see if a path option has been defined. If it has
        // we will use the path relative to the root of this installation folder
        // so that migrations may be run for any path within the applications.
        $this->migrator->run($this->getMigrationPaths(), [
            'pretend' => $this->option('pretend'),
            'step' => $this->option('step'),
        ]);
        
        // Once the migrator has run we will grab the note output and send it out to
        // the console screen, since the migrator itself functions without having
        // any instances of the OutputInterface contract passed into the class.
        foreach ($this->migrator->getNotes() as $note) {
            $this->output->writeln($note);
        }
        
        // Finally, if the "seed" option has been given, we will re-run the database
        // seed task to re-populate the database, which is convenient when adding
        // a migration and a seed at the same time, as it is only this command.
        if ($this->option('seed')) {
            $this->call('db:seed', ['--force' => true]);
        }
    }
    
    /**
     * Prepare the migration database for running.
     *
     * @param $config
     * @param $bigQuery
     * @return void
     */
    protected function prepareDatabase($bigQuery)
    {
        if (!$bigQuery->checkDataSetExist()) {
            $dataSet = $bigQuery->createDataSet();
            
            $this->info('Dataset '.$dataSet.' created.');
        }
    }
    
    /**
     * @param $config
     * @param $bigQuery
     */
    protected function prepareMigrationTable($bigQuery)
    {
        $bigQuery->createTable('migrations', [
            [
                'name' => 'migration',
                'type' => 'string',
                'mode' => 'required'
            ],
            [
                'name' => 'batch',
                'type' => 'integer'
            ],
        ]);
    }
}
