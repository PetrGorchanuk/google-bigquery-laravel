<?php

namespace BigQuery\Laravel\Console\Commands\Migrations;

use Illuminate\Console\ConfirmableTrait;
use Symfony\Component\Console\Input\InputOption;

use Illuminate\Console\Command;

class RollbackCommand extends Command
{
    use ConfirmableTrait, BaseCommand;

    /**
     * The console command name.
     *
     * @var string
     */
    protected $name = 'big_query_migrate:rollback';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Rollback the last database migration';

    /**
     * The migrator instance.
     *
     * @var \Illuminate\Database\Migrations\Migrator
     */
    protected $migrator;

    /**
     * Create a new migration rollback command instance.
     *
     * @param  \Illuminate\Database\Migrations\Migrator  $migrator
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
//        if (! $this->confirmToProceed()) {
//            return;
//        }

        $this->migrator->rollback($this->getMigrationPaths(), [
            'pretend' => $this->option('pretend'),
            'step' => (int) $this->option('step'),
        ]);

        // Once the migrator has run we will grab the note output and send it out to
        // the console screen, since the migrator itself functions without having
        // any instances of the OutputInterface contract passed into the class.
        foreach ($this->migrator->getNotes() as $note) {
            $this->output->writeln($note);
        }
    }

    /**
     * Get the console command options.
     *
     * @return array
     */
    protected function getOptions()
    {
        return [
            ['database', null, InputOption::VALUE_OPTIONAL, 'The database connection to use.'],

            ['force', null, InputOption::VALUE_NONE, 'Force the operation to run when in production.'],

            ['path', null, InputOption::VALUE_OPTIONAL, 'The path to the migrations files to be executed.'],

            ['realpath', null, InputOption::VALUE_NONE, 'Indicate any provided migration file paths are pre-resolved absolute paths.'],

            ['pretend', null, InputOption::VALUE_NONE, 'Dump the SQL queries that would be run.'],

            ['step', null, InputOption::VALUE_OPTIONAL, 'The number of migrations to be reverted.'],
        ];
    }
}
