<?php

namespace BigQuery\Laravel\Console\Commands\Migrations;

trait BaseCommand
{
    /**
     * Get all of the migration paths.
     *
     * @return array
     */
    protected function getMigrationPaths()
    {
        // Here, we will check to see if a path option has been defined. If it has we will
        // use the path relative to the root of the installation folder so our database
        // migrations may be run for any customized path from within the application.
        $mainPath = database_path('migrations/google_big_query');
        return array_merge(
            [$mainPath],
            glob($mainPath . '/*', GLOB_ONLYDIR)
        );
    }
}
