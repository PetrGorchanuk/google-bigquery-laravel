<?php

use Illuminate\Database\Migrations\Migration;

class CreateUsersTable extends Migration
{
    /**
     * Run the migrations.
     *
     * @return void
     */
    public function up()
    {
        resolve('BigQuery')
            ->createTable('users', [
                [
                    'name' => 'username',
                    'type' => 'string',
                    'mode' => 'required'
                ],
                [
                    'name' => 'password',
                    'type' => 'string',
                    'mode' => 'required'
                ],
                [
                    'name' => 'auth_token',
                    'type' => 'string'
                ],
                [
                    'name' => 'remember_token',
                    'type' => 'string'
                ],
                [
                    'name' => 'created_at',
                    'type' => 'datetime'
                ],
                [
                    'name' => 'updated_at',
                    'type' => 'datetime'
                ],
            ]);
    }
    
    /**
     * Reverse the migrations.
     *
     * @return void
     */
    public function down()
    {
        resolve('BigQuery')->deleteTable('users');
    }
}
