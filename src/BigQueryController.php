<?php
namespace BigQuery\Laravel;

use App\Http\Controllers\Controller;
use Carbon\Carbon;

class BigQueryController extends Controller
{
    
    public function index($timezone)
    {
        echo Carbon::now($timezone)->toDateTimeString();
    }
    
}