<?php
namespace BigQuery\Laravel;

use Carbon\Carbon;
use Exception;
use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\BigQuery\Table;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\DB;

class BigQuery
{
    /**
     * @var BigQueryClient
     */
    public $db;
    
    /**
     * @database config
     */
    public $config;
    
    /**
     * @var array
     */
    public $options;
    
    /**
     * @var string
     */
    public $defaultDataset;
    
    /**
     * Setup our Big wrapper with Google's BigQuery service
     */
    public function __construct()
    {
        if (is_null($this->db)) {
            $configPath = base_path('config/google_big_query.json');
            $config = json_decode(\File::get($configPath), true);
    
            $this->db = new BigQueryClient([
                'keyFilePath' => $configPath,
                'projectId' => $config['project_id']
            ]);
            
            $this->config = config('google_big_query.google_big_query');
            $this->defaultDataset = $this->config['database'];
        }
    }
    
    /**
     * @param $dataSet
     * @return \Google\Cloud\BigQuery\Dataset
     */
    public function dataSet($dataSet = null)
    {
        return $this->db->dataset($this->getDataSet($dataSet));
    }
    
    /**
     * @param $dataSet
     * @return mixed
     */
    public function createDataSet($dataSet)
    {
        $name = $this->getDataSet($dataSet);
        $this->db->createDataset($name);
        
        return $name;
    }
    
    /**
     * @param $table
     * @param $fields
     * @param $dataSet
     * @return Table
     */
    public function createTable($table, $fields, $dataSet = null)
    {
        try {
            $dataSet = $this->dataSet($dataSet);
    
            $dataSet->createTable($table, [
                'id' => 'string',
                'streamingBuffer' => false,
                'schema' => [
                    'fields' => array_merge(
                        [
                            [
                                'name' => 'id',
                                'type' => 'integer',
                                'mode' => 'required'
                            ]
                        ],
                        $fields
                    )
                ]
            ]);
        } catch (\Exception $e) {
            throw $this->getError($e);
        }
    }
    
    /**
     * @param $table
     * @param $dataSet
     * @return Table
     */
    public function deleteTable($table, $dataSet = null)
    {
        try {
            $dataSet = $this->dataSet($dataSet);
            $table = $dataSet->table($table);
            $table->delete();
        } catch (\Exception $e) {
            return $this->getError($e);
        }
    }
    
    /**
     * @param $e
     * @return mixed
     */
    protected function getError($e)
    {
        return json_decode($e->getMessage(), true);
    }
    
    /**
     * @return array
     */
    public function getDataSets()
    {
        $list = [];
        $dataSets = $this->db->datasets();
        foreach ($dataSets as $dataSet) {
            $list[] = $dataSet->id();
        }
        
        return $list;
    }
    
    /**
     * Check on exist dataSet
     *
     * @param $dataSet
     * @return bool
     */
    public function checkDataSetExist($dataSet = null)
    {
        return in_array($this->getDataSet($dataSet), $this->getDataSets());
    }
    
    /**
     * @param $builder
     * @return null|string|string[]
     */
    protected function getSql($builder)
    {
        $table = $builder->getQuery()->from;
        
        $sql = str_replace($table, $this->defaultDataset . '.' . $table, $builder->toSql());
        foreach ($builder->getBindings() as $binding) {
            $sql = preg_replace(
                '/\?/',
                is_numeric($binding) ? $binding : "'" . $binding . "'",
                $sql,
                1
            );
        }
        
        return $sql;
    }
    
    /**
     * Wrap around Google's BigQuery run method and handle results
     *
     * @param $builder
     * @param array $selectWith
     * @param array $options
     * @return \Illuminate\Support\Collection
     * @throws \Google\Cloud\Core\Exception\GoogleException
     */
    public function get($builder, $selectWith = [], $options = [])
    {
        return collect($this->getQuery($builder, $selectWith, $options));
    }
    
    
    /**
     * @param $builder
     * @param $selectWith
     * @param $options
     * @return array
     * @throws \Google\Cloud\Core\Exception\GoogleException
     */
    public function getQuery($builder, $selectWith, $options)
    {
        $aliases = $this->selectWitch($builder, $selectWith);
        
        // Set default options if nothing is passed in
        $queryResults = $this->db->runQuery(
            $this->db->query(
                is_string($builder)
                    ? $builder
                    : $this->getSql($builder)
            ),
            $options ?? $this->options
        );
    
        // Setup our result checks
        $isComplete = $queryResults->isComplete();
        while (!$isComplete) {
            sleep(.5); // let's wait for a moment...
            $queryResults->reload(); // trigger a network request
            $isComplete = $queryResults->isComplete(); // check the query's status
        }
    
        // Mutate into a laravel collection
        foreach ($queryResults->rows() as $row) {
            if (!empty($selectWith)) {
                $uid = $selectWith['uids'];
                foreach ($row as $key => $val) {
                    $alias = $aliases[$key];
                    if (empty($alias)) {
                        $data [$row[$uid[0]]] [$key] = $val;
                    } else {
                        $data [$row[$uid[0]]] [$alias] [$row[$uid[1]]] [$key] = $val;
                    }
                }
            } else {
                $data[] = $row;
            }
        }
    
        return $data ?? [];
    }
    
    /**
     * @param $builder
     * @param $selectWith
     * @return array
     */
    public function selectWitch($builder, $selectWith)
    {
        if (!empty($selectWith)) {
            $aliases = [];
            $select = [];
            foreach ($selectWith as $sk => $with) {
                if ($sk !== 'uids') {
                    $alias = explode('->', $sk);
    
                    array_walk(
                        $selectWith[$sk],
                        function ($val) use (&$select, $alias) {
                            $select[] = $alias[0] . '.' . $val;
                        }
                    );
    
                    foreach ($with as $field) {
                        $pref = explode(' as ', $field);
                        $pref = isset($pref[1]) ? $pref[1] : $pref[0];
        
                        $aliases[$pref] = isset($alias[1]) ? $alias[1] : '';
                    }
                }
            }
        
            $builder->select($select);

            return $aliases;
        }
    }
    
    /**
     *
     */
    public function rowArray()
    {
    
    }
    
    /**
     * Wrap around Google's BigQuery insert method
     *
     * @param Table $table
     * @param array $rows
     * @param array|null $options
     *
     * @return bool|array
     * @throws \Exception
     */
    public function insert($table, $rows, $options = null)
    {
        $dataSet = $this->db->dataset($this->config['database']);
        $dataSet = $dataSet->table($table);
        
        // Set default options if nothing is passed in
        $insertResponse = $dataSet->insertRows(
            $this->prepareData($rows, $this->getMaxId($table)),
            $options ?? ['ignoreUnknownValues' => true]
        );
        
        if (!$insertResponse->isSuccessful()) {
            foreach ($insertResponse->failedRows() as $row) {
                foreach ($row['errors'] as $error) {
                    $errors[] = $error;
                }
            }
    
            return $errors ?? [];
        }
    
        return true;
    }
    
    /**
     * @param $query
     * @return \Google\Cloud\BigQuery\QueryResults
     */
    public function delete($query)
    {
        return $this->db->runQuery($this->db->query($query));
    }
    
    
    /**
     * @param null $dataSet
     * @return array
     */
    public function getTables($dataSet = null)
    {
        $tables = $this->db->dataset($this->getDataSet($dataSet))->tables();
        
        $res = [];
        foreach ($tables as $table) {
            $res[] = $table->id();
        }
        
        return $res;
    }
    
    /**
     * @param \Illuminate\Database\Eloquent\Collection|\Illuminate\Support\Collection|array $data
     *
     * @return array
     */
    public function prepareData($data, $lastId)
    {
        $preparedData = [];
        // We loop our data and handle object conversion to an array
        foreach ($data as $item) {
            $lastId++;
            
            if (!is_array($item)) {
                $item = (array)$item;
            }
            $struct = [];
            // Handle nested array's as STRUCT<>
            foreach ($item as $field => $value) {
                // Map array's to STRUCT name/type
                if (is_array($value)) {
                    foreach ($value as $key => $attr) {
                        $struct[] = [
                            'name' => $key,
                            'type' => strtoupper(gettype($attr)),
                        ];
                    }
                }
            }
            
            if (!isset($item['id'])) {
                $item['id'] = $lastId;
            }

            // If we have an id column use Google's insertId
            // https://cloud.google.com/bigquery/streaming-data-into-bigquery#dataconsistency
            if (in_array('id', $item)) {
                $rowData = [
                    'insertId' => $item['id'],
                    'data' => $item,
                    'fields' => $struct,
                ];
            } else {
                $rowData = ['data' => $item];
            }
            // Set our struct definition if we have one
            if (!empty($struct)) {
                $rowData['fields'] = $struct;
            }
            $preparedData[] = $rowData;
        }
        
        return $preparedData;
    }
    
    /**
     * Wrapper function around the BigQuery create_table() function.
     * We also have the benefit of mutating a Laravel Eloquent Model into a proper field map for automation
     *
     * Example:
     * $fields = [
     *     [
     *         'name' => 'field1',
     *         'type' => 'string',
     *         'mode' => 'required'
     *     ],
     *     [
     *         'name' => 'field2',
     *         'type' => 'integer'
     *     ],
     * ];
     * $schema = ['fields' => $fields];
     * create_table($projectId, $datasetId, $tableId, $schema);
     *
     * @param string $datasetId
     * @param string $tableId
     * @param Model $model
     * @param array|null $structs
     * @param bool $useDelay
     *
     * @throws Exception
     * @return Table|null
     */
    public function createFromModel($datasetId, $tableId, $model, $structs = null, $useDelay = true)
    {
        // Check if we have this table
        $table = in_array($tableId, $this->getTables($datasetId));
        
        // If this table has been created, return it
        if ($table instanceof Table) {
            return $table;
        }
        
        // Generate a new dataset
        $dataset = $this->db->dataset($datasetId);
        // Flip our Eloquent model into a BigQuery schema map
        $options = ['schema' => static::flipModel($model, $structs)];
        // Create the table
        $table = $dataset->createTable($tableId, $options);
        
        // New tables are not instantly available, we will insert a delay to help the developer
        if ($useDelay) {
            sleep(10);
        }
        
        return $table;
    }
    
    /**
     * Flip a Laravel Eloquent Models into a Big Query Schemas
     *
     * @param Model $model
     * @param array|null $structs
     *
     * @throws Exception
     * @return array
     */
    public static function flipModel($model, $structs)
    {
        // Verify we have an Eloquent Model
        if (!$model instanceof Model) {
            throw new Exception(__METHOD__ . ' requires a Eloquent model, ' . get_class($model) . ' used.');
        }
        
        // Cache name based on table
        $cacheName = __CLASS__ . '.cache.' . $model->getTable();
        // Cache duration
        $liveFor = Carbon::now()->addDays(5);
        // Cache our results as these rarely change
        $fields = Cache::remember($cacheName, $liveFor, function () use ($model) {
            return DB::select('describe ' . $model->getTable());
        });
        
        // Loop our fields and return a Google BigQuery field map array
        return ['fields' => static::fieldMap($fields, $structs)];
    }
    
    /**
     * Map our fields to BigQuery compatible data types
     *
     * @param array $fields
     * @param array|null $structs
     *
     * @return array
     */
    public static function fieldMap($fields, $structs)
    {
        // Holders
        $map = [];
        // Loop our fields and map them
        foreach ($fields as $value) {
            // Compute short name for matching type
            $shortType = trim(explode('(', $value->Type)[0]);
            switch ($shortType) {
                // Custom handler
                case Types::TIMESTAMP:
                    $type = 'TIMESTAMP';
                    break;
                // Custom handler
                case Types::INT:
                    $type = 'INTEGER';
                    break;
                // Custom handler
                case Types::TINYINT:
                    $type = 'INTEGER';
                    break;
                case Types::BIGINT:
                    $type = 'INTEGER';
                    break;
                case Types::BOOLEAN:
                    $type = 'BOOLEAN';
                    break;
                case Types::DATE:
                    $type = 'DATETIME';
                    break;
                case Types::DATETIME:
                    $type = 'DATETIME';
                    break;
                case Types::DECIMAL:
                    $type = 'FLOAT';
                    break;
                case Types::FLOAT:
                    $type = 'FLOAT';
                    break;
                case Types::INTEGER:
                    $type = 'INTEGER';
                    break;
                case Types::SMALLINT:
                    $type = 'INTEGER';
                    break;
                case Types::TIME:
                    $type = 'TIME';
                    break;
                case Types::DOUBLE:
                    $type = 'FLOAT';
                    break;
                case Types::JSON:
                    // JSON data-types require a struct to be defined, here we check for developer hints or skip these
                    if (!empty($structs)) {
                        $struct = $structs[$value->Field];
                    } else {
                        continue 2;
                    }
                    $type = 'STRUCT';
                    break;
                default:
                    $type = 'STRING';
                    break;
            }
            // Nullable handler
            $mode = (strtolower($value->Null) === 'yes' ? 'NULLABLE' : 'REQUIRED');
            // Construct our BQ schema data
            $fieldData = [
                'name' => $value->Field,
                'type' => $type,
                'mode' => $mode,
            ];
            // Set our struct definition if we have one
            if (!empty($struct)) {
                $fieldData['fields'] = $struct;
                unset($struct);
            }
            $map[] = $fieldData;
        }
        
        // Return our map
        return $map;
    }
    
    /**
     * Return the max ID
     *
     * @param $table
     * @param null $dataSet
     * @return mixed
     * @throws \Google\Cloud\Core\Exception\GoogleException
     */
    public function getMaxId($table, $dataSet = null)
    {
        // Run our max ID query
        $id = $this->get('SELECT MAX(id) id FROM ' . $this->getDataSet($dataSet) . '.' . $table)->first()['id'];
        
        return $id ? $id : 0;
    }
    
    /**
     * Return the max created_at date
     *
     * @param $table
     * @param null $dataSet
     * @return mixed
     * @throws \Google\Cloud\Core\Exception\GoogleException
     */
    public function getMaxCreationDate($table, $dataSet = null)
    {
        // Run our max created_at query
        return $this->get(
            'SELECT max(created_at) created_at FROM `' . $this->getDataSet($dataSet) . '.' . $table . '`'
        )->first()['created_at'];
    }
    
    /**
     * Return the max of field
     *
     * @param $table
     * @param $field
     * @param null $dataSet
     * @return mixed
     * @throws \Google\Cloud\Core\Exception\GoogleException
     */
    public function getMaxField($table, $field, $dataSet = null)
    {
        // Run our max query
        return $this->get(
            'SELECT max(' . $field . ') ' . $field . ' FROM `' . $this->getDataSet($dataSet) . '.' . $table . '`'
        )->first()[$field];
    }
    
    /**
     * @param $dataSet
     * @return mixed|string
     */
    protected function getDataSet($dataSet)
    {
        return $dataSet ?? $this->defaultDataset;
    }
}
