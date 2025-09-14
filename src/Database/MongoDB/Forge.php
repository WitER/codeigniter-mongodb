<?php
declare(strict_types=1);

namespace CodeIgniter\Database\MongoDB;

use CodeIgniter\Database\ConnectionInterface;
use CodeIgniter\Database\Forge as BaseForge;
use CodeIgniter\Database\Exceptions\DatabaseException;
use MongoDB\Database as MongoDatabase;

/**
 * Forge for MongoDB
 *
 * This class manages collections and their validators (JSON Schema) and indexes.
 * It mimics a subset of SQL Forge capabilities for CodeIgniter usage.
 *
 * @property Connection $db
 */
class Forge extends BaseForge
{
    /**
     * MongoDB does not support CREATE DATABASE via driver in the same sense. No-op.
     */
    protected $createDatabaseStr = '';

    // -----------------------
    // Small internal helpers
    // -----------------------

    /** Ensure DBPrefix is applied to collection name only once. */
    protected function applyPrefixOnce(string $name): string
    {
        $prefix = (string) $this->db->DBPrefix;
        if ($prefix !== '' && !str_starts_with($name, $prefix)) {
            return $prefix . $name;
        }
        return $name;
    }

    /** Fully-qualified collection name as "db.collection" for renameCollection. */
    protected function fqcn(string $name): string
    {
        $dbName = (string) $this->db->database;
        return $dbName . '.' . $this->applyPrefixOnce($name);
    }

    /**
     * Encodes the command to JSON and executes it via Connection::query.
     * Returns true for write operations or a Result object/array for reads according to Connection.
     */
    protected function runCommand(array $command, array $options = [])
    {
        // Merge options if provided (Connection::execute supports top-level 'options')
        if ($options !== []) {
            $payload = $command + ['options' => $options];
        } else {
            $payload = $command;
        }
        $json = json_encode($payload, JSON_UNESCAPED_SLASHES);
        return $this->db->query($json);
    }

    /** Normalize index keys directions (ASC/DESC, 1/-1). */
    protected function normalizeIndexKeys(array $keys): array
    {
        $out = [];
        foreach ($keys as $k => $v) {
            $field = is_int($k) ? (string) $v : (string) $k;
            if ($field === '') {
                continue;
            }
            if (is_string($v)) {
                $dir = (strtoupper($v) === 'DESC') ? -1 : 1;
            } elseif (is_int($v)) {
                $dir = ($v < 0) ? -1 : 1;
            } else {
                $dir = 1;
            }
            $out[$field] = $dir;
        }
        return $out;
    }

    /** Build createIndexes command document for a set of indexes. */
    protected function buildCreateIndexesCommand(string $table, array $indexes): array
    {
        $pref = $this->applyPrefixOnce($table);
        $indexDocs = [];
        foreach ($indexes as $idx) {
            if (empty($idx['fields']) || !is_array($idx['fields'])) {
                continue;
            }
            $key = $this->normalizeIndexKeys($idx['fields']);
            if ($key === []) {
                continue;
            }
            $opts = $this->mapIndexOptions($idx['options'] ?? []);
            if (empty($opts['name'])) {
                $opts['name'] = 'idx_' . implode('_', array_keys($key));
            }
            $indexDocs[] = array_filter([
                'key'    => $key,
                'name'   => $opts['name'] ?? null,
                'unique' => $opts['unique'] ?? null,
                'sparse' => $opts['sparse'] ?? null,
                'expireAfterSeconds' => $opts['expireAfterSeconds'] ?? null,
            ], static fn($v) => $v !== null);
        }
        return [
            'createIndexes' => $pref,
            'indexes'       => $indexDocs,
        ];
    }

    /**
     * Provides access to the forge's current database connection.
     *
     * @return ConnectionInterface
     */
    public function getConnection()
    {
        if ($this->db->mongoDB === null) {
            $this->db->connect();
        }
        return $this->db;
    }

    /**
     * MongoDB does not use table options/attributes the same way; ignore.
     */
    protected function _createTableAttributes(array $attributes): string
    {
        return '';
    }

    /**
     * Soft-reset внутреннего состояния Forge для совместимости с вызовами _reset().
     */
    protected function _reset(): void
    {
        // Delegate to BaseForge::reset() for full compatibility with CodeIgniter's Forge.
        $this->reset();
    }

    /**
     * Create collection with JSON Schema validator and indexes if provided.
     *
     * @param array $ifNotExistsAttributes Unused.
     */
    public function createTable(string $table, bool $ifNotExists = false, array $attributes = []): bool
    {
        /** @var Connection $conn */
        $conn = $this->db;
        if (! $conn->mongoDB instanceof MongoDatabase) {
            $conn->connect();
        }

        // If requested, honor IF NOT EXISTS via tableExists() instead of error parsing
        $prefixedTable = $this->applyPrefixOnce($table);
        if ($ifNotExists && $conn->tableExists($prefixedTable, false)) {
            $this->_reset();
            return true;
        }

        $options = [];
        if (isset($attributes['validator']) && is_array($attributes['validator'])) {
            $options['validator'] = ['$jsonSchema' => $attributes['validator']];
        } elseif (isset($this->fields) && $this->fields !== []) {
            // Build a $jsonSchema from defined fields with rich constraints
            $schema = $this->buildJsonSchemaFromFields($this->fields);
            $options['validator'] = ['$jsonSchema' => $schema];
        }

        // Дополнительные опции MongoDB createCollection
        if (!empty($attributes['options']) && is_array($attributes['options'])) {
            $options = array_replace($options, $this->mapCreateCollectionOptions($attributes['options']));
        }

        // Собираем индексы из атрибутов + «SQL-ключей» (addKey/addUniqueKey/addPrimaryKey)
        $indexes = is_array($attributes['indexes'] ?? null) ? $attributes['indexes'] : [];
        // Преобразование addPrimaryKey()/addUniqueKey()/addKey() в индексы
        if (property_exists($this, 'primaryKeys') && !empty($this->primaryKeys)) {
            // BaseForge stores primaryKeys as ['fields'=>[...], 'keyName'=>...]
            $fieldsSrc = [];
            if (is_array($this->primaryKeys) && isset($this->primaryKeys['fields'])) {
                $fieldsSrc = (array) $this->primaryKeys['fields'];
            } else {
                $fieldsSrc = (array) $this->primaryKeys;
            }
            $flat = [];
            foreach ($fieldsSrc as $f => $v) {
                $fname = is_int($f) ? (string) $v : (string) $f;
                if ($fname !== '' && $fname !== '_id') {
                    $flat[$fname] = 1;
                }
            }
            if (!empty($flat)) {
                $nameParts = array_keys($flat);
                $indexes[] = [
                    'fields'  => $flat,
                    'options' => ['unique' => true, 'name' => 'pk_' . implode('_', $nameParts)],
                ];
            }
        }
        if (property_exists($this, 'uniqueKeys') && !empty($this->uniqueKeys)) {
            foreach ((array) $this->uniqueKeys as $uk) {
                $fields = [];
                if (is_int($uk) && isset($this->keys[$uk]['fields'])) {
                    $fields = (array) $this->keys[$uk]['fields'];
                } elseif (is_array($uk) && isset($uk['fields'])) {
                    $fields = (array) $uk['fields'];
                } else {
                    $fields = is_array($uk) ? $uk : [$uk];
                }
                $key = [];
                foreach ($fields as $f => $v) {
                    $fname = is_int($f) ? (string) $v : (string) $f;
                    if ($fname !== '') {
                        $key[$fname] = 1;
                    }
                }
                if ($key !== []) {
                    $indexes[] = [
                        'fields'  => $key,
                        'options' => ['unique' => true, 'name' => 'uniq_' . implode('_', array_keys($key))],
                    ];
                }
            }
        }
        if (property_exists($this, 'keys') && !empty($this->keys)) {
            // keys: support CodeIgniter form ['fields'=>[...], 'keyName'=>...]
            foreach ((array) $this->keys as $k) {
                $keySpec = [];

                $applyField = static function ($field, $dir) use (&$keySpec) {
                    $fname = is_string($field) ? $field : (is_int($field) ? (string) $dir : (string) $field);
                    // Normalize direction
                    if (is_string($dir)) {
                        $d = (strtoupper($dir) === 'DESC') ? -1 : 1;
                    } elseif (is_int($dir)) {
                        $d = ($dir < 0) ? -1 : 1;
                    } else {
                        $d = 1; // default ASC
                    }
                    if ($fname !== '') {
                        $keySpec[$fname] = $d;
                    }
                };

                if (is_string($k)) {
                    $keySpec[$k] = 1;
                } elseif (is_array($k) && isset($k['fields']) && is_array($k['fields'])) {
                    foreach ($k['fields'] as $f => $d) {
                        if (is_int($f)) {
                            $applyField($d, 1);
                        } else {
                            $applyField($f, $d);
                        }
                    }
                } elseif (is_array($k)) {
                    foreach ($k as $field => $dir) {
                        if (is_int($field)) {
                            if (is_string($dir)) {
                                $applyField($dir, 1);
                            } elseif (is_array($dir)) {
                                foreach ($dir as $f2 => $d2) {
                                    $applyField($f2, $d2);
                                }
                            } else {
                                continue;
                            }
                        } else {
                            $applyField($field, $dir);
                        }
                    }
                } else {
                    continue;
                }

                if ($keySpec !== []) {
                    $indexes[] = ['fields' => $keySpec, 'options' => ['name' => 'idx_' . implode('_', array_keys($keySpec))]];
                }
            }
        }

        try {
            // Build create command or view creation
            if (!empty($options['viewOn']) && !empty($options['pipeline']) && is_array($options['pipeline'])) {
                $cmd = [
                    'create'   => $prefixedTable,
                    'viewOn'   => $options['viewOn'],
                    'pipeline' => $options['pipeline'],
                ] + array_intersect_key($options, array_flip(['collation','comment']));
            } else {
                $cmd = ['create' => $prefixedTable] + $options;
            }

            $res = $this->runCommand($cmd);
            if ($res === false) {
                $this->_reset();
                return false;
            }

            // Indexes
            if (! empty($indexes)) {
                $this->createIndexes($table, $indexes);
            }
        } catch (\Throwable $e) {
            if ($this->db->DBDebug) {
                throw new DatabaseException('MongoDB createCollection failed: ' . $e->getMessage(), (int) $e->getCode(), $e);
            }
            $this->_reset();
            return false;
        }

        // Update data cache like BaseForge
        if (isset($this->db->dataCache['table_names']) && ! in_array($prefixedTable, $this->db->dataCache['table_names'], true)) {
            $this->db->dataCache['table_names'][] = $prefixedTable;
        }

        $this->_reset();
        return true;
    }

    /**
     * Update collection validator (schema) using collMod.
     */
    public function modifyTable(string $table, array $attributes = []): bool
    {
        /** @var Connection $conn */
        $conn = $this->db;
        if (! $conn->mongoDB instanceof MongoDatabase) {
            $conn->connect();
        }

        if (empty($attributes['validator']) && $this->fields === [] && empty($attributes['options'])) {
            $this->_reset();
            return true; // nothing to do
        }

        // Build or merge schema
        $newSchema = $attributes['validator'] ?? null;
        if ($newSchema === null && $this->fields !== []) {
            $newSchema = $this->buildJsonSchemaFromFields($this->fields);
        }

        $cmd = ['collMod' => $conn->DBPrefix . $table];
        if ($newSchema !== null) {
            // If explicit validator provided — use as-is. Otherwise merge with current schema to avoid losing properties/required
            if (!isset($attributes['validator'])) {
                $current = $conn->getCollectionSchema($table);
                if ($current !== []) {
                    $merged = $current;
                    $merged['properties'] = $merged['properties'] ?? [];
                    $incomingProps = $newSchema['properties'] ?? [];
                    foreach ($incomingProps as $name => $propSpec) {
                        $merged['properties'][(string)$name] = $propSpec;
                    }
                    // Required list: for provided fields we override requiredness; others remain untouched
                    $currReq = isset($merged['required']) && is_array($merged['required']) ? $merged['required'] : [];
                    $incomingReq = $newSchema['required'] ?? [];
                    $providedNames = array_map('strval', array_keys($incomingProps));
                    // Remove provided names from current required
                    if ($currReq !== []) {
                        $currReq = array_values(array_diff($currReq, $providedNames));
                    }
                    // Add new requireds
                    foreach ($incomingReq as $rn) {
                        $rn = (string)$rn;
                        if (!in_array($rn, $currReq, true)) {
                            $currReq[] = $rn;
                        }
                    }
                    if ($currReq !== []) {
                        $merged['required'] = array_values($currReq);
                    } else {
                        unset($merged['required']);
                    }
                    $newSchema = $merged;
                }
            }
            $cmd['validator'] = ['$jsonSchema' => $newSchema];
        }
        // Поддержка дополнительных collMod-опций
        if (!empty($attributes['options']) && is_array($attributes['options'])) {
            $cmd = array_replace($cmd, $this->mapCollModOptions($attributes['options']));
        }

        try {
            $res = $this->runCommand($cmd);
            if ($res === false) {
                $this->_reset();
                return false;
            }
        } catch (\Throwable $e) {
            if ($this->db->DBDebug) {
                throw new DatabaseException('MongoDB collMod failed: ' . $e->getMessage(), (int) $e->getCode(), $e);
            }
            $this->_reset();
            return false;
        }

        // Собираем индексы: объединяем переданные в attributes с ключами, заданными через addPrimaryKey/addUniqueKey/addKey
        $indexes = is_array($attributes['indexes'] ?? null) ? $attributes['indexes'] : [];

        // Преобразование addPrimaryKey() в unique-индекс (кроме _id)
        if (property_exists($this, 'primaryKeys') && ! empty($this->primaryKeys)) {
            // Поддержка составного ключа: объединяем все поля в один unique-индекс
            $flat = [];
            foreach ((array) $this->primaryKeys as $pk) {
                if (is_array($pk)) {
                    foreach ($pk as $f => $v) {
                        $f = is_int($f) ? (string) $v : (string) $f;
                        if ($f !== '_id') {
                            $flat[$f] = 1;
                        }
                    }
                } else {
                    $f = (string) $pk;
                    if ($f !== '_id') {
                        $flat[$f] = 1;
                    }
                }
            }
            if (!empty($flat)) {
                $indexes[] = [
                    'fields'  => $flat,
                    'options' => ['unique' => true, 'name' => 'pk_' . implode('_', array_keys($flat))],
                ];
            }
        }

        // Преобразование addUniqueKey()
        if (property_exists($this, 'uniqueKeys') && ! empty($this->uniqueKeys)) {
            foreach ((array) $this->uniqueKeys as $uk) {
                $fields = is_array($uk) ? $uk : [$uk];
                $key = [];
                foreach ($fields as $f) {
                    $key[(string) $f] = 1; // направление по умолчанию ASC
                }
                $indexes[] = [
                    'fields'  => $key,
                    'options' => ['unique' => true, 'name' => 'uniq_' . implode('_', array_keys($key))],
                ];
            }
        }

        // Преобразование addKey()
        // Преобразование addKey()
        if (property_exists($this, 'keys') && ! empty($this->keys)) {
            foreach ((array) $this->keys as $k) {
                $keySpec = [];

                $applyField = static function ($field, $dir) use (&$keySpec) {
                    $fname = is_string($field) ? $field : (is_int($field) ? (string) $dir : (string) $field);
                    if (is_string($dir)) {
                        $d = (strtoupper($dir) === 'DESC') ? -1 : 1;
                    } elseif (is_int($dir)) {
                        $d = ($dir < 0) ? -1 : 1;
                    } else {
                        $d = 1;
                    }
                    if ($fname !== '') {
                        $keySpec[$fname] = $d;
                    }
                };

                if (is_string($k)) {
                    $keySpec[$k] = 1;
                } elseif (is_array($k)) {
                    foreach ($k as $field => $dir) {
                        if (is_int($field)) {
                            if (is_string($dir)) {
                                $applyField($dir, 1);
                            } elseif (is_array($dir)) {
                                foreach ($dir as $f2 => $d2) {
                                    $applyField($f2, $d2);
                                }
                            } else {
                                continue;
                            }
                        } else {
                            $applyField($field, $dir);
                        }
                    }
                } else {
                    continue;
                }

                if ($keySpec !== []) {
                    $indexes[] = ['fields' => $keySpec, 'options' => ['name' => 'idx_' . implode('_', array_keys($keySpec))]];
                }
            }
        }

        // Создание индексов (если есть что создавать)
        if ($indexes !== []) {
            $this->createIndexes($table, $indexes);
        }

        $this->_reset();
        return true;
    }

    /**
     * Drop a collection.
     */
    public function dropTable(string $table, bool $ifExists = false, bool $cascade = false): bool
    {
        /** @var Connection $conn */
        $conn = $this->db;
        if (! $conn->mongoDB instanceof MongoDatabase) {
            $conn->connect();
        }

        // IF EXISTS using tableExists()
        $prefixedTable = $this->applyPrefixOnce($table);
        if ($ifExists && ! $conn->tableExists($prefixedTable, false)) {
            $this->_reset();
            return true;
        }

        try {
            $res = $this->runCommand(['drop' => $prefixedTable]);
            if ($res === false) {
                $this->_reset();
                return false;
            }
        } catch (\Throwable $e) {
            if ($this->db->DBDebug) {
                throw new DatabaseException('MongoDB dropCollection failed: ' . $e->getMessage(), (int) $e->getCode(), $e);
            }
            $this->_reset();
            return false;
        }

        // Update data cache like BaseForge
        if (! empty($this->db->dataCache['table_names'])) {
            $key = array_search(
                strtolower($prefixedTable),
                array_map(strtolower(...), $this->db->dataCache['table_names']),
                true,
            );
            if ($key !== false) {
                unset($this->db->dataCache['table_names'][$key]);
            }
        }

        $this->_reset();
        return true;
    }

    /**
     * Create indexes on collection.
     * Example $indexes: [ ['fields'=>['email'=>1],'options'=>['unique'=>true]] ]
     */
    public function createIndexes(string $table, array $indexes): bool
    {
        // Prepare createIndexes command
        $cmd = $this->buildCreateIndexesCommand($table, $indexes);
        if (empty($cmd['indexes'])) {
            $this->_reset();
            return true;
        }
        try {
            $res = $this->runCommand($cmd);
            if ($res === false) {
                $this->_reset();
                return false;
            }
        } catch (\Throwable $e) {
            if ($this->db->DBDebug) {
                throw new DatabaseException('MongoDB createIndexes failed: ' . $e->getMessage(), (int) $e->getCode(), $e);
            }
            $this->_reset();
            return false;
        }
        $this->_reset();
        return true;
    }

    /**
     * Drop indexes by names or patterns.
     * $indexes: string|array — имя индекса или массив имён; '*' — удалить все
     */
    public function dropIndexes(string $table, $indexes = '*'): bool
    {
        $pref = $this->applyPrefixOnce($table);
        try {
            if ($indexes === '*') {
                $res = $this->runCommand(['dropIndexes' => $pref, 'index' => '*']);
                $ok = $res !== false;
            } else {
                $ok = true;
                $names = is_array($indexes) ? $indexes : [(string) $indexes];
                foreach ($names as $n) {
                    $res = $this->runCommand(['dropIndexes' => $pref, 'index' => (string) $n]);
                    if ($res === false) { $ok = false; break; }
                }
            }
            $this->_reset();
            return $ok;
        } catch (\Throwable $e) {
            if ($this->db->DBDebug) {
                throw new DatabaseException('MongoDB dropIndexes failed: ' . $e->getMessage(), (int) $e->getCode(), $e);
            }
            $this->_reset();
            return false;
        }
    }

    /**
     * Drop primary key index(es) created via addPrimaryKey().
     * - If $key is null, drops all indexes with name starting with 'pk_'.
     * - If $key is string, treats it as field name or full index name ('pk_...').
     * - If $key is array of fields, builds composite name using default naming.
     */
    public function dropPrimaryKey(string $table, $key = null): bool
    {
        /** @var Connection $conn */
        $conn = $this->db;
        if ($key === null) {
            // Drop all 'pk_*' indexes
            $names = [];
            foreach ($this->listIndexes($table) as $spec) {
                $name = (string) ($spec['name'] ?? '');
                if (strpos($name, 'pk_') === 0) {
                    $names[] = $name;
                }
            }
            return $this->dropIndexes($table, $names);
        }
        if (is_array($key)) {
            $fields = [];
            foreach ($key as $f) { $fields[] = (string) $f; }
            $name = 'pk_' . implode('_', $fields);
        } else {
            $k = (string) $key;
            $name = (strpos($k, 'pk_') === 0) ? $k : ('pk_' . $k);
        }
        return $this->dropIndexes($table, $name);
    }

    /**
     * Drop unique key index(es) created via addUniqueKey().
     * - If $key is null, drops all indexes with name starting with 'uniq_'.
     * - If $key is string, treats it as field name or full index name ('uniq_...').
     * - If $key is array of fields, builds composite name using default naming.
     */
    public function dropUniqueKey(string $table, $key = null): bool
    {
        /** @var Connection $conn */
        $conn = $this->db;
        if ($key === null) {
            $names = [];
            foreach ($this->listIndexes($table) as $spec) {
                $name = (string) ($spec['name'] ?? '');
                if (strpos($name, 'uniq_') === 0) {
                    $names[] = $name;
                }
            }
            return $this->dropIndexes($table, $names);
        }
        if (is_array($key)) {
            $fields = [];
            foreach ($key as $f) { $fields[] = (string) $f; }
            $name = 'uniq_' . implode('_', $fields);
        } else {
            $k = (string) $key;
            $name = (strpos($k, 'uniq_') === 0) ? $k : ('uniq_' . $k);
        }
        return $this->dropIndexes($table, $name);
    }

    /**
     * Drop regular key index(es) created via addKey().
     * Thin wrapper over dropIndexes().
     * - If $key is '*', drops all indexes.
     * - If $key is string, treats it as field name or full index name ('idx_...').
     * - If $key is array of fields, builds composite name using default naming.
     */
    public function dropKey(string $table, string $keyName, bool $prefixKeyName = true): bool
    {
        // Thin wrapper around dropIndexes(); $prefixKeyName emulates CI's behavior of prefixing driver-specific key names
        $name = $keyName;
        if ($prefixKeyName) {
            if (strpos($name, 'idx_') !== 0 && strpos($name, 'uniq_') !== 0 && strpos($name, 'pk_') !== 0) {
                $name = 'idx_' . $name;
            }
        }
        return $this->dropIndexes($table, $name);
    }

    /**
     * List indexes for collection.
     * Возвращает массив спецификаций индексов.
     */
    public function listIndexes(string $table): array
    {
        $pref = $this->applyPrefixOnce($table);
        try {
            $res = $this->runCommand(['listIndexes' => $pref]);
            $rows = is_object($res) && method_exists($res, 'getResultArray') ? $res->getResultArray() : [];
            return $rows ?? [];
        } catch (\Throwable $e) {
            if ($this->db->DBDebug) {
                throw new DatabaseException('MongoDB listIndexes failed: ' . $e->getMessage(), (int) $e->getCode(), $e);
            }
            return [];
        }
    }

    /**
     * Rename collection (renameTable analogue).
     */
    public function renameTable(string $table, string $newName): bool
    {
        /** @var Connection $conn */
        $conn = $this->db;
        if (! $conn->mongoDB instanceof MongoDatabase) {
            $conn->connect();
        }

        try {
            $res = $this->runCommand([
                'renameCollection' => $this->fqcn($table),
                'to'               => $this->fqcn($newName),
                'dropTarget'       => false,
            ]);
            if ($res === false) {
                $this->_reset();
                return false;
            }
            // Update data cache like BaseForge
            if (! empty($this->db->dataCache['table_names'])) {
                $oldPref = $this->applyPrefixOnce($table);
                $key = array_search(
                    strtolower($oldPref),
                    array_map(strtolower(...), $this->db->dataCache['table_names']),
                    true,
                );
                if ($key !== false) {
                    $this->db->dataCache['table_names'][$key] = $this->applyPrefixOnce($newName);
                }
            }
            $this->_reset();
            return true;
        } catch (\Throwable $e) {
            if ($this->db->DBDebug) {
                throw new DatabaseException('MongoDB renameCollection failed: ' . $e->getMessage(), (int) $e->getCode(), $e);
            }
            $this->_reset();
            return false;
        }
    }

    /**
     * No-op createDatabase/dropDatabase for MongoDB (drop-in compatibility).
     */
    public function createDatabase(string $dbName, bool $ifNotExists = false): bool
    {
        // MongoDB databases are created implicitly on first write
        $this->_reset();
        return true;
    }

    public function dropDatabase(string $dbName, bool $ifExists = false): bool
    {
        // Not handled through Forge; treat as no-op for migrations
        $this->_reset();
        return true;
    }

    /**
     * Column operations as thin wrappers over collMod (modifyTable).
     */
    public function addColumn(string $table, $fields): bool
    {
        // Use provided field definitions to extend validator
        $this->fields = is_array($fields) ? $fields : [$fields => []];
        $ok = $this->modifyTable($table);
        return $ok;
    }

    public function modifyColumn(string $table, $fields): bool
    {
        $this->fields = is_array($fields) ? $fields : [$fields => []];
        $ok = $this->modifyTable($table);
        return $ok;
    }

    public function dropColumn(string $table, $columnNames): bool
    {
        /** @var Connection $conn */
        $conn = $this->db;

        // Опции управления миграцией данных и индексами
        $migrateExisting = false;
        $allowNoFilterWrite = false;
        $batchSize = null; // int|null
        $limit = null;     // int|null
        $autoFixIndexes = false;

        $columnsParam = $columnNames;
        if (is_array($columnsParam)) {
            if (array_key_exists('migrateExisting', $columnsParam)) {
                $migrateExisting = (bool) $columnsParam['migrateExisting'];
                unset($columnsParam['migrateExisting']);
            }
            if (array_key_exists('allowNoFilterWrite', $columnsParam)) {
                $allowNoFilterWrite = (bool) $columnsParam['allowNoFilterWrite'];
                unset($columnsParam['allowNoFilterWrite']);
            }
            if (array_key_exists('batchSize', $columnsParam)) {
                $batchSize = is_numeric($columnsParam['batchSize']) ? max(1, (int)$columnsParam['batchSize']) : null;
                unset($columnsParam['batchSize']);
            }
            if (array_key_exists('limit', $columnsParam)) {
                $limit = is_numeric($columnsParam['limit']) ? max(1, (int)$columnsParam['limit']) : null;
                unset($columnsParam['limit']);
            }
            if (array_key_exists('autoFixIndexes', $columnsParam)) {
                $autoFixIndexes = (bool) $columnsParam['autoFixIndexes'];
                unset($columnsParam['autoFixIndexes']);
            }
        }
        if (is_array($columnsParam) && array_key_exists('columns', $columnsParam) && is_array($columnsParam['columns'])) {
            $columns = array_map('strval', $columnsParam['columns']);
        } else {
            $columns = is_array($columnsParam) ? array_map('strval', $columnsParam) : [(string) $columnsParam];
        }

        if (! $migrateExisting && function_exists('log_message')) {
            log_message('warning', "MongoDB Forge: dropColumn on '{$table}' updates only the validator; existing documents will retain fields: " . implode(', ', $columns) . ". Pass migrateExisting=true to remove them from documents.");
        }

        // Fetch current schema and remove columns
        $schema = $conn->getCollectionSchema($table);
        if ($schema === []) {
            $this->_reset();
            return true;
        }
        $props = $schema['properties'] ?? [];
        foreach ($columns as $col) {
            unset($props[$col]);
        }
        $schema['properties'] = $props;
        if (!empty($schema['required'])) {
            $schema['required'] = array_values(array_diff($schema['required'], $columns));
            if ($schema['required'] === []) {
                unset($schema['required']);
            }
        }
        try {
            $res = $this->runCommand([
                'collMod'   => $this->applyPrefixOnce($table),
                'validator' => ['$jsonSchema' => $schema],
            ]);
            if ($res === false) {
                $this->_reset();
                return false;
            }
        } catch (\Throwable $e) {
            if ($this->db->DBDebug) {
                throw new DatabaseException('MongoDB dropColumn(collMod) failed: ' . $e->getMessage(), (int) $e->getCode(), $e);
            }
            $this->_reset();
            return false;
        }

        // Миграция данных: удаляем поля из существующих документов
        if ($migrateExisting) {
            $start = microtime(true);
            try {
                $unset = [];
                foreach ($columns as $c) { $unset[$c] = ''; }
                // Ограничиваем поиск документами, где поле существует
                $existsOr = [];
                foreach ($columns as $c) { $existsOr[] = [$c => ['$exists' => true]]; }
                $filter = $existsOr ? ['$or' => $existsOr] : [];

                $pref = $this->applyPrefixOnce($table);

                if (($limit === null && $batchSize === null) && ! $allowNoFilterWrite) {
                    if (function_exists('log_message')) {
                        log_message('error', "MongoDB Forge: Refusing to updateMany without filter on dropColumn('{$table}'). Set allowNoFilterWrite=true or provide limit/batchSize.");
                    }
                } else if ($limit === null && $batchSize === null && $allowNoFilterWrite) {
                    // Full collection update (dangerous unless user allowed)
                    $this->runCommand([
                        'update'  => $pref,
                        'updates' => [ ['q' => new \stdClass(), 'u' => ['$unset' => $unset], 'multi' => true] ],
                    ]);
                } else {
                    $max = $limit ?? PHP_INT_MAX;
                    $bs = $batchSize ?? min(1000, $max);
                    // Fetch up to $max ids using find command
                    $findCmd = [
                        'find'       => $pref,
                        'filter'     => $filter === [] ? new \stdClass() : $filter,
                        'projection' => ['_id' => 1],
                        'limit'      => $max,
                    ];
                    $res = $this->runCommand($findCmd);
                    $rows = is_object($res) && method_exists($res, 'getResultArray') ? $res->getResultArray() : [];
                    $ids = [];
                    foreach ($rows as $row) {
                        if (isset($row['_id'])) { $ids[] = $row['_id']; }
                    }
                    // Chunk and update
                    for ($i = 0, $n = count($ids); $i < $n; $i += $bs) {
                        $chunk = array_slice($ids, $i, $bs);
                        if ($chunk === []) { break; }
                        $this->runCommand([
                            'update'  => $pref,
                            'updates' => [ ['q' => ['_id' => ['$in' => $chunk]], 'u' => ['$unset' => $unset], 'multi' => true] ],
                        ]);
                    }
                }
            } catch (\Throwable $e) {
                if ($this->db->DBDebug) {
                    throw new DatabaseException('MongoDB dropColumn(updateMany) failed: ' . $e->getMessage(), (int) $e->getCode(), $e);
                }
                $this->_reset();
                return false;
            } finally {
                if (function_exists('log_message')) {
                    $durMs = (int) round((microtime(true) - $start) * 1000);
                    log_message('info', "MongoDB Forge: dropColumn migrateExisting on '{$table}' took {$durMs} ms.");
                }
            }
        }

        // Синхронизация индексов: удалить индексы, где встречаются удаляемые поля
        if ($autoFixIndexes) {
            $impacted = [];
            foreach ($this->listIndexes($table) as $spec) {
                $name = (string)($spec['name'] ?? '');
                $key  = (array)($spec['key'] ?? []);
                $fields = array_map('strval', array_keys($key));
                if (array_intersect($fields, $columns)) {
                    if ($name !== '' && $name !== '_id_') { $impacted[] = $name; }
                }
            }
            if ($impacted !== []) {
                $this->dropIndexes($table, $impacted);
            }
        } else {
            foreach ($this->listIndexes($table) as $spec) {
                $name = (string)($spec['name'] ?? '');
                $key  = (array)($spec['key'] ?? []);
                $fields = array_map('strval', array_keys($key));
                if (array_intersect($fields, $columns)) {
                    if (function_exists('log_message')) {
                        log_message('warning', "MongoDB Forge: dropColumn detected index '{$name}' contains dropped fields (" . implode(',', array_intersect($fields, $columns)) . ") on '{$table}'. Consider removing or recreating index or pass autoFixIndexes=true.");
                    }
                }
            }
        }

        $this->_reset();
        return true;
    }

    public function renameColumn(string $table, array $field): bool
    {
        /** @var Connection $conn */
        $conn = $this->db;
        $schema = $conn->getCollectionSchema($table);
        if ($schema === []) {
            $this->_reset();
            return true;
        }

        // Флаги миграции данных и индексов на верхнем уровне
        $migrateExisting = false;
        $allowNoFilterWrite = false;
        $batchSize = null; // int|null
        $limit = null;     // int|null
        $autoFixIndexes = false;
        if (isset($field['migrateExisting'])) {
            $migrateExisting = (bool) $field['migrateExisting'];
            unset($field['migrateExisting']);
        }
        if (isset($field['allowNoFilterWrite'])) {
            $allowNoFilterWrite = (bool) $field['allowNoFilterWrite'];
            unset($field['allowNoFilterWrite']);
        }
        if (isset($field['batchSize'])) {
            $batchSize = is_numeric($field['batchSize']) ? max(1, (int)$field['batchSize']) : null;
            unset($field['batchSize']);
        }
        if (isset($field['limit'])) {
            $limit = is_numeric($field['limit']) ? max(1, (int)$field['limit']) : null;
            unset($field['limit']);
        }
        if (isset($field['autoFixIndexes'])) {
            $autoFixIndexes = (bool) $field['autoFixIndexes'];
            unset($field['autoFixIndexes']);
        }

        if (! $migrateExisting && function_exists('log_message')) {
            log_message('warning', "MongoDB Forge: renameColumn on '{$table}' updates only the validator; existing documents keep old field names. Pass migrateExisting=true to rename fields in documents.");
        }

        $props = $schema['properties'] ?? [];
        $required = $schema['required'] ?? [];
        $renameMap = [];
        foreach ($field as $old => $spec) {
            $old = (string) $old;
            $specMigrate = is_array($spec) ? (bool) ($spec['migrateExisting'] ?? false) : false;
            if (is_array($spec)) { unset($spec['migrateExisting']); }
            $new = is_array($spec) ? (string) ($spec['name'] ?? '') : '';
            if ($new === '' || !array_key_exists($old, $props)) {
                continue;
            }
            // If spec includes new definition, rebuild property using it
            if (is_array($spec) && (isset($spec['type']) || isset($spec['enum']) || isset($spec['constraint']) || isset($spec['maxLength']) || isset($spec['pattern']) || isset($spec['min']) || isset($spec['max']) || isset($spec['default']))) {
                $newSchema = $this->buildJsonSchemaFromFields([$new => $spec]);
                $props[$new] = $newSchema['properties'][$new] ?? $props[$old];
            } else {
                $props[$new] = $props[$old];
            }
            unset($props[$old]);
            // required list
            $idx = array_search($old, $required, true);
            if ($idx !== false) {
                $required[$idx] = $new;
            }
            if ($migrateExisting || $specMigrate) {
                $renameMap[$old] = $new;
            }
        }
        $schema['properties'] = $props;
        if (!empty($required)) {
            $schema['required'] = array_values($required);
        } else {
            unset($schema['required']);
        }
        try {
            $res = $this->runCommand([
                'collMod'   => $this->applyPrefixOnce($table),
                'validator' => ['$jsonSchema' => $schema],
            ]);
            if ($res === false) {
                $this->_reset();
                return false;
            }
        } catch (\Throwable $e) {
            if ($this->db->DBDebug) {
                throw new DatabaseException('MongoDB renameColumn(collMod) failed: ' . $e->getMessage(), (int) $e->getCode(), $e);
            }
            $this->_reset();
            return false;
        }

        // Реальная миграция данных: $rename
        if (!empty($renameMap)) {
            $start = microtime(true);
            try {
                $pref = $this->applyPrefixOnce($table);
                // Фильтр только по документам, где есть хотя бы одно из старых полей
                $existsOr = [];
                foreach (array_keys($renameMap) as $old) { $existsOr[] = [$old => ['$exists' => true]]; }
                $filter = $existsOr ? ['$or' => $existsOr] : [];

                if (($limit === null && $batchSize === null) && ! $allowNoFilterWrite) {
                    if (function_exists('log_message')) {
                        log_message('error', "MongoDB Forge: Refusing to updateMany without filter on renameColumn('{$table}'). Set allowNoFilterWrite=true or provide limit/batchSize.");
                    }
                } else if ($limit === null && $batchSize === null && $allowNoFilterWrite) {
                    $this->runCommand([
                        'update'  => $pref,
                        'updates' => [ ['q' => new \stdClass(), 'u' => ['$rename' => $renameMap], 'multi' => true] ],
                    ]);
                } else {
                    $max = $limit ?? PHP_INT_MAX;
                    $bs = $batchSize ?? min(1000, $max);
                    // Fetch ids first
                    $findCmd = [
                        'find'       => $pref,
                        'filter'     => $filter === [] ? new \stdClass() : $filter,
                        'projection' => ['_id' => 1],
                        'limit'      => $max,
                    ];
                    $res = $this->runCommand($findCmd);
                    $rows = is_object($res) && method_exists($res, 'getResultArray') ? $res->getResultArray() : [];
                    $ids = [];
                    foreach ($rows as $row) { if (isset($row['_id'])) { $ids[] = $row['_id']; } }
                    for ($i = 0, $n = count($ids); $i < $n; $i += $bs) {
                        $chunk = array_slice($ids, $i, $bs);
                        if ($chunk === []) { break; }
                        $this->runCommand([
                            'update'  => $pref,
                            'updates' => [ ['q' => ['_id' => ['$in' => $chunk]], 'u' => ['$rename' => $renameMap], 'multi' => true] ],
                        ]);
                    }
                }
            } catch (\Throwable $e) {
                if ($this->db->DBDebug) {
                    throw new DatabaseException('MongoDB renameColumn(updateMany) failed: ' . $e->getMessage(), (int) $e->getCode(), $e);
                }
                $this->_reset();
                return false;
            } finally {
                if (function_exists('log_message')) {
                    $durMs = (int) round((microtime(true) - $start) * 1000);
                    log_message('info', "MongoDB Forge: renameColumn migrateExisting on '{$table}' took {$durMs} ms.");
                }
            }
        }

        // Синхронизация индексов: переименовать затронутые индексы
        if (!empty($renameMap)) {
            if ($autoFixIndexes) {
                foreach ($this->listIndexes($table) as $spec) {
                    $name = (string)($spec['name'] ?? '');
                    if ($name === '' || $name === '_id_') { continue; }
                    $key  = (array)($spec['key'] ?? []);
                    $newKey = [];
                    $changed = false;
                    foreach ($key as $f => $dir) {
                        $nf = array_key_exists((string)$f, $renameMap) ? $renameMap[(string)$f] : (string)$f;
                        if ($nf !== (string)$f) { $changed = true; }
                        $newKey[$nf] = (int)$dir;
                    }
                    if ($changed) {
                        // Сохраняем основные опции индекса
                        $opt = [];
                        foreach (['unique','expireAfterSeconds','sparse','hidden','collation','partialFilterExpression','wildcardProjection','weights','default_language','language_override','storageEngine'] as $k) {
                            if (isset($spec[$k])) { $opt[$k] = $spec[$k]; }
                        }
                        // Попробуем обновить название индекса, заменив старые имена полей
                        $newName = $name;
                        foreach ($renameMap as $o => $n) { $newName = str_replace($o, $n, (string)$newName); }
                        $opt['name'] = $newName;
                        // Удаляем старый индекс и создаём новый
                        $this->dropIndexes($table, $name);
                        $this->createIndexes($table, [ ['fields' => $newKey, 'options' => $opt] ]);
                    }
                }
            } else {
                foreach ($this->listIndexes($table) as $spec) {
                    $name = (string)($spec['name'] ?? '');
                    $key  = (array)($spec['key'] ?? []);
                    $fields = array_map('strval', array_keys($key));
                    $inter = array_intersect($fields, array_keys($renameMap));
                    if ($inter) {
                        if (function_exists('log_message')) {
                            log_message('warning', "MongoDB Forge: renameColumn detected index '{$name}' contains renamed fields (" . implode(',', $inter) . ") on '{$table}'. Consider recreating indexes or pass autoFixIndexes=true.");
                        }
                    }
                }
            }
        }

        $this->_reset();
        return true;
    }

    private function mapType(string $type): string
    {
        $t = strtolower(trim($type));
        // drop size/params and extra words
        $t = preg_replace('/\(.*$/', '', $t) ?? $t;
        $t = preg_replace('/\s.+$/', '', $t) ?? $t;
        return match ($t) {
            'int', 'integer', 'bigint', 'smallint', 'mediumint', 'tinyint', 'serial' => 'int',
            'double', 'float', 'decimal', 'real', 'numeric' => 'double',
            'bool', 'boolean' => 'bool',
            'array' => 'array',
            'object', 'json' => 'object',
            'date', 'datetime', 'timestamp' => 'date',
            'binary', 'varbinary' => 'binData',
            'char', 'varchar', 'text', 'enum', 'set' => 'string',
            default => 'string',
        };
    }

    /**
     * Build $jsonSchema from Forge fields including constraints like maxLength/enum/default/pattern/min/max.
     * @param array $fields
     * @return array
     */
    private function buildJsonSchemaFromFields(array $fields): array
    {
        $properties = [];
        $required   = [];
        foreach ($fields as $name => $field) {
            if (!is_array($field)) {
                continue;
            }
            $typeRaw  = (string)($field['type'] ?? 'string');
            $bsonType = $this->mapType($typeRaw);

            $prop = [
                'bsonType' => $bsonType,
            ];

            // Первичный ключ: _id — допускаем objectId и 24-символьную hex-строку
            if ((string)$name === '_id') {
                $prop['bsonType'] = ['objectId', 'string'];
                $prop['minLength'] = 24;
                $prop['maxLength'] = 24;
                $prop['pattern']   = '^[a-fA-F0-9]{24}$';
            }

            // ENUM (если задан через enum(...) или ключ 'enum')
            $enum = null;
            if (isset($field['enum']) && is_array($field['enum'])) {
                $enum = array_values($field['enum']);
            } else {
                if (preg_match('/^\s*enum\s*\((.*)\)\s*$/i', $typeRaw, $m)) {
                    $inside = $m[1];
                    if ($inside !== '') {
                        if (preg_match_all("/'([^']*)'|\"([^\"]*)\"/", $inside, $mm)) {
                            $vals = [];
                            foreach ($mm[1] as $i => $v1) {
                                $vals[] = $v1 !== '' ? $v1 : ($mm[2][$i] ?? '');
                            }
                            if ($vals !== []) {
                                $enum = $vals;
                            }
                        }
                    }
                }
            }
            if ($enum !== null) {
                $prop['enum'] = $enum;
                if (!isset($prop['bsonType'])) {
                    $prop['bsonType'] = 'string';
                }
            }

            // Строковые ограничения
            if ($bsonType === 'string') {
                if (isset($field['constraint']) && is_numeric($field['constraint'])) {
                    $prop['maxLength'] = (int) $field['constraint'];
                }
                if (isset($field['maxLength'])) {
                    $prop['maxLength'] = (int) $field['maxLength'];
                }
                if (isset($field['minLength'])) {
                    $prop['minLength'] = (int) $field['minLength'];
                }
                if (isset($field['pattern']) && is_string($field['pattern'])) {
                    $prop['pattern'] = $field['pattern'];
                }
            }

            // Числовые ограничения
            if ($bsonType === 'int' || $bsonType === 'double') {
                if (isset($field['min'])) {
                    $prop['minimum'] = ($bsonType === 'int') ? (int) $field['min'] : (float) $field['min'];
                }
                if (isset($field['max'])) {
                    $prop['maximum'] = ($bsonType === 'int') ? (int) $field['max'] : (float) $field['max'];
                }
                if (!empty($field['unsigned'])) {
                    $prop['minimum'] = max(0, (int) ($prop['minimum'] ?? 0));
                }
            }

            // Nullable: если поле может быть null — добавляем 'null' в список типов
            $isNullable = (bool) ($field['null'] ?? true);
            if ($isNullable) {
                $bt = $prop['bsonType'] ?? null;
                if (is_array($bt)) {
                    if (!in_array('null', $bt, true)) {
                        $bt[] = 'null';
                    }
                    $prop['bsonType'] = array_values($bt);
                } elseif (is_string($bt) && $bt !== 'null') {
                    $prop['bsonType'] = [$bt, 'null'];
                }
                // Не добавляем в required
            } else {
                // Только не-nullable поля помечаем как required
                $required[] = (string) $name;
            }

            $properties[(string) $name] = $prop;
        }

        $schema = ['bsonType' => 'object', 'properties' => $properties];
        if ($required !== []) {
            $schema['required'] = $required;
        }
        return $schema;
    }

    /**
     * Маппер опций createCollection
     */
    private function mapCreateCollectionOptions(array $opts): array
    {
        $out = [];

        // Общие
        if (isset($opts['collation']) && is_array($opts['collation'])) {
            $out['collation'] = $opts['collation'];
        }
        if (isset($opts['comment'])) {
            $out['comment'] = (string) $opts['comment'];
        }

        // Capped
        if (!empty($opts['capped'])) {
            $out['capped'] = true;
            if (isset($opts['size'])) {
                $out['size'] = (int) $opts['size'];
            }
            if (isset($opts['max'])) {
                $out['max'] = (int) $opts['max'];
            }
        }

        // Time-series
        if (!empty($opts['timeSeries']) && is_array($opts['timeSeries'])) {
            $ts = $opts['timeSeries'];
            if (!empty($ts['timeField'])) {
                $out['timeseries'] = [
                    'timeField' => (string) $ts['timeField'],
                ];
                if (!empty($ts['metaField'])) {
                    $out['timeseries']['metaField'] = (string) $ts['metaField'];
                }
                if (!empty($ts['granularity'])) {
                    $out['timeseries']['granularity'] = (string) $ts['granularity']; // 'seconds'|'minutes'|'hours'
                }
                if (isset($ts['bucketMaxSpanSeconds'])) {
                    $out['timeseries']['bucketMaxSpanSeconds'] = (int) $ts['bucketMaxSpanSeconds'];
                }
            }
            if (isset($opts['expireAfterSeconds'])) {
                $out['expireAfterSeconds'] = (int) $opts['expireAfterSeconds'];
            }
        }

        // Clustered index (MongoDB 5.3+)
        if (!empty($opts['clusteredIndex']) && is_array($opts['clusteredIndex'])) {
            $out['clusteredIndex'] = $opts['clusteredIndex'];
        }

        // View
        if (!empty($opts['viewOn'])) {
            $out['viewOn'] = (string) $opts['viewOn'];
        }
        if (!empty($opts['pipeline']) && is_array($opts['pipeline'])) {
            $out['pipeline'] = $opts['pipeline'];
        }

        return $out;
    }

    /**
     * Маппер опций collMod (modifyTable)
     */
    private function mapCollModOptions(array $opts): array
    {
        $out = [];
        if (isset($opts['validationLevel'])) {
            $out['validationLevel'] = (string) $opts['validationLevel']; // 'off'|'moderate'|'strict'
        }
        if (isset($opts['validationAction'])) {
            $out['validationAction'] = (string) $opts['validationAction']; // 'warn'|'error'
        }
        // capped → изменение размера
        if (!empty($opts['capped']) && isset($opts['size'])) {
            $out['cappedSize'] = (int) $opts['size']; // эквивалент collMod { collMod:X, cappedSize:Y }
        }
        // timeseries опции (частично зависят от версии)
        if (isset($opts['expireAfterSeconds'])) {
            $out['expireAfterSeconds'] = (int) $opts['expireAfterSeconds'];
        }
        // preAndPostImages (MongoDB 6.0+)
        if (isset($opts['recordPreAndPostImages'])) {
            $out['recordPreImages'] = (bool) $opts['recordPreAndPostImages'];
        }
        return $out;
    }

    /**
     * Маппер опций индексов для createIndexes
     */
    private function mapIndexOptions(array $opts): array
    {
        $out = [];

        // Базовые
        if (isset($opts['name'])) {
            $out['name'] = (string) $opts['name'];
        }
        if (isset($opts['unique'])) {
            $out['unique'] = (bool) $opts['unique'];
        }
        if (isset($opts['expireAfterSeconds'])) {
            $out['expireAfterSeconds'] = (int) $opts['expireAfterSeconds'];
        }

        // Расширенные
        if (!empty($opts['sparse'])) {
            $out['sparse'] = true;
        }
        if (!empty($opts['hidden'])) {
            $out['hidden'] = true;
        }
        if (!empty($opts['collation']) && is_array($opts['collation'])) {
            $out['collation'] = $opts['collation'];
        }
        if (!empty($opts['partialFilterExpression']) && is_array($opts['partialFilterExpression'])) {
            $out['partialFilterExpression'] = $opts['partialFilterExpression'];
        }
        if (!empty($opts['wildcardProjection']) && is_array($opts['wildcardProjection'])) {
            $out['wildcardProjection'] = $opts['wildcardProjection'];
        }
        // text index опции
        if (!empty($opts['weights']) && is_array($opts['weights'])) {
            $out['weights'] = $opts['weights'];
        }
        if (!empty($opts['default_language'])) {
            $out['default_language'] = (string) $opts['default_language'];
        }
        if (!empty($opts['language_override'])) {
            $out['language_override'] = (string) $opts['language_override'];
        }

        // storageEngine специфичные настройки
        if (!empty($opts['storageEngine']) && is_array($opts['storageEngine'])) {
            $out['storageEngine'] = $opts['storageEngine'];
        }

        return $out;
    }
    /**
     * Proxy to Connection::getFieldData() for consistency with Forge usage.
     */
    public function getFieldData(string $table): array
    {
        if (method_exists($this->db, 'getFieldData')) {
            /** @var Connection $conn */
            $conn = $this->db;
            return $conn->getFieldData($table);
        }
        return [];
    }
}
