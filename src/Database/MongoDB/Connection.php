<?php
declare(strict_types=1);

namespace CodeIgniter\Database\MongoDB;

use CodeIgniter\Database\BaseConnection;
use CodeIgniter\Database\BaseBuilder;
use CodeIgniter\Database\Exceptions\DatabaseException;
use CodeIgniter\Database\Query;
use CodeIgniter\Database\TableName;
use CodeIgniter\Events\Events;
use \JsonException;
use MongoDB\Client;
use MongoDB\Database as MongoDatabase;
use MongoDB\Driver\Cursor;
use MongoDB\Driver\Exception\CommandException;
use MongoDB\Driver\Session as DriverSession;
use MongoDB\Model\BSONDocument;
use stdClass;
use \Throwable;

/**
 * MongoDB Connection driver for CodeIgniter 4
 *
 * This provides a thin wrapper around MongoDB\Client so it can be obtained via
 * Database::connect() with DBDriver => 'MongoDB'.
 */
class Connection extends BaseConnection
{
    /**
     * Database driver
     *
     * @var string
     */
    public $DBDriver = 'MongoDB';

    /**
     * MongoDB Client instance
     *
     * @var Client|null
     * */
    public ?Client $client = null;

    /**
     * Database instance from Client
     *
     * @var MongoDatabase|null
     * */
    public ?MongoDatabase $mongoDB = null;

    /**
     * DriverSession instance - used for transactions
     *
     * @var DriverSession|null
     */
    public ?DriverSession $session = null;

    /**
     * URI options passed to MongoDB\Client
     *
     * @var array<string,mixed>
     */
    public array $options = [];

    /**
     * Last affected rows counter
     *
     * @var int|null
     */
    protected ?int $lastAffected = null;

    /**
     * Last inserted id
     *
     * @var string|null
     */
    protected ?string $lastInsertedId = null;

    /**
     * Last error code
     *
     * @var int|null
     */
    protected ?int $lastErrorCode = null;

    /**
     * Last error message
     *
     * @var string|null
     */
    protected ?string $lastErrorMessage = null;


    /**
     * Connect to the database.
     *
     * @return Client|false
     */
    public function connect(bool $persistent = false)
    {
        if ($this->client !== null) {
            return $this->client;
        }

        // Build MongoDB connection URI
        $dsn = $this->DSN;
        if ($dsn === '' || $dsn === null) {
            // Build from hostname/username/password/port
            $scheme = 'mongodb';
            // Allow mongodb+srv via subdriver
            if (! empty($this->subdriver) && $this->subdriver === 'srv') {
                $scheme = 'mongodb+srv';
            }

            $auth = '';
            if (($this->username ?? '') !== '' || ($this->password ?? '') !== '') {
                $auth = rawurlencode((string) $this->username) . ':' . rawurlencode((string) $this->password) . '@';
            }

            $host = ($this->hostname ?? '') !== '' ? $this->hostname : 'localhost';
            // mongodb+srv prohibits explicit port in the seed list
            if ($scheme === 'mongodb+srv') {
                $port = '';
            } else {
                $port = (string) $this->port !== '' ? ':' . (string) $this->port : '';
            }

            $dsn = sprintf('%s://%s%s%s', $scheme, $auth, $host, $port);
        }

        try {
            // Pass options array (URI options) if provided
            $uriOptions = is_array($this->options ?? null) ? $this->options : [];
            $this->client  = new Client($dsn, $uriOptions);
            $this->mongoDB = $this->client->selectDatabase($this->database);
        } catch (Throwable $e) {
            // Save last error
            $this->lastErrorCode = (int) $e->getCode();
            $this->lastErrorMessage = $e->getMessage();

            if ($this->DBDebug) {
                throw new DatabaseException('MongoDB connection failed: ' . $e->getMessage(), (int) $e->getCode(), $e);
            }

            return false;
        }

        return $this->client;
    }

    /**
     * Reconnect - for MongoDB just attempts to connect again.
     *
     * @return void
     */
    public function reconnect()
    {
        $this->_close();
        $this->initialize();
    }

    /**
     * Close connection
     *
     * @return void
     */
    protected function _close(): void
    {
        // MongoDB\Client has no explicit close; let GC handle it
        if ($this->session !== null) {
            $this->session->endSession();
            $this->session = null;
        }
        $this->client  = null;
        $this->mongoDB = null;
    }

    /**
     * Sets the database to be used by the client.
     *
     * @param string $databaseName The name of the database to set.
     * @return bool Returns true on successful database selection.
     */
    public function setDatabase(string $databaseName): bool
    {
        if ($databaseName === '') {
            $databaseName = $this->database;
        }

        if (empty($this->connID)) {
            $this->initialize();
        }

        try {
            $this->mongoDB = $this->client->selectDatabase($databaseName);
            $this->database = $databaseName;
            return true;
        } catch (\InvalidArgumentException $e) {
            if ($this->DBDebug) {
                throw new DatabaseException('MongoDB database selection failed: ' . $e->getMessage(), (int) $e->getCode(), $e);
            }
            return false;
        }
    }

    /**
     * MongoDB server version
     */
    public function getVersion(): string
    {
        try {
            //$buildInfo = $this->mongoDB?->command(['buildInfo' => 1])->toArray()[0] ?? null;
            $buildInfo = $this->runCommand(['buildInfo' => 1])->toArray()[0] ?? null;
            if ($buildInfo && isset($buildInfo->version)) {
                return (string) $buildInfo->version;
            }
        } catch (Throwable) {
            // ignore
        }

        return 'unknown';
    }

    /**
     * Executes a MongoDB command based on the provided JSON-encoded command string.
     *
     * @param string $sql The JSON-encoded string representing the MongoDB command.
     * @return mixed Returns a MongoDB driver cursor on successful execution or false on failure.
     *               If `$this->DBDebug` is enabled, exceptions are thrown on errors.
     */
    public function execute(string $sql)
    {
        // Инициализируем соединение при необходимости
        if ($this->mongoDB === null) {
            $this->connect();
        }

        // Пустая строка — ошибка
        if (trim($sql) === '') {
            $this->lastErrorCode = null;
            $this->lastErrorMessage = 'Empty command JSON.';
            if ($this->DBDebug) {
                throw new DatabaseException($this->lastErrorMessage);
            }
            return false;
        }

        try {
            // Парсим JSON с исключениями
            $decoded = json_decode($sql, true, 512, JSON_THROW_ON_ERROR);
        } catch (JsonException $e) {
            $this->lastErrorCode = (int) $e->getCode();
            $this->lastErrorMessage = 'Invalid JSON: ' . $e->getMessage();
            if ($this->DBDebug) {
                throw new DatabaseException($this->lastErrorMessage, (int) $e->getCode(), $e);
            }
            return false;
        }
        if (! is_array($decoded)) {
            $this->lastErrorCode = null;
            $this->lastErrorMessage = 'Command JSON must decode to an object.';
            if ($this->DBDebug) {
                throw new DatabaseException($this->lastErrorMessage);
            }
            return false;
        }

        $options = [];
        $commandDoc = null;

        // Поддержка вложенного формата {"command": {...}, "options": {...}}
        if (array_key_exists('command', $decoded)) {
            if (! is_array($decoded['command']) || $decoded['command'] === []) {
                $this->lastErrorCode = null;
                $this->lastErrorMessage = 'Invalid "command" document.';
                if ($this->DBDebug) {
                    throw new DatabaseException($this->lastErrorMessage);
                }
                return false;
            }
            $commandDoc = $decoded['command'];

            if (array_key_exists('options', $decoded)) {
                if (! is_array($decoded['options'])) {
                    $this->lastErrorCode = null;
                    $this->lastErrorMessage = '"options" must be an object.';
                    if ($this->DBDebug) {
                        throw new DatabaseException($this->lastErrorMessage);
                    }
                    return false;
                }
                $options = $decoded['options'];
            }
        } else {
            // Плоский формат: команда + возможный ключ "options" на верхнем уровне
            $commandDoc = $decoded;
            if (array_key_exists('options', $commandDoc)) {
                if (! is_array($commandDoc['options'])) {
                    $this->lastErrorCode = null;
                    $this->lastErrorMessage = '"options" must be an object.';
                    if ($this->DBDebug) {
                        throw new DatabaseException($this->lastErrorMessage);
                    }
                    return false;
                }
                $options = $commandDoc['options'];
                unset($commandDoc['options']);
            }

            if (! is_array($commandDoc) || $commandDoc === []) {
                $this->lastErrorCode = null;
                $this->lastErrorMessage = 'Command document is empty.';
                if ($this->DBDebug) {
                    throw new DatabaseException($this->lastErrorMessage);
                }
                return false;
            }
        }

        // Нормализация документа команды под требования MongoDB драйвера
        $toObject = static function ($v) {
            // Ассоциативные массивы безопасно конвертируются в BSON-документы автоматически,
            // пустой массив [] нужно привести к {}. Для надёжности приведём всё, что "похоже на док".
            if (is_array($v)) {
                // Пустой массив или неассоциативный — приводим к пустому объекту
                $isAssoc = false;
                $i = 0;
                foreach ($v as $k => $_) {
                    if ($k !== $i++) { $isAssoc = true; break; }
                }
                if (! $isAssoc) {
                    return new \stdClass();
                }
                return (object) $v;
            }
            if ($v === null) {
                return new \stdClass();
            }
            return $v;
        };

        // Преобразуем Extended JSON → BSON-типы (ObjectId, UTCDateTime и пр.)
        $commandDoc = $this->convertExtendedJsonToBson($commandDoc);

        // НАТИВНАЯ ВЕТКА: INSERT (insertOne/insertMany), не ломая lastQuery/коллектор
        if (isset($commandDoc['insert']) && isset($commandDoc['documents']) && is_array($commandDoc['documents'])) {
            if (! $this->mongoDB instanceof MongoDatabase) {
                $this->connect();
            }
            $collectionName = (string) $commandDoc['insert'];
            $docs = $commandDoc['documents'];

            // Добавим _id тем документам, где он не задан — чтобы insertID() можно было вернуть
            foreach ($docs as $i => $d) {
                if (is_array($d) && !array_key_exists('_id', $d)) {
                    $docs[$i]['_id'] = new \MongoDB\BSON\ObjectId();
                }
            }

            // Приведём значения к BSON-типам (даты/время и пр.)
            $normalize = function ($v) use (&$normalize) {
                if ($v instanceof \DateTimeInterface) {
                    $utc = (new \DateTimeImmutable('@' . $v->getTimestamp()))->setTimezone(new \DateTimeZone('UTC'));
                    return new \MongoDB\BSON\UTCDateTime($utc->getTimestamp() * 1000);
                }
                if (is_string($v)) {
                    $isIso = preg_match('/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}/', $v);
                    $isClassic = preg_match('/^\d{4}-\d{2}-\d{2}(?:\s+\d{2}:\d{2}:\d{2})?$/', $v);
                    if ($isIso || $isClassic) {
                        $ts = strtotime($v);
                        if ($ts !== false) {
                            return new \MongoDB\BSON\UTCDateTime(((int)$ts) * 1000);
                        }
                    }
                    return $v;
                }
                if (is_array($v)) {
                    $out = [];
                    foreach ($v as $k => $vv) {
                        $out[$k] = $normalize($vv);
                    }
                    return $out;
                }
                if ($v instanceof \stdClass) {
                    foreach (get_object_vars($v) as $k => $vv) {
                        $v->$k = $normalize($vv);
                    }
                    return $v;
                }
                return $v;
            };

            // Приведение типов по $jsonSchema коллекции (int/bool/array/object)
            $coerceBySchema = function (string $coll, array $doc) use (&$normalize): array {
                try {
                    $schema = $this->_getValidatorScheme($coll);
                } catch (\Throwable) {
                    $schema = [];
                }
                if (!is_array($schema) || empty($schema['properties']) || !is_array($schema['properties'])) {
                    return $doc;
                }
                $props = $schema['properties'];
                foreach ($props as $name => $spec) {
                    if (!array_key_exists($name, $doc)) {
                        continue;
                    }
                    $bsonType = $spec['bsonType'] ?? ($spec['type'] ?? null);
                    $target = is_array($bsonType) ? (string) reset($bsonType) : (string) $bsonType;
                    $val = $doc[$name];

                    // null оставляем как есть (nullable допускается схемой)
                    if ($val === null) {
                        continue;
                    }

                    if ($target === 'date') {
                        $doc[$name] = $val; // даты обработает normalize
                        continue;
                    }
                    if ($target === 'objectId') {
                        $doc[$name] = $val;
                        continue;
                    }

                    switch ($target) {
                        case 'int':
                            $doc[$name] = is_numeric($val) ? (int) $val : $val;
                            break;
                        case 'double':
                            $doc[$name] = is_numeric($val) ? (float) $val : $val;
                            break;
                        case 'bool':
                            $doc[$name] = (bool) $val;
                            break;
                        case 'array':
                            // null уже отсеяли; только скаляр → массив
                            if (! is_array($val)) {
                                $doc[$name] = [$val];
                            }
                            break;
                        case 'object':
                            if (is_array($val) || $val instanceof \stdClass) {
                                $doc[$name] = $val;
                            } else {
                                $doc[$name] = ['value' => $val];
                            }
                            break;
                        case 'string':
                            if (is_scalar($val) && !($val instanceof \Stringable)) {
                                $doc[$name] = (string) $val;
                            }
                            break;
                        default:
                            break;
                    }
                }
                return $normalize($doc);
            };

            foreach ($docs as $i => $d) {
                $d = $normalize($d);
                if (is_array($d)) {
                    $d = $coerceBySchema($collectionName, $d);
                }
                $docs[$i] = $d;
            }



            $coll = $this->mongoDB->selectCollection($collectionName);

            // Проводим операцию нативно
            if (count($docs) === 1) {
                $res = $coll->insertOne($docs[0]);
                // Зафиксировать метаданные
                $this->recordInsertResult($res->getInsertedId(), (int) $res->getInsertedCount());
                return $res; // Для write query() вернёт true, resultID тип не критичен
            } else {
                $res = $coll->insertMany($docs);
                $this->recordInsertResult(null, (int) $res->getInsertedCount());
                return $res;
            }
        }


        // aggregate: cursor обязан быть объектом
        if (isset($commandDoc['aggregate'])) {
            if (!isset($commandDoc['cursor'])) {
                $commandDoc['cursor'] = new \stdClass();
            } elseif (is_array($commandDoc['cursor'])) {
                $commandDoc['cursor'] = (object) $commandDoc['cursor'];
            }
        }

        // find: filter должен быть объектом-документом
        if (isset($commandDoc['find'])) {
            if (!isset($commandDoc['filter'])) {
                $commandDoc['filter'] = new \stdClass();
            } else {
                $commandDoc['filter'] = $toObject($commandDoc['filter']);
            }
        }

        // delete: deletes[].q должен быть документом
        if (isset($commandDoc['delete']) && isset($commandDoc['deletes']) && is_array($commandDoc['deletes'])) {
            foreach ($commandDoc['deletes'] as $idx => $del) {
                if (is_array($del)) {
                    if (!array_key_exists('q', $del)) {
                        $del['q'] = new \stdClass();
                    } else {
                        $del['q'] = $toObject($del['q']);
                    }
                    // limit приведём к int по спецификации
                    if (isset($del['limit'])) {
                        $del['limit'] = (int) $del['limit'];
                    }
                    $commandDoc['deletes'][$idx] = $del;
                }
            }
        }

        // update: updates[].q и updates[].u должны быть документами
        if (isset($commandDoc['update']) && isset($commandDoc['updates']) && is_array($commandDoc['updates'])) {
            // Нормализатор значений (даты → UTCDateTime), рекурсивно
            // Нормализатор значений (даты → UTCDateTime), рекурсивно
            $normalize = function ($v) use (&$normalize) {
                if ($v instanceof \DateTimeInterface) {
                    $utc = (new \DateTimeImmutable('@' . $v->getTimestamp()))->setTimezone(new \DateTimeZone('UTC'));
                    return new \MongoDB\BSON\UTCDateTime($utc->getTimestamp() * 1000);
                }
                if (is_string($v)) {
                    $isIso = preg_match('/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}/', $v);
                    $isClassic = preg_match('/^\d{4}-\d{2}-\d{2}(?:\s+\d{2}:\d{2}:\d{2})?$/', $v);
                    if ($isIso || $isClassic) {
                        $ts = strtotime($v);
                        if ($ts !== false) {
                            return new \MongoDB\BSON\UTCDateTime(((int)$ts) * 1000);
                        }
                    }
                    return $v;
                }
                if (is_array($v)) {
                    $out = [];
                    foreach ($v as $k => $vv) {
                        $out[$k] = $normalize($vv);
                    }
                    return $out;
                }
                if ($v instanceof \stdClass) {
                    foreach (get_object_vars($v) as $k => $vv) {
                        $v->$k = $normalize($vv);
                    }
                    return $v;
                }
                return $v;
            };

            // Коэрсинг по схеме для u/$set (age, active и т.п.)
            $coerceUpdateBySchema = function (string $coll, $u) {
                try {
                    $schema = $this->_getValidatorScheme($coll);
                } catch (\Throwable) {
                    $schema = [];
                }
                if (!is_array($schema) || empty($schema['properties']) || !is_array($schema['properties'])) {
                    return $u;
                }
                $props = $schema['properties'];

                $applyCoerce = function (array $data) use ($props) {
                    foreach ($data as $name => &$val) {
                        if (!array_key_exists($name, $props)) {
                            continue;
                        }
                        $bsonType = $props[$name]['bsonType'] ?? ($props[$name]['type'] ?? null);
                        $target = is_array($bsonType) ? (string) reset($bsonType) : (string) $bsonType;

                        // null оставляем как есть — соответствует nullable по схеме
                        if ($val === null) {
                            continue;
                        }

                        if ($target === 'date' || $target === 'objectId') {
                            continue; // даты/oid обработаются отдельно
                        }

                        switch ($target) {
                            case 'int':
                                $val = is_numeric($val) ? (int) $val : $val;
                                break;
                            case 'double':
                                $val = is_numeric($val) ? (float) $val : $val;
                                break;
                            case 'bool':
                                $val = (bool) $val;
                                break;
                            case 'array':
                                if (! is_array($val)) { $val = [$val]; }
                                break;
                            case 'object':
                                if (is_array($val) || $val instanceof \stdClass) {
                                    // ok
                                } else {
                                    $val = ['value' => $val];
                                }
                                break;
                            case 'string':
                                if (is_scalar($val) && !($val instanceof \Stringable)) {
                                    $val = (string) $val;
                                }
                                break;
                        }
                    }
                    unset($val);
                    return $data;
                };

                // поддержим основные операторы: $set, $setOnInsert
                if (is_array($u)) {
                    if (isset($u['$set']) && is_array($u['$set'])) {
                        $u['$set'] = $applyCoerce($u['$set']);
                    }
                    if (isset($u['$setOnInsert']) && is_array($u['$setOnInsert'])) {
                        $u['$setOnInsert'] = $applyCoerce($u['$setOnInsert']);
                    }
                } elseif ($u instanceof \stdClass) {
                    // преобразуем в массив, применим, вернём обратно
                    $arr = (array) $u;
                    if (isset($arr['$set']) && is_array($arr['$set'])) {
                        $arr['$set'] = $applyCoerce($arr['$set']);
                    }
                    if (isset($arr['$setOnInsert']) && is_array($arr['$setOnInsert'])) {
                        $arr['$setOnInsert'] = $applyCoerce($arr['$setOnInsert']);
                    }
                    $u = (object) $arr;
                }
                return $u;
            };

            foreach ($commandDoc['updates'] as $idx => $upd) {
                if (is_array($upd)) {
                    $upd['q'] = array_key_exists('q', $upd) ? $toObject($upd['q']) : new \stdClass();
                    $upd['u'] = array_key_exists('u', $upd) ? $toObject($upd['u']) : new \stdClass();

                    // Коэрсинг по схеме текущей коллекции
                    if (isset($commandDoc['update']) && is_string($commandDoc['update'])) {
                        $upd['u'] = $coerceUpdateBySchema((string)$commandDoc['update'], $upd['u']);
                    }
                    // Нормализация дат внутри u
                    if (is_object($upd['u']) || is_array($upd['u'])) {
                        $upd['u'] = $normalize($upd['u']);
                    }

                    $commandDoc['updates'][$idx] = $upd;
                }
            }
        }


        // count: query тоже должен быть BSON-документом
        if (isset($commandDoc['count'])) {
            if (!isset($commandDoc['query'])) {
                $commandDoc['query'] = new \stdClass();
            } else {
                $commandDoc['query'] = $toObject($commandDoc['query']);
            }
        }


        // Добавляем активную сессию (для транзакций), если есть
        if ($this->session instanceof DriverSession) {
            $options['session'] = $this->session;
        }

        try {
            // Выполняем команду
            //$cursor = $this->mongoDB->command($commandDoc, $options);
            $cursor = $this->runCommand($commandDoc, $options);

            return $cursor;
        } catch (CommandException $e) {
            $this->lastErrorCode = (int) $e->getCode();
            $this->lastErrorMessage = $e->getMessage();
            if ($this->DBDebug) {
                throw new DatabaseException('MongoDB command failed: ' . $e->getMessage(), (int) $e->getCode(), $e);
            }
            return false;
        } catch (Throwable $e) {
            $this->lastErrorCode = (int) $e->getCode();
            $this->lastErrorMessage = $e->getMessage();
            if ($this->DBDebug) {
                throw new DatabaseException('MongoDB command error: ' . $e->getMessage(), (int) $e->getCode(), $e);
            }
            return false;
        }

        return false;
    }

    /**
     * Сохранение метаданных последней операции вставки для insertID()/affectedRows().
     */
    public function recordInsertResult($insertedId, int $affected = 1): void
    {
        try {
            // insertedId может быть ObjectId, \MongoDB\BSON\Binary, строка и т.п.
            $this->lastInsertedId = $insertedId !== null ? (string) $insertedId : null;
        } catch (\Throwable) {
            $this->lastInsertedId = null;
        }
        $this->lastAffected = $affected > 0 ? $affected : 0;
    }


    /**
     * Выполнить произвольную команду MongoDB.
     * Если команда — renameCollection, она должна выполняться в базе admin.
     */
    public function runCommand(array $command)
    {
        $firstKey = (string) (is_string(array_key_first($command)) ? array_key_first($command) : '');
        $useAdmin = ($firstKey === 'renameCollection');

        // Выбираем правильную БД для выполнения команды.
        $db = $useAdmin
            ? $this->client->selectDatabase('admin')
            : $this->mongoDB;

        // Пробрасываем как DatabaseCommand, чтобы сохранить текущую механику ошибок
        try {
            return $db->command($command);
        } catch (\MongoDB\Driver\Exception\CommandException $e) {
            throw new \CodeIgniter\Database\Exceptions\DatabaseException('MongoDB command failed: ' . $e->getMessage(), previous: $e);
        }
    }


    /**
     * Рекурсивный конвертер Extended JSON → BSON.
     * Поддерживает ключевые случаи: {"$oid": "..."} → ObjectId, {"$date": "..."/millis} → UTCDateTime,
     * а также массивы/документы произвольной вложенности.
     */
    private function convertExtendedJsonToBson($value)
    {
        // Примитивы
        if (!is_array($value)) {
            return $value;
        }

        // Документ c единственным ключом $oid
        if (count($value) === 1 && array_key_exists('$oid', $value) && is_string($value['$oid'])) {
            try {
                return new \MongoDB\BSON\ObjectId($value['$oid']);
            } catch (\Throwable) {
                // если невалидный — вернём как есть
                return $value;
            }
        }

        // Документ с единственным ключом $date (строка ISO или {"$numberLong":"millis"})
        if (count($value) === 1 && array_key_exists('$date', $value)) {
            $d = $value['$date'];
            try {
                if (is_string($d)) {
                    // ISO8601 строка
                    $ts = (new \DateTimeImmutable($d))->getTimestamp() * 1000;
                    return new \MongoDB\BSON\UTCDateTime($ts);
                }
                if (is_array($d) && isset($d['$numberLong'])) {
                    $ms = (int) $d['$numberLong'];
                    return new \MongoDB\BSON\UTCDateTime($ms);
                }
            } catch (\Throwable) {
                return $value;
            }
        }

        // Иначе обрабатываем рекурсивно массив/документ
        $out = [];
        foreach ($value as $k => $v) {
            if (is_array($v)) {
                $out[$k] = $this->convertExtendedJsonToBson($v);
            } else {
                $out[$k] = $v;
            }
        }
        return $out;
    }


    public function query(string $sql, $binds = null, bool $setEscapeFlags = true, string $queryClass = '')
    {
        $queryClass = $queryClass !== '' && $queryClass !== '0' ? $queryClass : $this->queryClass;

        if (empty($this->connID)) {
            $this->initialize();
        }

        /** @var Query $query */
        $query = new $queryClass($this);
        $query->setQuery($sql, $binds, $setEscapeFlags);

        if (! empty($this->swapPre) && ! empty($this->DBPrefix)) {
            $query->swapPrefix($this->DBPrefix, $this->swapPre);
        }

        $startTime = microtime(true);

        // Сохраняем последний запрос
        $this->lastQuery = $query;

        // Режим имитации — вернуть объект запроса без выполнения
        if ($this->pretend) {
            $query->setDuration($startTime);
            return $query;
        }

        // Выполняем команду
        $exception = null;
        try {
            $this->resultID = $this->execute($query->getQuery());
        } catch (DatabaseException $exception) {
            $this->resultID = false;
        }

        // Обработка неуспеха
        if ($this->resultID === false) {
            $query->setDuration($startTime, $startTime);

            // Отметить ошибку для транзакций
            $this->handleTransStatus();

            if (
                $this->DBDebug
                && ($this->transDepth === 0 || $this->transException)
            ) {
                // Попытка завершить «зависшие» транзакции
                while ($this->transDepth !== 0) {
                    $transDepth = $this->transDepth;
                    $this->transComplete();

                    if ($transDepth === $this->transDepth) {
                        log_message('error', 'Database: Failure during an automated transaction commit/rollback!');
                        break;
                    }
                }

                // Событие запроса
                Events::trigger('DBQuery', $query);

                if ($exception instanceof DatabaseException) {
                    throw new DatabaseException(
                        $exception->getMessage(),
                        $exception->getCode(),
                        $exception
                    );
                }

                return false;
            }

            // Событие запроса
            Events::trigger('DBQuery', $query);
            return false;
        }

        // Успех
        $query->setDuration($startTime);
        Events::trigger('DBQuery', $query);

        // Определяем тип команды (write/read) по JSON-команде
        $isWrite = false;
        $cmdName = null;
        $commandDoc = null;
        try {
            $decoded = json_decode($sql, true, 512, JSON_THROW_ON_ERROR);
            if (is_array($decoded)) {
                if (array_key_exists('command', $decoded) && is_array($decoded['command'])) {
                    $commandDoc = $decoded['command'];
                } else {
                    $commandDoc = $decoded;
                    unset($commandDoc['options']);
                }

                if (is_array($commandDoc) && $commandDoc !== []) {
                    $keys = array_keys($commandDoc);
                    $cmdName = strtolower((string) ($keys[0] ?? ''));
                }

                // Набор write-команд
                $writeCommands = [
                    'insert', 'update', 'delete',
                    'findandmodify', 'findandmodify', // разные кейсы
                    'create', 'drop', 'createindexes', 'dropindexes',
                    'renamecollection', 'collmod',
                ];

                if ($cmdName !== null) {
                    $isWrite = in_array($cmdName, $writeCommands, true);

                    // aggregate считаем read, если нет $out
                    if ($cmdName === 'aggregate') {
                        $isWrite = isset($commandDoc['$out']) || (isset($commandDoc['out']) && $commandDoc['out'] !== null);
                    }
                }
            }
        } catch (\JsonException) {
            // Если не распарсили — по умолчанию считаем read
            $isWrite = false;
        }

        // Для write-команд возвращаем true и пытаемся обновить служебные поля
        if ($isWrite) {
            // Пытаемся получить количество затронутых документов из ответа команды
            try {
                if ($this->resultID instanceof Cursor) {
                    $docs = $this->resultID->toArray();
                    $first = $docs[0] ?? null;
                    if ($first instanceof BSONDocument || is_array($first)) {
                        $arr = is_array($first) ? $first : $first->getArrayCopy();
                        // n, nModified, deletedCount и т.п.
                        $this->lastAffected = (int) (
                            $arr['n'] ??
                            $arr['nModified'] ??
                            $arr['deletedCount'] ??
                            $arr['modifiedCount'] ??
                            $arr['insertedCount'] ??
                            0
                        );
                        // lastInsertedId недоступен в raw command стабильно — сбрасываем
                        $this->lastInsertedId = null;
                    }
                }
            } catch (Throwable) {
                // игнорируем метаданные, если не удалось
            }
            return true;
        }

        // Иначе — read: вернуть Result
        $resultClass = str_replace('Connection', 'Result', static::class);
        return new $resultClass($this->connID, $this->resultID);
    }

    public function simpleQuery(string $sql)
    {
        return $this->execute($sql);
    }

    public function affectedRows(): int
    {
        // Если уже вычисляли в query() — вернём закешированное значение
        if ($this->lastAffected > 0) {
            return $this->lastAffected;
        }

        // Пытаемся определить по последнему запросу и результату, только для write-команд
        try {
            $lastSql = $this->lastQuery ? $this->lastQuery->getQuery() : '';
            if ($lastSql !== '') {
                $decoded = json_decode($lastSql, true, 512, JSON_THROW_ON_ERROR);
                if (is_array($decoded)) {
                    // Выделяем документ команды
                    $commandDoc = isset($decoded['command']) && is_array($decoded['command'])
                        ? $decoded['command']
                        : (is_array($decoded) ? $decoded : []);

                    // Убираем options с плоского уровня
                    if (isset($commandDoc['options'])) {
                        unset($commandDoc['options']);
                    }

                    // Имя команды
                    $keys   = array_keys($commandDoc);
                    $cmd    = strtolower((string) ($keys[0] ?? ''));
                    $isWrite = in_array($cmd, [
                            'insert', 'update', 'delete',
                            'findandmodify', 'findandmodify',
                            'create', 'drop', 'createindexes', 'dropindexes',
                            'renamecollection', 'collmod',
                        ], true) || ($cmd === 'aggregate' && (isset($commandDoc['$out']) || isset($commandDoc['out'])));

                    if ($isWrite && $this->resultID instanceof \MongoDB\Driver\Cursor) {
                        // Команды записи возвращают одиночный документ статуса, его безопасно прочитать
                        $docs  = $this->resultID->toArray();
                        $first = $docs[0] ?? null;

                        if ($first instanceof \MongoDB\Model\BSONDocument || is_array($first)) {
                            $arr = is_array($first) ? $first : $first->getArrayCopy();

                            // Популярные поля счётчиков в ответах команд
                            $this->lastAffected = (int) (
                                $arr['n'] ??
                                $arr['nModified'] ??
                                $arr['modifiedCount'] ??
                                $arr['deletedCount'] ??
                                $arr['insertedCount'] ??
                                $arr['upsertedCount'] ??
                                0
                            );
                        }
                    }
                }
            }
        } catch (\Throwable) {
            // Мягко игнорируем любые проблемы при попытке вычисления
        }

        return $this->lastAffected;

    }

    /**
     * Returns the last error code and message.
     *
     * Must return an array with keys 'code' and 'message':
     *
     * @return array{code: int|string|null, message: string|null}
     */
    public function error(): array
    {
        return [
            'code'    => $this->lastErrorCode ?? 0,
            'message' => $this->lastErrorMessage ?? '',
        ];

    }

    public function insertID()
    {
        // Если ранее уже был сохранён ID — возвращаем его
        if (!empty($this->lastInsertedId)) {
            return $this->lastInsertedId;
        }

        try {
            $lastSql = $this->lastQuery ? $this->lastQuery->getQuery() : '';
            if ($lastSql === '' || !($this->resultID instanceof \MongoDB\Driver\Cursor)) {
                return '';
            }

            $decoded = json_decode($lastSql, true, 512, JSON_THROW_ON_ERROR);
            if (!is_array($decoded)) {
                return '';
            }

            // Выделяем документ команды
            $commandDoc = isset($decoded['command']) && is_array($decoded['command'])
                ? $decoded['command']
                : (is_array($decoded) ? $decoded : []);

            if (isset($commandDoc['options'])) {
                unset($commandDoc['options']);
            }

            // Имя команды
            $keys   = array_keys($commandDoc);
            $cmd    = strtolower((string) ($keys[0] ?? ''));

            // Документ ответа (команд записи обычно возвращают 1 документ статуса)
            $docs  = $this->resultID->toArray();
            $first = $docs[0] ?? null;
            $resp  = is_array($first)
                ? $first
                : ($first instanceof \MongoDB\Model\BSONDocument ? $first->getArrayCopy() : null);

            d($resp);

            if (!is_array($resp)) {
                return '';
            }

            // 1) insert: пытаемся взять insertedIds или _id из исходного документа (если был один)
            if ($cmd === 'insert') {
                // В ответе некоторых версий доступен insertedIds
                if (isset($resp['insertedIds']) && is_array($resp['insertedIds']) && $resp['insertedIds'] !== []) {
                    $any = reset($resp['insertedIds']);
                    $this->lastInsertedId = (string) $any;
                    return $this->lastInsertedId;
                }

                // Если вставляли один документ — часто можно взять _id из отправленного документа
                if (isset($commandDoc['insert']) && isset($commandDoc['documents']) && is_array($commandDoc['documents'])) {
                    if (count($commandDoc['documents']) === 1) {
                        $doc = $commandDoc['documents'][0];
                        if (is_array($doc) && isset($doc['_id'])) {
                            $this->lastInsertedId = (string) $doc['_id'];
                            return $this->lastInsertedId;
                        }
                    }
                }
            }

            // 2) update с upsert: upserted -> [{index:0, _id: ObjectId(...)}]
            if ($cmd === 'update' && isset($resp['upserted']) && is_array($resp['upserted']) && $resp['upserted'] !== []) {
                $firstUpsert = $resp['upserted'][0] ?? null;
                if (is_array($firstUpsert) && isset($firstUpsert['_id'])) {
                    $this->lastInsertedId = (string) $firstUpsert['_id'];
                    return $this->lastInsertedId;
                }
            }

            // 3) findAndModify с upsert: lastErrorObject.upserted
            if ($cmd === 'findandmodify') {
                if (isset($resp['lastErrorObject']) && is_array($resp['lastErrorObject']) && isset($resp['lastErrorObject']['upserted'])) {
                    $this->lastInsertedId = (string) $resp['lastErrorObject']['upserted'];
                    return $this->lastInsertedId;
                }
            }
        } catch (\Throwable) {
            // молча возвращаем пустую строку
        }

        return '';

    }

    public function escapeString($str, bool $like = false)
    {
        // Массивы — обрабатываем рекурсивно
        if (is_array($str)) {
            foreach ($str as $key => $val) {
                $str[$key] = $this->escapeString($val, $like);
            }
            return $str;
        }

        // Поддержка Stringable и RawSql
        if ($str instanceof \Stringable) {
            if ($str instanceof \CodeIgniter\Database\RawSql) {
                return $str->__toString();
            }
            $str = (string) $str;
        }

        // Приводим к строке и применяем платформозависимое экранирование
        $str = $this->_escapeString((string) $str);

        // Для условий "LIKE" в MongoDB используются регулярные выражения:
        // экранируем все спецсимволы regex, чтобы интерпретировать строку буквально.
        if ($like) {
            return preg_quote($str, '/');
        }

        return $str;
    }

    /**
     * Platform-dependant string escape
     */
    protected function _escapeString(string $str): string
    {
        return $str;
    }


    /**
     * Begin Transaction
     */
    protected function _transBegin(): bool
    {
        try {
            if (! $this->client) {
                $this->connect();
            }
            $this->session = $this->client->startSession();
            $this->session->startTransaction();
            return true;
        } catch (Throwable $e) {
            if ($this->DBDebug) {
                throw new DatabaseException('MongoDB transaction begin failed: ' . $e->getMessage(), (int) $e->getCode(), $e);
            }
            return false;
        }
    }

    /**
     * Commit Transaction
     */
    protected function _transCommit(): bool
    {
        try {
            if ($this->session) {
                $this->session->commitTransaction();
                $this->session = null;
            }
            return true;
        } catch (Throwable $e) {
            if ($this->DBDebug) {
                throw new DatabaseException('MongoDB transaction commit failed: ' . $e->getMessage(), (int) $e->getCode(), $e);
            }
            return false;
        }
    }

    /**
     * Rollback Transaction
     */
    protected function _transRollback(): bool
    {
        try {
            if ($this->session) {
                $this->session->abortTransaction();
                $this->session = null;
            }
            return true;
        } catch (Throwable $e) {
            if ($this->DBDebug) {
                throw new DatabaseException('MongoDB transaction rollback failed: ' . $e->getMessage(), (int) $e->getCode(), $e);
            }
            return false;
        }
    }

    /**
     * Generates the SQL for listing tables in a platform-dependent manner.
     *
     * @param string|null $tableName If $tableName is provided will return only this table if exists.
     *
     * @return false|string
     */
    protected function _listTables(bool $constrainByPrefix = false, ?string $tableName = null)
    {
        // Build MongoDB listCollections command as JSON string for our query() executor.
        $filter = [];

        if ((string) $tableName !== '') {
            // Exact match by collection name
            $filter['name'] = $tableName;
        } elseif ($constrainByPrefix && $this->DBPrefix !== '') {
            // Limit by prefix using regex
            $filter['name'] = ['$regex' => '^' . preg_quote($this->DBPrefix, '/')];
        }

        $cmd = [
            'listCollections' => 1,
            'nameOnly'        => true,
        ];
        if ($filter !== []) {
            $cmd['filter'] = $filter;
        }

        // Our execute() accepts plain command JSON object.
        return json_encode($cmd, JSON_UNESCAPED_SLASHES);
    }

    /**
     * Helper: получить данные валидатора ($jsonSchema) коллекции.
     * Возвращает массив схемы валидатора или пустой массив, если валидатора нет.
     *
     * @return array<string,mixed>
     */
    protected function _getValidatorScheme(string $table): array
    {
        $jsonSchema = [];

        try {
            // Иначе пробуем получить через команду listCollections
            if ($this->mongoDB === null) {
                $this->connect();
            }

            $prefixed = $this->protectIdentifiers($table, true, false, false);
            $cmd = [
                'listCollections' => 1,
                'filter'          => ['name' => $prefixed],
                'nameOnly'        => false,
            ];
            $res = $this->query(json_encode($cmd, JSON_UNESCAPED_SLASHES));
            $rows = is_object($res) && method_exists($res, 'getResultArray') ? $res->getResultArray() : [];

            // В ответе ищем options.validator.$jsonSchema
            foreach ($rows as $row) {
                $options   = $row['options'] ?? [];
                $validator = is_array($options) ? ($options['validator'] ?? []) : [];
                $schema    = is_array($validator) ? ($validator['$jsonSchema'] ?? []) : [];
                if (is_array($schema) && $schema !== []) {
                    $jsonSchema = $schema;
                    break;
                }
            }
        } catch (\Throwable) {
            // Тихо игнорируем и возвращаем пустой массив — вызывающий сделает fallback
        }

        return $jsonSchema;
    }

    /**
     * Public wrapper used by Forge: returns collection's validator $jsonSchema (or empty array if none).
     *
     * @return array<string,mixed>
     */
    public function getCollectionSchema(string $table): array
    {
        return $this->_getValidatorScheme($table);
    }

    /**
     * Helper: получить список полей из схемы валидатора коллекции.
     * Возвращает список имён полей или пустой массив, если схемы нет/она не содержит properties.
     *
     * @return list<string>
     */
    protected function _listColumnsFromScheme(string $table): array
    {
        $fields = [];
        $schema = $this->_getValidatorScheme($table);
        if (isset($schema['properties']) && is_array($schema['properties'])) {
            foreach (array_keys($schema['properties']) as $name) {
                $name = (string) $name;
                if ($name !== '' && !in_array($name, $fields, true)) {
                    $fields[] = $name;
                }
            }
        }
        return $fields;
    }


    /**
     * Generates a platform-specific query string so that the column names can be fetched.
     *
     * @param string|TableName $table
     *
     * @return false|string
     */
    protected function _listColumns($table = '')
    {
        // Accept CodeIgniter's TableName wrapper
        $tableName = ($table instanceof TableName) ? $table->getTableName() : (string) $table;
        if ($tableName === '') {
            return false;
        }

        // Use aggregation to sample one document and unwind fields into rows with column_name
        $pipeline = [
            [ '$limit' => 1 ],
            [ '$project' => [ 'kv' => [ '$objectToArray' => '$$ROOT' ] ] ],
            [ '$unwind' => '$kv' ],
            [ '$project' => [ 'column_name' => '$kv.k' ] ],
        ];

        $cmd = [
            'aggregate' => $tableName,
            'pipeline'  => $pipeline,
            'cursor'    => new stdClass(),
        ];

        return json_encode($cmd, JSON_UNESCAPED_SLASHES);
    }
    /**
 * Platform-specific field data information.
 *
 * @return list<stdClass>
 * @see getFieldData()
 *
 */
    protected function _fieldData(string $table): array
    {
        $tableName = (string) $table;
        if ($tableName === '') {
            return [];
        }

        // Build aggregation to extract field names and BSON types from a sample document
        $pipeline = [
            [ '$limit' => 1 ],
            [ '$project' => [ 'kv' => [ '$objectToArray' => '$$ROOT' ] ] ],
            [ '$unwind' => '$kv' ],
            [ '$project' => [ 'name' => '$kv.k', '_mongo_type' => [ '$type' => '$kv.v' ] ] ],
        ];

        $cmd = [
            'aggregate' => $tableName,
            'pipeline'  => $pipeline,
            'cursor'    => new stdClass(),
        ];

        $result = $this->query(json_encode($cmd, JSON_UNESCAPED_SLASHES));
        $rows   = is_object($result) ? $result->getResultArray() : [];

        $out = [];
        foreach ($rows as $row) {
            $o = new stdClass();
            $o->name        = $row['name'] ?? ($row['column_name'] ?? array_values($row)[0] ?? '');
            $o->type        = $row['_mongo_type'] ?? null;
            $o->max_length  = null;
            $o->default     = null;
            $o->nullable    = true; // MongoDB is schemaless
            $o->primary_key = (int) ($o->name === '_id');
            $out[] = $o;
        }

        return $out;
    }

    /**
     * Platform-specific index data.
     *
     * @return array<string, stdClass>
     * @see    getIndexData()
     *
     */
    protected function _indexData(string $table): array
    {
        $tableName = (string) $table;
        if ($tableName === '') {
            return [];
        }

        $cmd = [ 'listIndexes' => $tableName ];
        $result = $this->query(json_encode($cmd, JSON_UNESCAPED_SLASHES));
        $rows   = is_object($result) ? $result->getResultArray() : [];

        $keys = [];
        foreach ($rows as $row) {
            $name = (string) ($row['name'] ?? '');
            if ($name === '') {
                continue;
            }

            if (! isset($keys[$name])) {
                $obj       = new stdClass();
                $obj->name = $name;

                if ($name === '_id_') {
                    $type = 'PRIMARY';
                } elseif (! empty($row['unique'])) {
                    $type = 'UNIQUE';
                } else {
                    $type = 'INDEX';
                }

                $obj->type   = $type;
                $obj->fields = [];
                $keys[$name] = $obj;
            }

            // Fields come from key document
            if (isset($row['key']) && is_array($row['key'])) {
                foreach (array_keys($row['key']) as $field) {
                    $keys[$name]->fields[] = $field;
                }
            }
        }

        return $keys;
    }

    /**
     * Platform-specific foreign keys data.
     *
     * @return array<string, stdClass>
     * @see    getForeignKeyData()
     *
     */
    protected function _foreignKeyData(string $table): array
    {
        // MongoDB does not support foreign keys; return empty structure
        return [];
    }

    /**
     * Determine if a particular collection exists (override for MongoDB).
     * Delegates to parent when using cache; otherwise uses a direct listCollections by name.
     */
    public function tableExists(string $tableName, bool $cached = true): bool
    {
        if ($cached) {
            return parent::tableExists($tableName, $cached);
        }

        $sql = $this->_listTables(false, $tableName);
        if ($sql === false) {
            if ($this->DBDebug) {
                throw new DatabaseException('This feature is not available for the database you are using.');
            }
            return false;
        }

        $query = $this->query($sql);
        return is_object($query) && $query->getResultArray() !== [];
    }

    /**
     * Fetch field names from the collection validator schema
     *
     * @param string|\CodeIgniter\Database\TableName $tableName
     * @return list<string>
     */
    public function getFieldNames($tableName): array
    {
        $table = ($tableName instanceof TableName) ? $tableName->getTableName() : (string) $tableName;

        // Кеш
        if (isset($this->dataCache['field_names'][$table])) {
            return $this->dataCache['field_names'][$table];
        }

        // 1) Пытаемся взять поля из валидатора
        $fields = $this->_listColumnsFromScheme($table);

        // 2) Если валидатор пуст — fallback: агрегируем один документ и извлекаем ключи
        if ($fields === []) {
            $sql = $this->_listColumns($table);
            if ($sql !== false) {
                $query = $this->query($sql);
                if (is_object($query) && method_exists($query, 'getResultArray')) {
                    $rows = $query->getResultArray();
                    $seen = [];
                    foreach ($rows as $row) {
                        if (isset($row['column_name'])) {
                            $name = (string) $row['column_name'];
                            if ($name !== '' && !isset($seen[$name])) {
                                $seen[$name] = true;
                                $fields[] = $name;
                            }
                        }
                    }
                }
            }
        }

        // 3) Если всё ещё пусто — вернём хотя бы _id
        if ($fields === []) {
            $fields = ['_id'];
        }

        // Кешируем и возвращаем
        $this->dataCache['field_names'][$table] = $fields;
        return $fields;
    }

    /**
     * Returns an object with field data
     *
     * @return list<stdClass>
     */
    public function getFieldData(string $table)
    {
        $protected = $this->protectIdentifiers($table, true, false, false);

        // Cache
        if (isset($this->dataCache['field_data'][$protected])) {
            return $this->dataCache['field_data'][$protected];
        }

        $out = [];

        // 1) Try to build from collection validator ($jsonSchema)
        $schema = $this->_getValidatorScheme($protected);
        if (isset($schema['properties']) && is_array($schema['properties'])) {
            $required = [];
            if (isset($schema['required']) && is_array($schema['required'])) {
                $required = $schema['required'];
            }

            foreach ($schema['properties'] as $name => $prop) {
                $name = (string) $name;
                if ($name === '') {
                    continue;
                }

                $o = new stdClass();
                $o->name        = $name;

                // Determine type from bsonType/type
                $type = null;
                $bsonType = $prop['bsonType'] ?? ($prop['type'] ?? null);
                if (is_array($bsonType)) {
                    // Join multiple types for visibility
                    $type = implode('|', array_map('strval', $bsonType));
                } elseif ($bsonType !== null) {
                    $type = (string) $bsonType;
                }
                $o->type = $type;

                // max_length from maxLength if present
                $o->max_length = isset($prop['maxLength']) && is_numeric($prop['maxLength'])
                    ? (int) $prop['maxLength']
                    : null;

                // default value if provided by schema
                $o->default = $prop['default'] ?? null;

                // nullable: not in required OR type includes null
                $includesNull = false;
                if (is_array($bsonType)) {
                    $includesNull = in_array('null', $bsonType, true);
                } elseif (is_string($bsonType)) {
                    $includesNull = ($bsonType === 'null');
                }
                $o->nullable = (! in_array($name, $required, true)) || $includesNull;

                $o->primary_key = (bool) ($name === '_id');

                $out[] = $o;
            }
        }

        // 2) Fallback to sampling-based metadata if schema gave nothing
        if ($out === []) {
            $out = $this->_fieldData($protected);
        }

        // 3) Ensure at least _id is present
        if ($out === []) {
            $o = new stdClass();
            $o->name        = '_id';
            $o->type        = null;
            $o->max_length  = null;
            $o->default     = null;
            $o->nullable    = true;
            $o->primary_key = true;
            $out[] = $o;
        }

        // Cache and return
        $this->dataCache['field_data'][$protected] = $out;
        return $out;
    }

    /**
     * Returns a new Builder instance for a collection (table equivalent)
     */
    public function table($tableName)
    {
        if (empty($tableName)) {
            throw new DatabaseException('You must set the database table to be used with your query.');
        }
        return new Builder($this->protectIdentifiers($tableName, true, false, false), $this);
    }
}
