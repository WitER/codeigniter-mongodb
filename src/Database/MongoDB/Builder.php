<?php
declare(strict_types=1);

namespace CodeIgniter\Database\MongoDB;

use CodeIgniter\Database\BaseBuilder;
use CodeIgniter\Database\Exceptions\DatabaseException;
use CodeIgniter\Database\Exceptions\DataException;
use MongoDB\Collection;

/**
 * MongoDB Query Builder (limited)
 *
 * Supports a subset of BaseBuilder methods and translates them to MongoDB
 * collection operations: select (projection), where/orWhere (simple equality and IN),
 * orderBy, limit/offset, get, insert, insertBatch, update, delete, countAllResults.
 */
class Builder extends BaseBuilder
{
    protected array $filter = [];
    protected array $projection = [];
    protected array $sort = [];
    protected ?int $limitVal = null;
    protected ?int $skipVal = null;

    // Extra features
    protected ?string $distinctField = null; // legacy idea; not used with CI4 signature
    protected bool $isDistinct = false;
    protected array $groupFields = [];
    protected array $havingFilter = [];
    protected array $aggregations = [];
    // Накопитель данных для update (эквивалент BaseBuilder::set())
    protected array $updateDoc = [];

    // Safety and options
    protected bool $allowNoFilterWrite = false; // allow updateMany/deleteMany with empty filter
    protected bool $upsertFlag = false; // upsert option for update()

    // Distinct support for fields list
    protected array $distinctFields = [];

    // Grouping stack to build nested boolean logic
    protected array $groupStack = [];

    // Last compiled mongo shell command (for testMode diagnostics)
    protected ?string $lastCompiled = null;


    protected function getCollection(): Collection
    {
        /** @var Connection $conn */
        $conn = $this->db;
        if ( ! $conn->mongoDB) {
            $conn->connect();
        }
        // Honor DBPrefix for physical collection name (migrations create prefixed collections)
        $collectionName = $conn->DBPrefix . $this->getTable();
        return $conn->mongoDB->selectCollection($collectionName);
    }

    /**
     * Normalize field key for MongoDB filters: strip leading "<table>." or "<DBPrefix><table>."
     * since MongoDB documents are not namespaced by collection name.
     */
    protected function normalizeFieldKey(string $field): string
    {
        $f = trim($field);
        if ($f === '') {
            return $f;
        }
        $table = (string) $this->getTable();
        if ($table !== '') {
            // 1) Отрезаем точное имя коллекции, которое уже включает префикс (например, "tests_users.")
            $prefFull = $table . '.';
            if (str_starts_with($f, $prefFull)) {
                return substr($f, strlen($prefFull));
            }

            // 2) Отрезаем вариант с protectIdentifiers (на случай иных настроек)
            if ($this->db instanceof Connection) {
                $withPrefix = $this->db->protectIdentifiers($table, true, false, false) . '.';
                if ($withPrefix !== '.' && str_starts_with($f, $withPrefix)) {
                    return substr($f, strlen($withPrefix));
                }
            }

            // 3) Отрезаем «базовое» имя без префикса (например, "users."), если текущая таблица содержит префикс
            if ($this->db instanceof Connection) {
                $dbPrefix = (string) $this->db->DBPrefix;
                $base = $table;
                if ($dbPrefix !== '' && str_starts_with($table, $dbPrefix)) {
                    $base = substr($table, strlen($dbPrefix));
                }
                if ($base !== '' && $base !== $table) {
                    $basePref = $base . '.';
                    if (str_starts_with($f, $basePref)) {
                        return substr($f, strlen($basePref));
                    }
                }
            }
        }
        return $f;

    }

    // Overridden chairs to record our own state (do not call parent)

    /**
     * Sets the fields to be selected in the query.
     *
     * @param string|array $select A string or array defining the fields to be selected. Defaults to '*', which means all fields.
     * @param bool|null $escape Whether to escape field names. If null, the default behavior is used.
     * @return self Returns the current instance for method chaining.
     */
    public function select($select = '*', ?bool $escape = null)
    {
        if ($select === '*' || $select === null) {
            $this->projection = [];
            return $this;
        }
        $fields = is_array($select) ? $select : (preg_split('/\s*,\s*/', (string)$select) ?: []);
        foreach ($fields as $f) {
            if ($f === '' || $f === '*') {
                continue;
            }
            $this->projection[$f] = 1;
        }
        return $this;
    }

    /**
     * Select MIN(field) as alias (учитывается при группировке или как общий агрегат без groupBy)
     */
    public function selectMin(string $select = '', string $alias = ''): self
    {
        $field = trim((string) $select);
        if ($field === '') {
            throw new DatabaseException('selectMin requires a non-empty field name');
        }
        $this->aggregations[] = ['type' => 'min', 'field' => $field, 'alias' => ($alias !== '' ? $alias : $field)];
        return $this;
    }

    /**
     * Select MAX(field) as alias (учитывается при группировке или как общий агрегат без groupBy)
     */
    public function selectMax(string $select = '', string $alias = ''): self
    {
        $field = trim((string) $select);
        if ($field === '') {
            throw new DatabaseException('selectMax requires a non-empty field name');
        }
        $this->aggregations[] = ['type' => 'max', 'field' => $field, 'alias' => ($alias !== '' ? $alias : $field)];
        return $this;
    }

    /**
     * Select AVG(field) as alias (учитывается при группировке или как общий агрегат без groupBy)
     */
    public function selectAvg(string $select = '', string $alias = ''): self
    {
        $field = trim((string) $select);
        if ($field === '') {
            throw new DatabaseException('selectAvg requires a non-empty field name');
        }
        $this->aggregations[] = ['type' => 'avg', 'field' => $field, 'alias' => ($alias !== '' ? $alias : $field)];
        return $this;
    }

    /**
     * Select SUM(field) as alias (учитывается при группировке или как общий агрегат без groupBy)
     */
    public function selectSum(string $select = '', string $alias = ''): self
    {
        $field = trim((string) $select);
        if ($field === '') {
            throw new DatabaseException('selectSum requires a non-empty field name');
        }
        $this->aggregations[] = ['type' => 'sum', 'field' => $field, 'alias' => ($alias !== '' ? $alias : $field)];
        return $this;
    }

    /**
     * Select COUNT(field) as alias (учитывается при группировке или как общий агрегат без groupBy)
     * Если $select пустой или '*', эквивалентно COUNT всех документов в группе/подмножестве.
     */
    public function selectCount(string $select = '*', string $alias = ''): self
    {
        $field = trim((string) $select);
        $this->aggregations[] = [
            'type'  => 'count',
            'field' => ($field === '' ? '*' : $field),
            'alias' => ($alias !== '' ? $alias : ($field === '*' ? 'count' : ($field . '_count'))),
        ];
        return $this;
    }


    public function where($key, $value = null, ?bool $escape = null)
    {
        if (is_array($key)) {
            foreach ($key as $k => $v) {
                $this->applyParsedWhere($k, $v, false, false);
            }
        } else {
            $this->applyParsedWhere((string)$key, $value, false, false);
        }
        return $this;
    }

    public function orWhere($key, $value = null, ?bool $escape = null)
    {
        if (is_array($key)) {
            foreach ($key as $k => $v) {
                $this->applyParsedWhere((string)$k, $v, true, false);
            }
        } else {
            $this->applyParsedWhere((string)$key, $value, true, false);
        }
        return $this;
    }

    public function whereNot($key, $value = null): self
    {
        if (is_array($key)) {
            foreach ($key as $k => $v) {
                $this->applyParsedWhere((string)$k, $v, false, true);
            }
        } else {
            $this->applyParsedWhere((string)$key, $value, false, true);
        }
        return $this;
    }

    public function orWhereNot($key, $value = null): self
    {
        if (is_array($key)) {
            foreach ($key as $k => $v) {
                $this->applyParsedWhere((string)$k, $v, true, true);
            }
        } else {
            $this->applyParsedWhere((string)$key, $value, true, true);
        }
        return $this;
    }

    // Grouping with stack: groupStart/orGroupStart/notGroupStart + groupEnd/orGroupEnd
    public function groupStart(): self { $this->groupStack[] = ['op' => '$and', 'terms' => []]; return $this; }
    public function orGroupStart(): self { $this->groupStack[] = ['op' => '$or', 'terms' => []]; return $this; }
    public function notGroupStart(): self { $this->groupStack[] = ['op' => '$nor', 'terms' => []]; return $this; }

    public function groupEnd(): self { return $this->closeGroup(false); }
    public function orGroupEnd(): self { return $this->closeGroup(true); }

    public function whereIn($key = null, $values = null, ?bool $escape = null)
    {
        if ($key !== null && is_array($values)) {
            $this->applyParsedWhere((string)$key, ['$in' => array_values($values)], false, false, true);
        }
        return $this;
    }

    public function whereNotIn($key = null, $values = null, ?bool $escape = null)
    {
        if ($key !== null && is_array($values)) {
            $this->applyParsedWhere((string)$key, ['$nin' => array_values($values)], false, false, true);
        }
        return $this;
    }

    public function orWhereIn($key = null, $values = null, ?bool $escape = null)
    {
        if ($key !== null && is_array($values)) {
            $this->applyParsedWhere((string)$key, ['$in' => array_values($values)], true, false, true);
        }
        return $this;
    }

    public function orWhereNotIn($key = null, $values = null, ?bool $escape = null)
    {
        if ($key !== null && is_array($values)) {
            $this->applyParsedWhere((string)$key, ['$nin' => array_values($values)], true, false, true);
        }
        return $this;
    }

    public function like(
        $field,
        string $match = '',
        string $side = 'both',
        ?bool $escape = null,
        bool $insensitiveSearch = false
    ) {
        $pattern = (string)$match;
        if ($escape) {
            $pattern = $this->escapeLikeString($pattern);
        }
        if ($side === 'before') {
            $pattern = $pattern . '$';
        } elseif ($side === 'after') {
            $pattern = '^' . $pattern;
        }
        $expr = ['$regex' => $pattern];
        if ($insensitiveSearch) {
            $expr['$options'] = 'i';
        }
        if (is_array($field)) {
            $or = [];
            foreach ($field as $f) {
                $key = $this->normalizeFieldKey((string)$f);
                $or[] = [$key => $expr];
            }
            if ($this->filter === []) {
                $this->filter = ['$or' => $or];
            } else {
                $this->filter = ['$and' => [$this->filter, ['$or' => $or]]];
            }
        } else {
            $this->applyParsedWhere((string)$field, $expr, false, false, true);
        }
        return $this;
    }

    public function notLike(
        $field,
        string $match = '',
        string $side = 'both',
        ?bool $escape = null,
        bool $insensitiveSearch = false
    ) {
        $pattern = (string)$match;
        if ($escape) {
            $pattern = $this->escapeLikeString($pattern);
        }
        if ($side === 'before') {
            $pattern = $pattern . '$';
        } elseif ($side === 'after') {
            $pattern = '^' . $pattern;
        }
        $expr = ['$regex' => $pattern];
        if ($insensitiveSearch) {
            $expr['$options'] = 'i';
        }
        if (is_array($field)) {
            // For notLike across multiple fields, combine with AND (all must not match)
            $ands = [];
            foreach ($field as $f) {
                $key = $this->normalizeFieldKey((string)$f);
                $ands[] = [$key => ['$not' => $expr]];
            }
            $clause = ['$and' => $ands];
            if ($this->filter === []) {
                $this->filter = $clause;
            } else {
                $this->filter = ['$and' => [$this->filter, $clause]];
            }
        } else {
            $this->applyParsedWhere((string)$field, ['$not' => $expr], false, false, true);
        }
        return $this;
    }

    /**
     * OR LIKE — объединяет существующий фильтр с $or, добавляя regex-условие.
     */
    public function orLike(
        $field,
        string $match = '',
        string $side = 'both',
        ?bool $escape = null,
        bool $insensitiveSearch = false
    ): self {
        $pattern = (string)$match;
        if ($escape) {
            $pattern = $this->escapeLikeString($pattern);
        }
        if ($side === 'before') {
            $pattern = $pattern . '$';
        } elseif ($side === 'after') {
            $pattern = '^' . $pattern;
        }
        $expr = ['$regex' => $pattern];
        if ($insensitiveSearch) {
            $expr['$options'] = 'i';
        }
        if (is_array($field)) {
            $or = [];
            foreach ($field as $f) {
                $key = $this->normalizeFieldKey((string)$f);
                $or[] = [$key => $expr];
            }
            $clause = ['$or' => $or];
            $this->filter = $this->filter === [] ? $clause : ['$or' => [$this->filter, $clause]];
        } else {
            $key = $this->normalizeFieldKey((string)$field);
            $clause = [$key => $expr];
            $this->filter = $this->filter === [] ? $clause : ['$or' => [$this->filter, $clause]];
        }
        return $this;
    }

    /**
     * OR NOT LIKE — отрицание regex в составе $or
     */
    public function orNotLike(
        $field,
        string $match = '',
        string $side = 'both',
        ?bool $escape = null,
        bool $insensitiveSearch = false
    ): self {
        $pattern = (string)$match;
        if ($escape) {
            $pattern = $this->escapeLikeString($pattern);
        }
        if ($side === 'before') {
            $pattern = $pattern . '$';
        } elseif ($side === 'after') {
            $pattern = '^' . $pattern;
        }
        $expr = ['$regex' => $pattern];
        if ($insensitiveSearch) {
            $expr['$options'] = 'i';
        }
        if (is_array($field)) {
            $or = [];
            foreach ($field as $f) {
                $or[] = [(string)$f => ['$not' => $expr]];
            }
            $clause = ['$or' => $or];
            $this->filter = $this->filter === [] ? $clause : ['$or' => [$this->filter, $clause]];
        } else {
            $clause = [(string)$field => ['$not' => $expr]];
            $this->filter = $this->filter === [] ? $clause : ['$or' => [$this->filter, $clause]];
        }
        return $this;
    }


    public function between($field, $min, $max)
    {
        $this->applyParsedWhere((string)$field, ['$gte' => $min, '$lte' => $max], false, false, true);
        return $this;
    }

    public function notBetween($field, $min, $max)
    {
        $clause = ['$or' => [
            [(string)$field => ['$lt' => $min]],
            [(string)$field => ['$gt' => $max]],
        ]];
        if ($this->filter === []) {
            $this->filter = $clause;
        } else {
            $this->filter = ['$and' => [$this->filter, $clause]];
        }
        return $this;
    }

    public function distinct($val = true)
    {
        // Accept bool, string field, or array of fields (CI-style flexibility)
        if (is_bool($val)) {
            $this->isDistinct = $val;
            if (!$val) { $this->distinctFields = []; }
        } elseif (is_string($val)) {
            $this->isDistinct = true;
            $this->distinctFields = [$val];
        } elseif (is_array($val)) {
            $this->isDistinct = true;
            $this->distinctFields = array_values(array_map('strval', $val));
        } else {
            $this->isDistinct = (bool)$val;
        }
        return $this;
    }

    public function groupBy($by, ?bool $escape = null)
    {
        $fields = is_array($by) ? $by : (preg_split('/\s*,\s*/', (string)$by) ?: []);
        // Фильтруем пустые имена, чтобы не получить '$_id.' в $project
        $this->groupFields = array_values(array_filter(array_map(static fn($s) => trim((string)$s), $fields), static fn($s) => $s !== ''));
        return $this;
    }

    public function having($key, $value = null, ?bool $escape = null)
    {
        if (is_array($key)) {
            foreach ($key as $k => $v) {
                $this->havingFilter = $this->havingFilter + $this->buildClauseFromKeyVal((string)$k, $v);
            }
        } else {
            $this->havingFilter = $this->havingFilter + $this->buildClauseFromKeyVal((string)$key, $value);
        }
        return $this;
    }

    public function orderBy($orderBy, $direction = '', ?bool $escape = null)
    {
        $direction = strtoupper((string)$direction);
        $dir = ($direction === 'DESC') ? -1 : 1;
        $fields = [];
        if (is_array($orderBy)) {
            $fields = $orderBy;
        } else {
            $tmp = preg_split('/\s*,\s*/', (string)$orderBy);
            $fields = $tmp ?: [];
        }
        foreach ($fields as $f => $maybeDir) {
            if (is_string($f)) {
                $this->sort[$f] = (strtoupper((string)$maybeDir) === 'DESC') ? -1 : 1;
            } else {
                $this->sort[$maybeDir] = $dir;
            }
        }
        return $this;
    }

    public function limit(?int $value = null, ?int $offset = null)
    {
        $this->limitVal = $value;
        if ($offset !== null) {
            $this->skipVal = $offset;
        }
        return $this;
    }

    /**
     * set() — накапливает данные для последующего update().
     * Принимает массив или пару ключ/значение. $escape игнорируется.
     */
    public function set($key, $value = '', ?bool $escape = null)
    {
        if (is_array($key)) {
            foreach ($key as $k => $v) {
                $this->updateDoc['$set'][$k] = $v;
            }
        } else {
            $this->updateDoc['$set'][(string)$key] = $value;
        }
        return $this;
    }

    // Extra update operators
    public function setInc($key, $value = 1): self
    {
        if (is_array($key)) {
            foreach ($key as $k => $v) {
                $this->updateDoc['$inc'][(string)$k] = (int)$v;
            }
        } else {
            $this->updateDoc['$inc'][(string)$key] = (int)$value;
        }
        return $this;
    }

    public function setDec($key, $value = 1): self
    {
        $val = is_array($key) ? $key : [(string)$key => $value];
        foreach ($val as $k => $v) {
            $this->updateDoc['$inc'][(string)$k] = -abs((int)$v);
        }
        return $this;
    }

    public function unsetField($key): self
    {
        if (is_array($key)) {
            foreach ($key as $k) {
                $this->updateDoc['$unset'][(string)$k] = 1;
            }
        } else {
            $this->updateDoc['$unset'][(string)$key] = 1;
        }
        return $this;
    }

    public function setOnInsert($key, $value = null): self
    {
        if (is_array($key)) {
            foreach ($key as $k => $v) {
                $this->updateDoc['$setOnInsert'][(string)$k] = $v;
            }
        } else {
            $this->updateDoc['$setOnInsert'][(string)$key] = $value;
        }
        return $this;
    }

    public function push($key, $value): self
    {
        if (is_array($key) && $value === null) {
            foreach ($key as $k => $v) {
                $this->updateDoc['$push'][(string)$k] = $v;
            }
        } else {
            $this->updateDoc['$push'][(string)$key] = $value;
        }
        return $this;
    }

    public function pull($key, $value): self
    {
        if (is_array($key) && $value === null) {
            foreach ($key as $k => $v) {
                $this->updateDoc['$pull'][(string)$k] = $v;
            }
        } else {
            $this->updateDoc['$pull'][(string)$key] = $value;
        }
        return $this;
    }

    public function addToSet($key, $value): self
    {
        if (is_array($key) && $value === null) {
            foreach ($key as $k => $v) {
                $this->updateDoc['$addToSet'][(string)$k] = $v;
            }
        } else {
            $this->updateDoc['$addToSet'][(string)$key] = $value;
        }
        return $this;
    }

    public function setUpsertFlag(bool $flag = true): self
    {
        $this->upsertFlag = $flag;
        return $this;
    }

    public function allowNoFilterWrite(bool $allow = true): self
    {
        $this->allowNoFilterWrite = $allow;
        return $this;
    }


    public function offset(int $offset)
    {
        $this->skipVal = $offset;
        return $this;
    }

    // CRUD operations
    public function get(?int $limit = null, int $offset = 0, bool $reset = true)
    {
        if ($limit !== null) {
            $this->limitVal = $limit;
        }
        if ($offset) {
            $this->skipVal = $offset;
        }

        // Prepare command for Connection::query in accordance with BaseBuilder contract
        $table = $this->getTable();
        $isAggregate = ($this->groupFields !== [] || $this->isDistinct || $this->aggregations !== []);
        if ($isAggregate) {
            $pipeline = $this->buildPipelineForAggregate();
            $cmd = [
                'aggregate' => $table,
                'pipeline'  => $pipeline,
                'cursor'    => new \stdClass(),
            ];
        } else {
            $cmd = [
                'find'   => $table,
                'filter' => $this->filter ?: [],
            ];
            if ($this->projection !== []) {
                $cmd['projection'] = $this->projection;
            }
            if ($this->sort !== []) {
                $cmd['sort'] = $this->sort;
            }
            if ($this->limitVal !== null) {
                $cmd['limit'] = $this->limitVal;
            }
            if ($this->skipVal !== null) {
                $cmd['skip'] = $this->skipVal;
            }
        }
        $json = json_encode($cmd, JSON_UNESCAPED_SLASHES);

        if ($this->testMode) {
            if ($reset) {
                $this->resetQuery();
                $this->binds = [];
            }
            return $json;
        }

        $result = $this->db->query($json, $this->binds, false);
        if ($reset) {
            $this->resetQuery();
            $this->binds = [];
        }
        return $result;
    }

    /**
     * getWhere — стандартный хелпер CI4
     */
    public function getWhere($where = null, ?int $limit = null, ?int $offset = null, bool $reset = true)
    {
        if ($where !== null) {
            $this->where($where);
        }
        return $this->get($limit, $offset, $reset);
    }


    public function countAllResults(bool $reset = true, bool $test = false): int|string
    {
        // In test mode return compiled JSON command
        if ($test || $this->testMode) {
            // Prefer native count command for simple case, otherwise aggregate + $count
            if ($this->groupFields !== [] || $this->isDistinct || $this->aggregations !== []) {
                $pipeline = $this->buildPipelineForAggregate();
                $pipeline[] = ['$count' => 'c'];
                $cmd = [
                    'aggregate' => $this->getTable(),
                    'pipeline'  => $pipeline,
                    'cursor'    => new \stdClass(),
                ];
            } else {
                $cmd = [
                    'count' => $this->getTable(),
                    'query' => $this->filter ?: [],
                ];
            }
            $json = json_encode($cmd, JSON_UNESCAPED_SLASHES);
            if ($reset) {
                $this->resetQuery();
            }
            return $json;
        }

        // Build command JSON and execute via Connection::query()
        if ($this->groupFields !== [] || $this->isDistinct || $this->aggregations !== []) {
            $pipeline = $this->buildPipelineForAggregate();
            $pipeline[] = ['$count' => 'c'];
            $cmd = [
                'aggregate' => $this->getTable(),
                'pipeline'  => $pipeline,
                'cursor'    => new \stdClass(),
            ];
        } else {
            $cmd = [
                'count' => $this->getTable(),
                'query' => $this->filter ?: [],
            ];
        }
        $json   = json_encode($cmd, JSON_UNESCAPED_SLASHES);
        $result = $this->db->query($json, $this->binds, false);

        $count = 0;
        if (is_object($result)) {
            $rows = $result->getResultArray();
            // count command returns {n: <int>} while aggregate+$count returns [{c: <int>}]
            if ($rows !== []) {
                $first = $rows[0];
                if (isset($first['c'])) {
                    $count = (int) $first['c'];
                } elseif (isset($first['n'])) {
                    $count = (int) $first['n'];
                }
            }
        }

        if ($reset) {
            $this->resetQuery();
            $this->binds = [];
        }

        return $count;
    }

    /**
     * Подсчёт всех документов коллекции (без учёта текущих условий)
     * @param bool $reset
     */
    public function countAll(bool $reset = true): int
    {
        // Use count command without filter
        $cmd = [
            'count' => $this->getTable(),
            'query' => [],
        ];
        $json   = json_encode($cmd, JSON_UNESCAPED_SLASHES);
        $result = $this->db->query($json, $this->binds, false);

        $count = 0;
        if (is_object($result)) {
            $rows = $result->getResultArray();
            if ($rows !== []) {
                $first = $rows[0];
                if (isset($first['n'])) {
                    $count = (int) $first['n'];
                }
            }
        }

        if ($reset) {
            $this->resetQuery();
        }
        return $count;
    }


    public function insert($set = null, ?bool $escape = null)
    {
        if ($set !== null) {
            $this->set($set, '', $escape);
        }

        if ($this->validateInsert() === false) {
            return false;
        }

        $doc = [];
        if (!empty($this->updateDoc['$set'])) {
            $doc = $this->updateDoc['$set'];
        } else {
            throw new DataException('Nothing to insert: empty document.');
        }

        // Если _id не задан — сгенерируем на клиенте, чтобы можно было вернуть его после вставки
        /*if (!array_key_exists('_id', $doc)) {
            $doc['_id'] = new \MongoDB\BSON\ObjectId();
        }*/


        // Build insert command
        $cmd = [
            'insert'    => $this->getTable(),
            'documents' => [$doc],
        ];
        $json = json_encode($cmd, JSON_UNESCAPED_SLASHES);

        if ($this->testMode) {
            // Do not reset before returning so user can inspect; mimic BaseBuilder compiled behavior
            $this->resetQuery();
            return $json;
        }

        $result = $this->db->query($json, $this->binds, false);
        // Reset builder after execution per BaseBuilder
        $this->resetQuery();

        return $result !== false;
    }

    public function insertBatch($set = null, ?bool $escape = null, int $batchSize = 100)
    {
        $docs = $set ?? [];
        if ($docs === []) {
            return true;
        }

        // Merge defaults from set()
        if (!empty($this->updateDoc['$set'])) {
            $defaults = $this->updateDoc['$set'];
            foreach ($docs as $i => $doc) {
                if (!is_array($doc)) {
                    throw new DataException('insertBatch expects an array of documents (arrays).');
                }
                $docs[$i] = $doc + $defaults;
            }
        }

        // Сгенерируем _id там, где отсутствует
        foreach ($docs as $i => $d) {
            if (!is_array($d)) {
                throw new DataException('insertBatch expects an array of documents (arrays).');
            }
            /*if (!array_key_exists('_id', $d)) {
                $d['_id'] = new \MongoDB\BSON\ObjectId();
            }*/
            $docs[$i] = $d;
        }


        $cmd = [
            'insert'    => $this->getTable(),
            'documents' => array_values($docs),
        ];
        $json = json_encode($cmd, JSON_UNESCAPED_SLASHES);

        if ($this->testMode) {
            $this->resetQuery();
            return $json;
        }

        $result = $this->db->query($json, $this->binds, false);
        $affected = 0;
        if ($result !== false) {
            $affected = (int) $this->db->affectedRows();
        }
        $this->resetQuery();
        return $affected;
    }

    public function update($set = null, $where = null, ?int $limit = null): bool
    {
        // Merge provided $set into accumulated doc
        if ($set !== null) {
            if (!is_array($set) || $set === []) {
                throw new DatabaseException('update() requires a non-empty array of data');
            }
            foreach ($set as $k => $v) {
                $this->updateDoc['$set'][$k] = $v;
            }
        }
        if (is_array($where) && $where !== []) {
            foreach ($where as $k => $v) {
                $this->applyParsedWhere((string)$k, $v, false, false);
            }
        }
        $update = $this->updateDoc ?: [];
        if ($update === [] && empty($this->updateDoc['$set'])) {
            throw new DatabaseException('update() requires data to be set via set()/operators');
        }

        // Safety: disallow mass update without filter unless explicitly allowed
        $isMany = ($limit !== 1);
        if ($isMany && $this->filter === [] && !$this->allowNoFilterWrite) {
            throw new DatabaseException('Refusing to updateMany without filter. Enable allowNoFilterWrite(true) to permit.');
        }

        // Build command
        $updates = [[
            'q'      => $this->filter ?: [],
            'u'      => $update,
            'multi'  => $limit === 1 ? false : true,
        ]];
        if ($this->upsertFlag) {
            $updates[0]['upsert'] = true;
        }
        $cmd = [
            'update'  => $this->getTable(),
            'updates' => $updates,
        ];
        $json = json_encode($cmd, JSON_UNESCAPED_SLASHES);

        if ($this->testMode) {
            $this->resetQuery();
            return $json !== '';
        }

        $result = $this->db->query($json, $this->binds, false);
        $this->resetQuery();
        return $result !== false;
    }

    /**
     * Batch update by index field: each document must include $index key.
     * CI-compatible signature: updateBatch($set, $index = null, $batchSize = 100)
     */
    public function updateBatch($set = null, $constraints = null, int $batchSize = 100)
    {
        $rows = $set ?? [];
        if (!is_array($rows) || $rows === []) {
            return 0;
        }
        if ($constraints === null || $constraints === '') {
            throw new DatabaseException('updateBatch requires an index field name');
        }

        // Build a single update command with multiple updates
        $updates = [];
        foreach ($rows as $row) {
            if (!is_array($row) || !array_key_exists($constraints, $row)) {
                throw new DatabaseException('Each row in updateBatch must be an array and contain the index field');
            }
            $val  = $row[$constraints];
            $data = $row;
            unset($data[$constraints]);
            $updates[] = [
                'q'     => [$constraints => $val],
                'u'     => ['$set' => $data],
                'multi' => false,
            ];
        }

        $cmd = [
            'update'  => $this->getTable(),
            'updates' => $updates,
        ];
        $json = json_encode($cmd, JSON_UNESCAPED_SLASHES);

        if ($this->testMode) {
            $this->resetQuery();
            return $json;
        }

        $result = $this->db->query($json, $this->binds, false);
        $affected = 0;
        if ($result !== false) {
            $affected = (int) $this->db->affectedRows();
        }
        $this->resetQuery();
        return $affected;
    }

    /**
     * Mongo-style REPLACE: upsert by current filter or _id from document.
     */
    public function replace(?array $set = null)
    {
        if ($set !== null) {
            $this->set($set);
        }
        if (empty($this->updateDoc['$set'])) {
            if ($this->db->DBDebug) {
                throw new DatabaseException('You must use the "set" method to replace an entry.');
            }
            return false;
        }
        $doc = $this->updateDoc['$set'];

        $filter = $this->filter;
        if ($filter === [] && isset($doc['_id'])) {
            $filter = ['_id' => $doc['_id']];
        }
        if ($filter === [] && !$this->allowNoFilterWrite) {
            throw new DatabaseException('Refusing to replaceOne without filter. Enable allowNoFilterWrite(true) or provide _id.');
        }

        // Build command: replace is an update with full document
        $cmd = [
            'update'  => $this->getTable(),
            'updates' => [[
                'q'     => $filter ?: [],
                'u'     => $doc,
                'multi' => false,
            ]],
        ];
        $json = json_encode($cmd, JSON_UNESCAPED_SLASHES);

        if ($this->testMode) {
            $this->resetQuery();
            return $json;
        }

        $result = $this->db->query($json, $this->binds, false);
        $this->resetQuery();
        return $result !== false;
    }

    public function delete($where = '', ?int $limit = null, bool $resetData = true)
    {
        if (is_array($where) && $where !== []) {
            $this->filter = array_merge($this->filter, $where);
        }

        // Safety: block deleteMany without filter unless allowed
        if ($limit !== 1 && $this->filter === [] && !$this->allowNoFilterWrite) {
            // Keep parity with previous behavior: was commented out, so allow silently
        }

        $deletes = [[
            'q'     => $this->filter ?: [],
            'limit' => ($limit === 1) ? 1 : 0,
        ]];
        $cmd = [
            'delete'  => $this->getTable(),
            'deletes' => $deletes,
        ];
        $json = json_encode($cmd, JSON_UNESCAPED_SLASHES);

        if ($this->testMode) {
            if ($resetData) {
                $this->resetQuery();
            }
            return $json;
        }

        $result = $this->db->query($json, $this->binds, false);
        if ($resetData) {
            $this->resetQuery();
        }
        return $result !== false;
    }

    public function getCompiledSelect(bool $reset = true): string
    {
        $table = $this->getTable();
        $isAggregate = ($this->groupFields !== [] || $this->isDistinct || $this->aggregations !== []);
        if ($isAggregate) {
            $pipeline = $this->buildPipelineForAggregate();
            $cmd = [
                'aggregate' => $table,
                'pipeline'  => $pipeline,
                'cursor'    => new \stdClass(),
            ];
        } else {
            $cmd = [
                'find'   => $table,
                'filter' => $this->filter ?: [],
            ];
            if ($this->projection !== []) {
                $cmd['projection'] = $this->projection;
            }
            if ($this->sort !== []) {
                $cmd['sort'] = $this->sort;
            }
            if ($this->limitVal !== null) {
                $cmd['limit'] = $this->limitVal;
            }
            if ($this->skipVal !== null) {
                $cmd['skip'] = $this->skipVal;
            }
        }
        $json = json_encode($cmd, JSON_UNESCAPED_SLASHES);
        if ($reset) {
            $this->resetQuery();
        }
        return $json;
    }

    public function getCompiledInsert($set = null, bool $reset = true): string
    {
        $doc = [];
        if ($set !== null) {
            if (!is_array($set)) {
                throw new DataException('getCompiledInsert expects array.');
            }
            $doc = $set;
        } elseif (!empty($this->updateDoc['$set'])) {
            $doc = $this->updateDoc['$set'];
        }
        return $this->getCompiledMongo('insertOne', ['doc' => $doc], $reset);
    }

    public function getCompiledUpdate($set = null, bool $reset = true): string
    {
        $data = [];
        if ($set !== null) {
            if (!is_array($set)) {
                throw new DataException('getCompiledUpdate expects array.');
            }
            $data = $set;
        } elseif (!empty($this->updateDoc['$set'])) {
            $data = $this->updateDoc['$set'];
        }
        return $this->getCompiledMongo('updateMany', ['update' => ['$set' => $data]], $reset);
    }

    public function getCompiledDelete(bool $reset = true): string
    {
        return $this->getCompiledMongo('deleteMany', [], $reset);
    }

    public function getCompiledQBWhere(bool $reset = true)
    {
        return $this->filter;
    }

    public function resetQuery()
    {
        // Reset our custom state
        $this->filter = [];
        $this->projection = [];
        $this->sort = [];
        $this->limitVal = null;
        $this->skipVal = null;
        $this->distinctField = null;
        $this->isDistinct = false;
        $this->distinctFields = [];
        $this->groupFields = [];
        $this->havingFilter = [];
        $this->aggregations = [];
        $this->updateDoc = [];
        $this->upsertFlag = false;
        $this->allowNoFilterWrite = false;
        $this->groupStack = [];
        // Also reset BaseBuilder state
        parent::resetQuery();
        return $this;
    }

    /**
     * This method is used by both insert() and getCompiledInsert() to
     * validate that the there data is actually being set and that table
     * has been chosen to be inserted into.
     *
     * @throws DatabaseException
     */
    protected function validateInsert(): bool
    {
        if (empty($this->updateDoc['$set'])) {
            if ($this->db->DBDebug) {
                throw new DatabaseException('You must use the "set" method to insert an entry.');
            }

            return false; // @codeCoverageIgnore
        }

        return true;
    }

    /**
     * Генерирует mongo shell-подобную строку команды на основе текущего состояния билдера
     * или переданной операции/данных.
     *
     * Примеры:
     * - getCompiledMongo('find')
     * - getCompiledMongo('aggregate')
     * - getCompiledMongo('insertOne', ['doc' => [...]])
     * - getCompiledMongo('insertMany', ['docs' => [[...], [...]]])
     * - getCompiledMongo('updateOne', ['update' => ['$set' => [...]]])
     * - getCompiledMongo('deleteMany')
     * - getCompiledMongo('count')
     */
    public function getCompiledMongo(string $operation = 'find', array $payload = [], bool $reset = true): string
    {
        $coll = $this->getTable();
        $db   = $this->db->database ?? 'db';

        // Сбор find-опций
        $filter = $this->filter ?: [];
        $options = [];
        if ($this->projection !== []) $options['projection'] = $this->projection;
        if ($this->sort !== [])       $options['sort']       = $this->sort;
        if ($this->limitVal !== null) $options['limit']      = $this->limitVal;
        if ($this->skipVal !== null)  $options['skip']       = $this->skipVal;

        $cmd = '';

        switch ($operation) {
            case 'find':
                $filterStr  = $this->encodeMongoValue($filter);
                $optionsStr = $options !== [] ? ', ' . $this->encodeMongoValue($options) : '';
                $cmd = sprintf('%s.%s.find(%s%s)', $db, $coll, $filterStr, $optionsStr);
                break;

            case 'aggregate':
                $pipeline = $this->buildPipelineForAggregate(); // использует текущее состояние билдера
                $cmd = sprintf('%s.%s.aggregate(%s)', $db, $coll, $this->encodeMongoValue($pipeline));
                break;

            case 'insertOne':
                $doc = $payload['doc'] ?? ($this->updateDoc['$set'] ?? []);
                $cmd = sprintf('%s.%s.insertOne(%s)', $db, $coll, $this->encodeMongoValue($doc));
                break;

            case 'insertMany':
                $docs = $payload['docs'] ?? [];
                $cmd  = sprintf('%s.%s.insertMany(%s)', $db, $coll, $this->encodeMongoValue(array_values($docs)));
                break;

            case 'updateOne':
            case 'updateMany':
                $upd = $payload['update'] ?? ($this->updateDoc ?: []);
                if ($upd === [] && !empty($this->updateDoc['$set'])) {
                    $upd = $this->updateDoc;
                }
                if ($upd === []) {
                    $upd = ['$set' => (object)[]];
                }
                $cmd = sprintf('%s.%s.%s(%s, %s)',
                    $db,
                    $coll,
                    $operation,
                    $this->encodeMongoValue($filter),
                    $this->encodeMongoValue($upd)
                );
                break;

            case 'replaceOne':
                $doc = $payload['doc'] ?? ($this->updateDoc['$set'] ?? []);
                $cmd = sprintf('%s.%s.replaceOne(%s, %s)',
                    $db,
                    $coll,
                    $this->encodeMongoValue($filter),
                    $this->encodeMongoValue($doc)
                );
                break;

            case 'deleteOne':
            case 'deleteMany':
                $cmd = sprintf('%s.%s.%s(%s)', $db, $coll, $operation, $this->encodeMongoValue($filter));
                break;

            case 'count':
            case 'countDocuments':
                // Если есть группировки/агрегаты/distinct — считаем через pipeline + $count
                if ($this->groupFields !== [] || $this->isDistinct || $this->aggregations !== []) {
                    $pipeline = $this->buildPipelineForAggregate();
                    $pipeline[] = ['$count' => 'c'];
                    $cmd = sprintf('%s.%s.aggregate(%s)', $db, $coll, $this->encodeMongoValue($pipeline));
                } else {
                    $cmd = sprintf('%s.%s.countDocuments(%s)', $db, $coll, $this->encodeMongoValue($filter));
                }
                break;

            default:
                $cmd = '// unsupported operation: ' . $operation;
        }

        // Сохраним последнюю скомпилированную команду для диагностики/testMode
        $this->lastCompiled = $cmd;

        if ($reset) {
            $this->resetQuery();
        }

        return $cmd;
    }

    /**
     * Собирает aggregation pipeline из текущего состояния (аналогично get()).
     */
    protected function buildPipelineForAggregate(): array
    {
        $pipeline = [];

        if ($this->filter !== []) {
            $pipeline[] = ['$match' => $this->filter];
        }

        if ($this->groupFields === [] && $this->isDistinct && $this->projection !== []) {
            $this->groupFields = array_keys($this->projection);
        }
        if ($this->groupFields === [] && $this->isDistinct && $this->distinctFields !== []) {
            $this->groupFields = $this->distinctFields;
        }
        // Безопасно отфильтруем пустые имена полей группировки
        $this->groupFields = array_values(array_filter($this->groupFields, static fn($s) => (string)$s !== ''));

        $groupId = null;
        if ($this->groupFields !== []) {
            $groupId = [];
            foreach ($this->groupFields as $gf) {
                $groupId[$gf] = '$' . $gf;
            }
        }
        $groupStage = ['_id' => $groupId === null ? null : (object)$groupId];

        if ($this->aggregations !== []) {
            foreach ($this->aggregations as $agg) {
                $alias = (string)$agg['alias'];
                $field = (string)$agg['field'];
                switch ($agg['type']) {
                    case 'min': $groupStage[$alias] = ['$min' => '$' . $field]; break;
                    case 'max': $groupStage[$alias] = ['$max' => '$' . $field]; break;
                    case 'avg': $groupStage[$alias] = ['$avg' => '$' . $field]; break;
                    case 'sum': $groupStage[$alias] = ['$sum' => '$' . $field]; break;
                    case 'count':
                        if ($field === '*') {
                            $groupStage[$alias] = ['$sum' => 1];
                        } else {
                            $groupStage[$alias] = ['$sum' => [
                                '$cond' => [[ '$ne' => [[ '$ifNull' => ['$' . $field, null] ], null ] ], 1, 0]
                            ]];
                        }
                        break;
                }
            }
        }

        // Пронесение полей из projection, не входящих в группировку
        $carryFields = [];
        if ($this->projection !== []) {
            foreach (array_keys($this->projection) as $pf) {
                $pf = trim((string)$pf);
                if ($pf !== '' && !in_array($pf, $this->groupFields, true)) {
                    $carryFields[] = $pf;
                }
            }
        }
        foreach ($carryFields as $cf) {
            $groupStage[$cf] = ['$first' => '$' . $cf];
        }

        if ($groupId !== null || $this->aggregations !== [] || $carryFields !== []) {
            $pipeline[] = ['$group' => $groupStage];

            $project = ['_id' => 0];
            if ($this->groupFields !== []) {
                foreach ($this->groupFields as $gf) {
                    $project[$gf] = '$_id.' . $gf;
                }
            }
            if ($this->aggregations !== []) {
                foreach ($this->aggregations as $agg) {
                    $project[(string)$agg['alias']] = 1;
                }
            }
            foreach ($carryFields as $cf) {
                $project[$cf] = 1;
            }
            $pipeline[] = ['$project' => $project];
        }

        if ($this->havingFilter !== []) {
            $pipeline[] = ['$match' => $this->havingFilter];
        }
        if ($this->sort !== []) {
            $pipeline[] = ['$sort' => $this->sort];
        }
        if ($this->skipVal !== null) {
            $pipeline[] = ['$skip' => $this->skipVal];
        }
        if ($this->limitVal !== null) {
            $pipeline[] = ['$limit' => $this->limitVal];
        }

        return $pipeline;
    }

    /**
     * Преобразует PHP-значение в текст для mongo shell.
     * Поддерживает ObjectId("..."), ISODate("..."), /regex/i, вложенные массивы/объекты.
     */
    // ===== Helper methods for where/having parsing and merging =====
    protected function applyParsedWhere(string $key, $value, bool $or = false, bool $negate = false, bool $rawValueIsExpr = false): void
    {
        $clause = $this->buildClauseFromKeyVal($key, $value, $negate, $rawValueIsExpr);

        // If inside a group, add to the current group context
        if (!empty($this->groupStack)) {
            $this->addClauseToCurrentGroup($clause, $or);
            return;
        }

        // Top-level behavior
        if ($or) {
            if ($this->filter === []) {
                $this->filter = $clause;
            } else {
                $this->filter = ['$or' => [$this->filter, $clause]];
            }
            return;
        }
        // AND merge field-wise at top-level
        foreach ($clause as $field => $expr) {
            $this->mergeFieldCondition((string)$field, $expr);
        }
    }

    protected function addClauseToCurrentGroup(array $clause, bool $asOr): void
    {
        $idx = count($this->groupStack) - 1;
        if ($idx < 0) {
            // Should not happen; fallback to top-level merge
            foreach ($clause as $field => $expr) {
                $this->mergeFieldCondition((string)$field, $expr);
            }
            return;
        }
        $op = $this->groupStack[$idx]['op'];
        if ($asOr && $op === '$and') {
            // Mixed OR inside AND-group: convert accumulated AND terms into a single term
            $compiledSoFar = $this->compileGroup($this->groupStack[$idx]);
            $this->groupStack[$idx] = ['op' => '$or', 'terms' => [$compiledSoFar]];
        }
        $this->groupStack[$idx]['terms'][] = $clause;
    }

    protected function compileGroup(array $group): array
    {
        $op = $group['op'] ?? '$and';
        $terms = $group['terms'] ?? [];

        if ($op === '$and') {
            if ($terms === []) {
                return [];
            }
            // Merge field-level expressions into a single document where possible
            $accDoc = [];
            $otherBoolTerms = [];
            foreach ($terms as $t) {
                if (!is_array($t)) {
                    // Treat scalars as equality on a virtual field (unlikely)
                    $otherBoolTerms[] = $t;
                    continue;
                }
                $isAssoc = $this->isAssoc($t);
                $firstKey = is_string(array_key_first($t)) ? (string)array_key_first($t) : '';
                                $isOperatorDoc = $isAssoc && !empty($t) && $firstKey !== '' && $firstKey[0] === '$';
                if ($isAssoc && !$isOperatorDoc) {
                    // Merge fields into $accDoc
                    foreach ($t as $field => $expr) {
                        if (!array_key_exists($field, $accDoc)) {
                            $accDoc[$field] = $expr;
                            continue;
                        }
                        $cur = $accDoc[$field];
                        $curObj = is_array($cur) && $this->isAssoc($cur) ? $cur : ['$eq' => $cur];
                        $newObj = is_array($expr) && $this->isAssoc($expr) ? $expr : ['$eq' => $expr];
                        // do not overwrite existing operators
                        $merged = $curObj + $newObj;
                        // If both had $eq and different values, last one wins
                        if (isset($curObj['$eq']) && isset($newObj['$eq']) && $curObj['$eq'] !== $newObj['$eq']) {
                            $merged['$eq'] = $newObj['$eq'];
                        }
                        $accDoc[$field] = $merged;
                    }
                } else {
                    // keep as separate boolean term ($or/$nor/etc.)
                    $otherBoolTerms[] = $t;
                }
            }
            if ($otherBoolTerms === []) {
                return $accDoc;
            }
            $andTerms = [];
            if ($accDoc !== []) {
                $andTerms[] = $accDoc;
            }
            foreach ($otherBoolTerms as $ob) {
                $andTerms[] = $ob;
            }
            if (count($andTerms) === 1) {
                return $andTerms[0];
            }
            return ['$and' => $andTerms];
        }
        if ($op === '$or') {
            return ['$or' => $terms];
        }
        // $nor
        return ['$nor' => $terms];
    }

    protected function closeGroup(bool $orWithPrev): self
    {
        if (empty($this->groupStack)) {
            return $this;
        }

        $group = array_pop($this->groupStack);
        $expr  = $this->compileGroup($group);

        // Если группа пуста — ничего не делаем
        if ($expr === [] || $expr === null) {
            return $this;
        }

        // Нормализуем в явный булев контейнер: {$and|$or|$nor: [...]}
        $op = $group['op'] ?? '$and';
        $wrapped = null;
        if ($op === '$and') {
            // compileGroup для AND может вернуть простой документ без оператора — оборачиваем
            if (is_array($expr)) {
                $firstKey = (string) (is_string(array_key_first($expr)) ? array_key_first($expr) : '');
                if ($firstKey === '$and') {
                    $wrapped = $expr; // уже {$and: [...]} 
                } else {
                    $wrapped = ['$and' => [$expr]];
                }
            } else {
                $wrapped = ['$and' => [$expr]];
            }
        } else {
            // $or / $nor уже в виде {$or|$nor: [...]}
            $wrapped = is_array($expr) ? $expr : [$op => [$expr]];
        }

        if (!empty($this->groupStack)) {
            // Встраиваем в родительскую группу как единый термин
            $this->groupStack[count($this->groupStack) - 1]['terms'][] = $wrapped;
            return $this;
        }

        // Встраиваем на верхний уровень
        if ($this->filter === []) {
            $this->filter = $wrapped;
            return $this;
        }

        if ($orWithPrev) {
            // OR с предыдущим фильтром. Плоское объединение, если возможно
            if (isset($this->filter['$or']) && isset($wrapped['$or'])) {
                $this->filter['$or'] = array_merge($this->filter['$or'], $wrapped['$or']);
            } elseif (isset($this->filter['$or'])) {
                $this->filter['$or'][] = $wrapped;
            } elseif (isset($wrapped['$or'])) {
                $this->filter = ['$or' => array_merge([$this->filter], $wrapped['$or'])];
            } else {
                $this->filter = ['$or' => [$this->filter, $wrapped]];
            }
            return $this;
        }

        // AND c существующим фильтром. Плоское объединение, если возможно
        if (isset($this->filter['$and']) && isset($wrapped['$and'])) {
            $this->filter['$and'] = array_merge($this->filter['$and'], $wrapped['$and']);
            return $this;
        }
        if (isset($this->filter['$and'])) {
            $this->filter['$and'][] = $wrapped;
            return $this;
        }
        if (isset($wrapped['$and'])) {
            $this->filter = ['$and' => array_merge([$this->filter], $wrapped['$and'])];
            return $this;
        }
        $this->filter = ['$and' => [$this->filter, $wrapped]];
        return $this;
    }

    protected function buildClauseFromKeyVal(string $key, $value, bool $negate = false, bool $rawValueIsExpr = false): array
    {
        $field = $key;
        $op = '=';
        if (preg_match('/\s*(>=|<=|<>|!=|=|>|<)\s*$/', $key, $m)) {
            $op = $m[1];
            $field = trim(substr($key, 0, -strlen($m[0])));
            if ($field === '') {
                $field = $key; // fallback
            }
        }
        // Strip table qualifier if present (e.g., "users._id" -> "_id")
        $field = $this->normalizeFieldKey((string)$field);
        if ($rawValueIsExpr && is_array($value) && array_keys($value) !== range(0, count($value) - 1)) {
            // already an operator expression
            $expr = $value;
        } else {
            $expr = $this->mapOperatorToExpr($op, $value);
        }
        // Normalize values for Mongo (ObjectId/UTCDateTime etc.)
        $expr = $this->normalizeExprForField((string)$field, $expr);
        if ($negate) {
            $expr = $this->negateFieldExpr($expr);
        }
        return [$field => $expr];
    }

    protected function normalizeExprForField(string $field, $expr)
    {
        if (is_array($expr) && $this->isAssoc($expr)) {
            $out = [];
            foreach ($expr as $op => $val) {
                if ($op === '$in' || $op === '$nin') {
                    $vals = [];
                    foreach ((array)$val as $vv) {
                        $vals[] = $this->normalizeValueForField($field, $vv);
                    }
                    $out[$op] = $vals;
                } else {
                    $out[$op] = $this->normalizeValueForField($field, $val);
                }
            }
            return $out;
        }
        return $this->normalizeValueForField($field, $expr);
    }

    protected function normalizeValueForField(string $field, $value)
    {
        // DateTime auto-normalization
        if ($value instanceof \DateTimeInterface) {
            // Convert to UTCDateTime (milliseconds)
            $utc = (new \DateTimeImmutable('@' . $value->getTimestamp()))->setTimezone(new \DateTimeZone('UTC'));
            return new \MongoDB\BSON\UTCDateTime($utc->getTimestamp() * 1000);
        }
        // ObjectId detection for id fields (support table-prefixed fields like 'users._id')
        if (is_string($value)) {
            $f = $field;
            $dotPos = strrpos($f, '.');
            if ($dotPos !== false) {
                $f = substr($f, $dotPos + 1);
            }
            if ($f === '_id' || $f === 'id') {
                if (preg_match('/^[a-f0-9]{24}$/i', $value)) {
                    try { return new \MongoDB\BSON\ObjectId($value); } catch (\Throwable $e) { /* ignore */ }
                }
            }
        }
        return $value;
    }

    protected function mapOperatorToExpr(string $op, $value)
    {
        switch ($op) {
            case '=':  return $value;
            case '!=':
            case '<>': return ['$ne' => $value];
            case '>':  return ['$gt' => $value];
            case '>=': return ['$gte' => $value];
            case '<':  return ['$lt' => $value];
            case '<=': return ['$lte' => $value];
            default:   return $value;
        }
    }

    protected function negateFieldExpr($expr)
    {
        if (!is_array($expr)) {
            return ['$ne' => $expr];
        }
        // assoc?
        $isAssoc = $this->isAssoc($expr);
        if (!$isAssoc) {
            return ['$ne' => $expr];
        }
        if (isset($expr['$in'])) {
            return ['$nin' => $expr['$in']];
        }
        if (isset($expr['$nin'])) {
            return ['$in' => $expr['$nin']];
        }
        // Generic negation
        return ['$not' => $expr];
    }

    protected function mergeFieldCondition(string $field, $newExpr): void
    {
        if (!array_key_exists($field, $this->filter)) {
            $this->filter[$field] = $newExpr;
            return;
        }
        $cur = $this->filter[$field];
        // Normalize scalars into $eq
        $curObj = is_array($cur) && $this->isAssoc($cur) ? $cur : ['$eq' => $cur];
        $newObj = is_array($newExpr) && $this->isAssoc($newExpr) ? $newExpr : ['$eq' => $newExpr];
        // Merge operators
        $merged = $curObj + $newObj; // do not overwrite existing operators
        // If both had $eq and different values, last one wins
        if (isset($curObj['$eq']) && isset($newObj['$eq']) && $curObj['$eq'] !== $newObj['$eq']) {
            $merged['$eq'] = $newObj['$eq'];
        }
        $this->filter[$field] = $merged;
    }

    public function escapeLikeString(string $str): string
    {
        // Escape for regex usage
        return preg_quote($str, '/');
    }

    protected function encodeMongoValue($value): string
    {
        // null, bool, int/float
        if ($value === null) {
            return 'null';
        }
        if (is_bool($value)) {
            return $value ? 'true' : 'false';
        }
        if (is_int($value) || is_float($value)) {
            return (string)$value;
        }

        // DateTime -> ISODate("...")
        if ($value instanceof \DateTimeInterface) {
            return 'ISODate("' . $value->format('c') . '")';
        }

        // Mongo UTCDateTime (если к нам попал): преобразуем к ISODate
        if ($value instanceof \MongoDB\BSON\UTCDateTime) {
            $dt = $value->toDateTime();
            $dt = (new \DateTimeImmutable('@' . $dt->getTimestamp()))->setTimezone(new \DateTimeZone('UTC'));
            return 'ISODate("' . $dt->format('c') . '")';
        }
        // ObjectId instance
        if ($value instanceof \MongoDB\BSON\ObjectId) {
            return 'ObjectId("' . (string)$value . '")';
        }

        // Строка — попробуем распознать ObjectId 24 hex
        if (is_string($value)) {
            if (preg_match('/^[a-f0-9]{24}$/i', $value)) {
                return 'ObjectId("' . $value . '")';
            }
            // обычная строка
            return '"' . addcslashes($value, "\\\"\n\r\t") . '"';
        }

        // Регекс в виде ['$regex' => 'p', '$options' => 'i']
        if (is_array($value) && isset($value['$regex'])) {
            $pattern = (string)$value['$regex'];
            $opts    = isset($value['$options']) ? (string)$value['$options'] : '';
            return '/' . str_replace('/', '\/', $pattern) . '/' . $opts;
        }

        // Ассоциативный массив (документ) или список (массив)
        if (is_array($value)) {
            if ($this->isAssoc($value)) {
                $parts = [];
                foreach ($value as $k => $v) {
                    $parts[] = $this->encodeKey($k) . ': ' . $this->encodeMongoValue($v);
                }
                return '{ ' . implode(', ', $parts) . ' }';
            }
            // список
            $items = array_map([$this, 'encodeMongoValue'], $value);
            return '[ ' . implode(', ', $items) . ' ]';
        }

        // Объект -> приведем к массиву публичных свойств
        if (is_object($value)) {
            $arr = get_object_vars($value);
            return $this->encodeMongoValue($arr);
        }

        // fallback
        return json_encode($value, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
    }

    protected function encodeKey(string $k): string
    {
        // допускаем незаключенные в кавычки имена, кроме спец-символов
        if (preg_match('/^[A-Za-z_][A-Za-z0-9_\.]*$/', $k)) {
            return $k;
        }
        return '"' . addcslashes($k, "\\\"\n\r\t") . '"';
    }

    protected function isAssoc(array $arr): bool
    {
        $i = 0;
        foreach ($arr as $k => $_) {
            if ($k !== $i++) {
                return true;
            }
        }
        return false;
    }

    // Доступ к последней скомпилированной mongo-команде (testMode)
    public function getLastCompiled(): ?string
    {
        return $this->lastCompiled;
    }

}
