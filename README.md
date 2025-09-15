# CodeIgniter 4 MongoDB Driver

[![Build & Tests](https://github.com/WitER/codeigniter-mongodb/actions/workflows/php.yml/badge.svg)](https://github.com/WitER/codeigniter-mongodb/actions/workflows/php.yml)
[![Packagist Version](https://img.shields.io/packagist/v/witer/codeigniter-mongodb.svg)](https://packagist.org/packages/witer/codeigniter-mongodb)
[![PHP Version](https://img.shields.io/packagist/php-v/witer/codeigniter-mongodb.svg)](https://packagist.org/packages/witer/codeigniter-mongodb)
[![Downloads](https://img.shields.io/packagist/dt/witer/codeigniter-mongodb.svg)](https://packagist.org/packages/witer/codeigniter-mongodb)
[![GitHub issues](https://img.shields.io/github/issues/WitER/codeigniter-mongodb.svg)](https://github.com/WitER/codeigniter-mongodb/issues)
[![GitHub stars](https://img.shields.io/github/stars/WitER/codeigniter-mongodb.svg?style=social)](https://github.com/WitER/codeigniter-mongodb)

> **Disclaimer:** This driver doesn't claim to have high code quality and functionality. If you need it or have ideas/desire/time to improve it, it would be great.


Native MongoDB database driver for CodeIgniter 4. Fully integrates with CI4's connection factory (Config\Database::connect()) and the Query Builder.
It provides a convenient API for MongoDB CRUD operations, aggregations, and transactions while keeping familiar CodeIgniter interfaces.


## Features
- Connect to MongoDB through the standard CodeIgniter DB interface (`DBDriver => 'MongoDB'`).
- Most Query Builder methods supported: `select`, `where`/`orWhere`, `whereIn`, `like`, `orderBy`, `limit`/`offset`, `insert`/`update`/`delete`, `insertBatch`/`updateBatch`/`replace`, and more.
- Mongo-specific builder operations: `setInc`/`setDec` ($inc), `unsetField` ($unset), `setOnInsert` ($setOnInsert), `push`/`pull`/`addToSet` (array operations), upsert flag, allow writes without a filter. (**NOT TESTED**)
- Aggregations: `select[Min|Max|Avg|Sum|Count]` and pipeline building for aggregate().
- MongoDB transactions via driver sessions (**replica set required**).
- getCompiled… helpers for debugging: output of "pseudo-SQL"/Mongo JSON request.
- Compatible with CodeIgniter 4 Models/Entities (including field casting).
- Introspection methods: list collections, fields, index info, read validator schema.

## Requirements
- PHP >= 8.1
- ext-mongodb ^1.19
- mongodb/mongodb 1.19.x
- CodeIgniter 4 (develop branch is used for tests in this repo)

See composer.json for exact versions.

## Installation
Via Composer:

```
composer require witer/codeigniter-mongodb
```

Make sure the ext-mongodb PHP extension is installed and enabled.

## Quick start
```php
use Config\Database;

$db = Database::connect();            // Default group from Config\Database
$builder = $db->table('users');       // 'users' collection

// Insert
$builder->insert([
    'email' => 'john@example.com',
    'name'  => 'John',
]);

// Find
$user = $builder->where('email', 'john@example.com')->get()->getFirstRow();

// Update with increment
$builder->where('email', 'john@example.com')
        ->setInc('logins', 1)
        ->update();

// Delete
$builder->where('email', 'john@example.com')->delete();
```

## Configuration
You can configure the connection in app/Config/Database.php or via .env.

Example .env:
```
# Default group
database.default.DBDriver = MongoDB

# Option 1: DSN (recommended)
database.default.dsn = mongodb+srv://user:pass@cluster0.mongodb.net/mydatabase?retryWrites=true&w=majority

# Option 2: separate parameters
# database.default.hostname = localhost
# database.default.port = 27017
# database.default.username = user
# database.default.password = pass
# database.default.database = mydatabase
```

Example app/Config/Database.php:
```php
public array $default = [
    'DBDriver' => 'MongoDB',
    'dsn'      => 'mongodb://localhost:27017/mydatabase',
    // or hostname/port/username/password/database
];
```

Switch database at runtime:
```php
$db->setDatabase('another_db');
```

## Using the Query Builder
- select([...]) — list of fields (default is "*").
- where()/orWhere()/whereNot()/orWhereNot() — conditions, arrays and expressions supported.
- whereIn()/whereNotIn()/orWhereIn()/orWhereNotIn()
- like()/notLike()/orLike()/orNotLike() — case-insensitive search is supported via $insensitiveSearch flag.
- groupBy()/orderBy()/limit()/offset()
- insert()/insertBatch()/update()/updateBatch()/replace()/delete()
- countAll()/countAllResults()

Mongo-specific methods:
- setInc('field', n)/setDec('field', n) — $inc
- unsetField('field') — $unset
- setOnInsert('field', value) — $setOnInsert
- push('field', value)/pull('field', value)/addToSet('field', value)
- setUpsertFlag(true|false) — upsert for update/replace

Examples:
```php
// Projection and sorting
$rows = $db->table('orders')
    ->select(['_id', 'total', 'status'])
    ->whereIn('status', ['new', 'paid'])
    ->orderBy('created_at', 'DESC')
    ->limit(20)
    ->get()
    ->getResult();

// Array operations with push/addToSet
$db->table('users')
   ->where('_id', '65123abc...') // 24-char hex will be auto-converted to ObjectId
   ->addToSet('roles', 'admin')
   ->push('log', ['at' => new \MongoDB\BSON\UTCDateTime(), 'ev' => 'grant'])
   ->update();

// Upsert
$db->table('counters')
   ->where('_id', 'page_views')
   ->setInc('value', 1)
   ->setOnInsert('created_at', new \MongoDB\BSON\UTCDateTime())
   ->setUpsertFlag(true)
   ->update();
```

### Aggregations
Basic aggregates:
```php
$stats = $db->table('orders')
    ->selectSum('total', 'sumTotal')
    ->selectAvg('total', 'avgTotal')
    ->selectMax('total', 'maxTotal')
    ->selectMin('total', 'minTotal')
    ->where('status', 'paid')
    ->get()
    ->getFirstRow();
```
For complex pipelines use Query Builder where/group/order/limit chains — the driver will build an equivalent Mongo aggregate. Inspect the built query via getCompiled…:
```php
echo $db->table('orders')->where('status', 'paid')->getCompiledSelect();
```

### Transactions
The driver supports transactions via standard CodeIgniter methods:
```php
$db->transBegin();
try {
    $db->table('wallets')->where('_id', $id)->setDec('balance', 100)->update();
    $db->table('wallets')->where('_id', $id2)->setInc('balance', 100)->update();
    $db->transCommit();
} catch (\Throwable $e) {
    $db->transRollback();
    throw $e;
}
```
Note: transactions require a MongoDB replica set.

## Models and Entities
The driver works with `CodeIgniter\Model` and entity casting.

Example model:
```php
use CodeIgniter\Model;

class UserModel extends Model
{
    protected $table      = 'users';
    protected $primaryKey = '_id';
    protected $useSoftDeletes = true;           // soft deletes via deleted_at field
    protected $allowedFields = ['email','name','roles','log'];

    protected $returnType = \App\Entities\User::class; // optional

    protected $casts = [
        '_id'        => 'string',
        'created_at' => 'datetime',
        'roles'      => 'array',
    ];
}
```

## Working with ObjectId and dates
- `_id` field: a 24-hex-character string is automatically converted to `MongoDB\BSON\ObjectId`. You may also pass `new ObjectId($hex)` manually if needed.
- Dates: use `MongoDB\BSON\UTCDateTime` or any `DateTimeInterface` (with model/entity casting).

## Introspection and schema
- `getFieldNames($collection)` — list of fields according to the collection's validation schema.
- `getFieldData($collection)` — types/constraints from the validator.
- `tableExists($collection)` — check for collection existence.
- Indexes: `_indexData($collection)` returns index information.

## Limitations
- No JOINs — use denormalization or aggregation `$lookup` when needed (manually via the collection).
- Raw SQL is not supported; use the Query Builder or access the native collection via `$builder->getCollection()`.
- Some Forge features for RDBMS are unavailable or limited.

## Access to native MongoDB collection
```php
$collection = $db->table('users')->getCollection(); // MongoDB\Collection
$cursor = $collection->find(['status' => 'active']);
```

## Error handling and debugging
- `$db->error()` — last error code and message.
- `$db->affectedRows()` — number of affected documents for the last operation.
- `$db->insertID()` — last inserted `_id` (if applicable).
- `getCompiledSelect()/getCompiledUpdate()/…` — see what will be sent to MongoDB.

## Tests
Run locally:
```
composer install
composer test
```
GitHub Actions workflow (php.yml) is configured to run tests automatically. See the badge at the top of this README.

## Migrations and seeds
You can use the standard CI4 mechanisms. Tests contain examples of migrations/seeds for MongoDB (see tests/_support folder).

## License
MIT (see LICENSE file).
