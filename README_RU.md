# CodeIgniter 4 MongoDB Driver

[![Build & Tests](https://github.com/WitER/codeigniter-mongodb/actions/workflows/php.yml/badge.svg)](https://github.com/WitER/codeigniter-mongodb/actions/workflows/php.yml)
[![Packagist Version](https://img.shields.io/packagist/v/witer/codeigniter-mongodb.svg)](https://packagist.org/packages/witer/codeigniter-mongodb)
[![PHP Version](https://img.shields.io/packagist/php-v/witer/codeigniter-mongodb.svg)](https://packagist.org/packages/witer/codeigniter-mongodb)
[![Downloads](https://img.shields.io/packagist/dt/witer/codeigniter-mongodb.svg)](https://packagist.org/packages/witer/codeigniter-mongodb)
[![License: MIT](https://img.shields.io/packagist/l/witer/codeigniter-mongodb.svg)](LICENSE)
[![GitHub issues](https://img.shields.io/github/issues/WitER/codeigniter-mongodb.svg)](https://github.com/WitER/codeigniter-mongodb/issues)
[![GitHub stars](https://img.shields.io/github/stars/WitER/codeigniter-mongodb.svg?style=social)](https://github.com/WitER/codeigniter-mongodb)

> **Дисклеймер:** Этот драйвер не претендует на высокое качество кода и работоспособность, если вам он нужен или есть идеи\желание\время на его доработку - будет отлично


Нативный драйвер базы данных MongoDB для CodeIgniter 4. Драйвер полностью интегрируется с фабрикой соединений CI4 (Config\Database::connect()) и Query Builder’ом, предоставляет удобный API для CRUD‑операций, агрегаций и транзакций MongoDB, сохраняя привычные интерфейсы CodeIgniter.

Сайт пакета: https://github.com/WitER/codeigniter-mongodb
Пакет на Packagist: https://packagist.org/packages/witer/codeigniter-mongodb

## Возможности
- Подключение к MongoDB через стандартный интерфейс CodeIgniter DB (`DBDriver => 'MongoDB'`).
- Поддержка большинства методов Query Builder: select, where/orWhere, whereIn, like, orderBy, limit/offset, insert/update/delete, insertBatch/updateBatch/replace и др.
- Специальные Mongo‑операции в билдере: setInc/setDec ($inc), unsetField ($unset), setOnInsert ($setOnInsert), push/pull/addToSet (работа с массивами), upsert‑флаг, разрешение записей без фильтра.
- Агрегации: selectMin/Max/Avg/Sum/Count и построение pipeline для aggregate().
- Транзакции MongoDB через сессии драйвера (требуется реплика‑сет).
- Поддержка getCompiled… для отладки: вывод «псевдо‑SQL»/Mongo JSON запроса.
- Совместимость с моделями/сущностями CodeIgniter 4 (в т.ч. кастинг полей).
- Методы introspection: список коллекций, полей, индексная информация, чтение схемы валидатора.

## Требования
- PHP >= 8.1
- Расширение ext-mongodb ^1.19
- Библиотека mongodb/mongodb 1.19.x
- CodeIgniter 4 (ветка develop для разработки тестов)

См. composer.json для точных версий зависимостей.

## Установка
Через Composer:

```
composer require witer/codeigniter-mongodb
```

Убедитесь, что PHP‑расширение ext-mongodb установлено и активно.

## Быстрый старт
```php
use Config\Database;

$db = Database::connect();            // Группа по умолчанию из Config\Database
$builder = $db->table('users');       // Коллекция users

// Вставка
$builder->insert([
    'email' => 'john@example.com',
    'name'  => 'John',
]);

// Поиск
$user = $builder->where('email', 'john@example.com')->get()->getFirstRow();

// Обновление с инкрементом
$builder->where('email', 'john@example.com')
        ->setInc('logins', 1)
        ->update();

// Удаление
$builder->where('email', 'john@example.com')->delete();
```

## Конфигурация
Вы можете настроить подключение в app/Config/Database.php или через .env.

Пример .env:
```
# Группа по умолчанию
database.default.DBDriver = MongoDB

# Вариант 1: DSN (рекомендуется)
database.default.dsn = mongodb+srv://user:pass@cluster0.mongodb.net/mydatabase?retryWrites=true&w=majority

# Вариант 2: отдельные параметры
# database.default.hostname = localhost
# database.default.port = 27017
# database.default.username = user
# database.default.password = pass
# database.default.database = mydatabase
```

Пример app/Config/Database.php:
```php
public array $default = [
    'DBDriver' => 'MongoDB',
    'dsn'      => 'mongodb://localhost:27017/mydatabase',
    // либо hostname/port/username/password/database
];
```

Смена базы данных на лету:
```php
$db->setDatabase('another_db');
```

## Использование Query Builder
- select([...]) — перечисление полей (по умолчанию «*»). Для вложенных полей используйте точку: `profile.age`.
- where()/orWhere()/whereNot()/orWhereNot() — условия, поддерживаются массивы и выражения.
- whereIn()/whereNotIn()/orWhereIn()/orWhereNotIn()
- like()/notLike()/orLike()/orNotLike() — регистронезависимый поиск поддерживается флагом $insensitiveSearch.
- groupBy()/orderBy()/limit()/offset()
- insert()/insertBatch()/update()/updateBatch()/replace()/delete()
- countAll()/countAllResults()

Специфичные для Mongo методы:
- setInc('field', n)/setDec('field', n) — $inc
- unsetField('field') — $unset
- setOnInsert('field', value) — $setOnInsert
- push('field', value)/pull('field', value)/addToSet('field', value)
- setUpsertFlag(true|false) — upsert при update/replace
- allowNoFilterWrite(true|false) — разрешить операции записи без where

Примеры:
```php
// Выборка с проекцией и сортировкой
$rows = $db->table('orders')
    ->select(['_id', 'total', 'status'])
    ->whereIn('status', ['new', 'paid'])
    ->orderBy('created_at', 'DESC')
    ->limit(20)
    ->get()
    ->getResult();

// Массивные операции с push/addToSet
$db->table('users')
   ->where('_id', '65123abc...') // 24‑симв. hex будет автоматически конвертирован в ObjectId
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

### Агрегации
Базовые агрегаты:
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
Для сложных pipeline используйте Query Builder цепочки where/group/order/limit — драйвер соберет эквивалентный Mongo aggregate. Посмотреть собранный запрос можно через getCompiled…:
```php
echo $db->table('orders')->where('status', 'paid')->getCompiledSelect();
```

### Транзакции
Драйвер поддерживает транзакции через стандартные методы CodeIgniter:
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
Примечание: транзакции требуют реплика‑сета в MongoDB.

## Модели и сущности
Драйвер работает с `CodeIgniter\Model` и кастингом сущностей.

Пример модели:
```php
use CodeIgniter\Model;

class UserModel extends Model
{
    protected $table      = 'users';
    protected $primaryKey = '_id';
    protected $useSoftDeletes = true;           // поддержка soft deletes через поле deleted_at
    protected $allowedFields = ['email','name','roles','log'];

    protected $returnType = \App\Entities\User::class; // опционально

    protected $casts = [
        '_id'        => 'objectid',
        'created_at' => 'datetime',
        'roles'      => 'array',
    ];
}
```

## Работа с ObjectId и датами
- Поле `_id`: строка из 24 hex символов автоматически конвертируется в `MongoDB\BSON\ObjectId`. При необходимости можно передавать `new ObjectId($hex)` вручную.
- Даты: используйте `MongoDB\BSON\UTCDateTime` или `DateTimeInterface` (при кастинге сущностей/моделей).

## Инспекция и схема
- `getFieldNames($collection)` — список полей согласно схеме валидации коллекции.
- `getFieldData($collection)` — типы/ограничения из валидатора.
- `tableExists($collection)` — проверка коллекции.
- Индексы: `_indexData($collection)` вернет информацию об индексах.

## Ограничения
- JOIN отсутствуют — используйте денормализацию или агрегации `$lookup` при необходимости (кастомно через коллекцию).
- Сырые SQL не поддерживаются; используйте Query Builder либо обращайтесь к нативной коллекции через `$builder->getCollection()`.
- Некоторые возможности Forge для РСУБД недоступны или ограничены.

## Доступ к нативной коллекции MongoDB
```php
$collection = $db->table('users')->getCollection(); // MongoDB\Collection
$cursor = $collection->find(['status' => 'active']);
```

## Обработка ошибок и отладка
- `$db->error()` — код и сообщение последней ошибки.
- `$db->affectedRows()` — число затронутых документов для последней операции.
- `$db->insertID()` — последний вставленный `_id` (если применимо).
- `getCompiledSelect()/getCompiledUpdate()/…` — посмотреть, что будет отправлено в MongoDB.

## Тесты
Локальный запуск:
```
composer install
composer test
```
В репозитории настроен GitHub Actions workflow (php.yml), который запускает тесты автоматически. См. статус по беджу в начале README.

## Миграции и сиды
Вы можете использовать стандартные механизмы CI4. В тестах есть примеры миграций/сидов для MongoDB (см. каталог tests/_support).

## FAQ
- Бейдж CI не отображается? Убедитесь, что файл workflow называется `php.yml` и находится в `.github/workflows`. В локальном дереве проекта директория может отличаться, однако для GitHub важен путь `.github/workflows/php.yml`.
- Как задать строку подключения? Через `dsn` (рекомендуется) или отдельные параметры `hostname/port/username/password/database`.

## Лицензия
MIT (см. файл LICENSE).