<?php
declare(strict_types=1);

namespace Tests;

use CodeIgniter\Database\BaseConnection;
use CodeIgniter\Database\MongoDB\Connection;

final class ConnectionTest extends BaseMongoTestCase
{
    public function testConnectsAndListsCollections(): void
    {
        /**
         * @var Connection $db
         */
        $db = $this->db();
        $this->assertInstanceOf(BaseConnection::class, $db);

        // Force connect
        $db->connect();

        // Ensure our migrated collection exists with prefix
        $exists = $db->tableExists('users');
        $this->assertTrue($exists, 'users collection should exist after migration');

        $tables = $db->listTables();
        $this->assertIsArray($tables);
        $this->assertNotEmpty(array_filter($tables, static fn($t) => str_contains((string)$t, 'users')));

        // Check schema fields mapping
        $fields = $db->getFieldNames('users');
        $this->assertContains('email', $fields);
        $this->assertContains('age', $fields);

        // Check schema fields meta
        $fieldsMeta = $db->getFieldData('users');
        $this->assertNotEmpty($fieldsMeta);
        $this->assertContainsEquals((object)[
            'name' => 'name',
            'type' => 'string',
            'max_length' => 60,
            'default' => null,
            'nullable' => false,
            'primary_key' => false
        ], $fieldsMeta);
        $this->assertContainsEquals((object)[
            'name' => 'roles',
            'type' => 'array|null',
            'max_length' => null,
            'default' => null,
            'nullable' => true,
            'primary_key' => false
        ], $fieldsMeta);
    }
}
