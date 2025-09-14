<?php
declare(strict_types=1);

namespace Tests;

use CodeIgniter\Database\MongoDB\Forge as MongoForge;
use Config\Database;

final class ForgeTest extends BaseMongoTestCase
{
    private function forge(): MongoForge
    {
        /** @var MongoForge $forge */
        $forge = Database::forge($this->DBGroup);
        $this->assertInstanceOf(MongoForge::class, $forge);
        return $forge;
    }

    private function uniqueName(string $prefix): string
    {
        return $prefix . '_' . bin2hex(random_bytes(3));
    }

    public function testCreateTableWithFieldsAndIndexes(): void
    {
        $forge = $this->forge();
        $db = $this->db();
        $table = $this->uniqueName('forge_ct');

        // Define fields and keys
        $forge->addField([
            '_id'   => ['type' => 'char', 'constraint' => 24],
            'email' => ['type' => 'varchar', 'constraint' => 120, 'null' => false],
            'age'   => ['type' => 'int', 'min' => 0, 'max' => 200, 'null' => true],
        ]);
        $forge->addUniqueKey('email');
        $forge->addKey('age');

        try {
            $this->assertTrue($forge->createTable($table, true));

            // Collection exists
            $this->assertTrue($db->tableExists($db->DBPrefix . $table, false));

            // Schema-derived fields
            $fields = $db->getFieldNames($table);
            $this->assertContains('email', $fields);
            $this->assertContains('age', $fields);

            // Field metadata
            $meta = $db->getFieldData($table);
            $this->assertNotEmpty($meta);
            $email = null;
            foreach ($meta as $m) {
                if ($m->name === 'email') { $email = $m; break; }
            }
            $this->assertNotNull($email);
            $this->assertSame('string', $email->type);
            $this->assertFalse($email->nullable);

            // Indexes (unique on email, normal on age)
            $indexes = $forge->listIndexes($table);
            $this->assertIsArray($indexes);
            $hasEmailUnique = false;
            $hasAgeIndex = false;
            foreach ($indexes as $idx) {
                $key = $idx['key'] ?? [];
                if (isset($key['email']) && ($idx['unique'] ?? false)) {
                    $hasEmailUnique = true;
                }
                if (isset($key['age']) && !($idx['unique'] ?? false)) {
                    $hasAgeIndex = true;
                }
            }
            $this->assertTrue($hasEmailUnique, 'Expected unique index on email');
            $this->assertTrue($hasAgeIndex, 'Expected index on age');
        } finally {
            // Cleanup
            $forge->dropTable($table, true);
        }
    }

    public function testDropTable(): void
    {
        $forge = $this->forge();
        $db = $this->db();
        $table = $this->uniqueName('forge_dt');

        $forge->addField(['_id' => ['type' => 'char', 'constraint' => 24]]);
        $this->assertTrue($forge->createTable($table, true));
        $this->assertTrue($db->tableExists($db->DBPrefix . $table, false));

        $this->assertTrue($forge->dropTable($table));
        $this->assertFalse($db->tableExists($db->DBPrefix . $table, false));

        // ifExists should not error
        $this->assertTrue($forge->dropTable($table, true));
    }

    public function testCreateAndDropIndexes(): void
    {
        $forge = $this->forge();
        $db = $this->db();
        $table = $this->uniqueName('forge_idx');

        $forge->addField(['_id' => ['type' => 'char', 'constraint' => 24]]);
        $forge->createTable($table, true);

        try {
            $defs = [
                [ 'fields' => ['a' => 1], 'options' => ['unique' => true, 'name' => 'uniq_a'] ],
                [ 'fields' => ['b' => -1], 'options' => ['name' => 'idx_b'] ],
            ];
            $this->assertTrue($forge->createIndexes($table, $defs));

            $idx = $forge->listIndexes($table);
            $names = array_map(static fn($i) => (string)($i['name'] ?? ''), $idx);
            $this->assertContains('uniq_a', $names);
            $this->assertContains('idx_b', $names);

            // Drop single index
            $this->assertTrue($forge->dropIndexes($table, 'idx_b'));
            $idx = $forge->listIndexes($table);
            $names = array_map(static fn($i) => (string)($i['name'] ?? ''), $idx);
            $this->assertNotContains('idx_b', $names);
            $this->assertContains('uniq_a', $names);

            // Drop the remaining
            $this->assertTrue($forge->dropIndexes($table, ['uniq_a']));
            $idx = $forge->listIndexes($table);
            $names = array_map(static fn($i) => (string)($i['name'] ?? ''), $idx);
            $this->assertSame(['_id_'], array_values(array_unique($names)));
        } finally {
            $forge->dropTable($table, true);
        }
    }

    public function testRenameTable(): void
    {
        $forge = $this->forge();
        $db = $this->db();
        $src = $this->uniqueName('forge_ren');
        $dst = $src . '_new';

        $forge->addField(['_id' => ['type' => 'char', 'constraint' => 24]]);
        $forge->createTable($src, true);

        try {
            $this->assertTrue($forge->renameTable($src, $dst));
            $this->assertFalse($db->tableExists($db->DBPrefix . $src, false));
            $this->assertTrue($db->tableExists($db->DBPrefix . $dst, false));
        } finally {
            $forge->dropTable($dst, true);
            $forge->dropTable($src, true); // in case rename failed
        }
    }

    public function testAddAndDropColumnUpdatesSchema(): void
    {
        $forge = $this->forge();
        $db = $this->db();
        $table = $this->uniqueName('forge_col');

        $forge->addField([
            '_id' => ['type' => 'char', 'constraint' => 24],
            'title' => ['type' => 'varchar', 'constraint' => 50, 'null' => true],
        ]);
        $forge->createTable($table, true);

        try {
            // Add required int field
            $this->assertTrue($forge->addColumn($table, [
                'votes' => ['type' => 'int', 'null' => false, 'min' => 0],
            ]));

            $fields = $db->getFieldNames($table);
            $this->assertContains('votes', $fields);
            $meta = $db->getFieldData($table);
            $votes = null;
            foreach ($meta as $m) { if ($m->name === 'votes') { $votes = $m; break; } }
            $this->assertNotNull($votes);
            $this->assertSame('int', $votes->type);
            $this->assertFalse($votes->nullable);

            // Now drop the field (validator only)
            $this->assertTrue($forge->dropColumn($table, 'votes'));
            $fields = $db->getFieldNames($table);
            $this->assertNotContains('votes', $fields);
        } finally {
            $forge->dropTable($table, true);
        }
    }

    public function testRenameColumnUpdatesSchema(): void
    {
        $forge = $this->forge();
        $db = $this->db();
        $table = $this->uniqueName('forge_rencol');

        $forge->addField([
            '_id' => ['type' => 'char', 'constraint' => 24],
            'old' => ['type' => 'varchar', 'constraint' => 20, 'null' => false],
        ]);
        $forge->createTable($table, true);

        try {
            // Rename validator field only (no data migration)
            $this->assertTrue($forge->renameColumn($table, [ 'old' => ['name' => 'new'] ]));

            $fields = $db->getFieldNames($table);
            $this->assertContains('new', $fields);
            $this->assertNotContains('old', $fields);

            // Metadata should reflect new field and remain non-nullable
            $meta = $db->getFieldData($table);
            $found = null;
            foreach ($meta as $m) { if ($m->name === 'new') { $found = $m; break; } }
            $this->assertNotNull($found);
            $this->assertSame('string', $found->type);
            $this->assertFalse($found->nullable);
        } finally {
            $forge->dropTable($table, true);
        }
    }
}
