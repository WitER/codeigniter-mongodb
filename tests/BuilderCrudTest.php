<?php
declare(strict_types=1);

namespace Tests;

use CodeIgniter\Database\MongoDB\Connection;

final class BuilderCrudTest extends BaseMongoTestCase
{
    public function testCrudOperationsWithBuilder(): void
    {
        /**
         * @var Connection $db
         */
        $db = $this->db();
        $builder = $db->table('users');

        // Clean collection for this test
        $builder->delete([]);

        // Insert one
        $data = [
            'email'  => 'john@example.com',
            'name'   => 'John',
            'age'    => 30,
            'active' => true,
            'roles'  => ['user'],
            'meta'   => ['city' => 'NY'],
        ];
        $this->assertTrue($builder->insert($data));
        $id1 = (string) $db->insertID();
        $this->assertNotEmpty($id1);

        // Insert batch
        $batch = [];
        for ($i=0; $i<5; $i++) {
            $batch[] = [
                'email'  => "user$i@example.com",
                'name'   => "User $i",
                'age'    => 20 + $i,
                'active' => ($i % 2) === 0,
            ];
        }
        $builder->insertBatch($batch);

        // Get where
        $res = $builder->where('email', 'john@example.com')->get()->getResultArray();
        $this->assertCount(1, $res);
        $this->assertSame('John', $res[0]['name']);

        // Count
        $count = $db->table('users')->countAllResults();
        $this->assertGreaterThanOrEqual(6, $count);

        // Update
        $this->assertTrue($db->table('users')->where('email', 'john@example.com')->update(['age' => 31]));
        $john = $db->table('users')->where('email', 'john@example.com')->get()->getRowArray();
        $this->assertSame(31, (int) $john['age']);

        // Update batch
        $batchUpd = [
            ['email' => 'user0@example.com', 'name' => 'User Zero'],
            ['email' => 'user1@example.com', 'name' => 'User One'],
        ];
        $db->table('users')->updateBatch($batchUpd, 'email');
        $u0 = $db->table('users')->where('email', 'user0@example.com')->get()->getRowArray();
        $this->assertSame('User Zero', $u0['name']);

        // Order/limit/offset
        $rows = $db->table('users')->select('email,age')->orderBy('age', 'DESC')->limit(3, 1)->get()->getResultArray();
        $this->assertNotEmpty($rows);
        $this->assertArrayHasKey('email', $rows[0]);

        // Delete
        $this->assertTrue($db->table('users')->where('email', 'john@example.com')->delete());
        $this->assertSame(0, $db->table('users')->where('email', 'john@example.com')->countAllResults());
    }
}
