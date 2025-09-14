<?php
declare(strict_types=1);

namespace Tests;

final class BuilderAggregateTest extends BaseMongoTestCase
{
    public function testAggregationWithBuilder(): void
    {
        $db = $this->db();
        $tb = $db->table('users');
        // Clean
        $tb->delete([]);

        // Seed sample
        $docs = [
            ['email' => 'a@ex.com','name'=>'A','age'=>10,'active'=>true],
            ['email' => 'b@ex.com','name'=>'B','age'=>15,'active'=>false],
            ['email' => 'c@ex.com','name'=>'C','age'=>20,'active'=>true],
            ['email' => 'd@ex.com','name'=>'D','age'=>25,'active'=>false],
        ];
        $tb->insertBatch($docs);

        // Aggregate: by active -> count, sum/avg/min/max age
        $res = $db->table('users')
            ->selectCount('_id', 'cnt')
            ->selectSum('age', 'sumAge')
            ->selectAvg('age', 'avgAge')
            ->selectMin('age', 'minAge')
            ->selectMax('age', 'maxAge')
            ->groupBy('active')
            ->orderBy('active', 'DESC')
            ->get()->getResultArray();

        $this->assertCount(2, $res);
        // First row: active = true
        $row1 = $res[0];
        $this->assertArrayHasKey('active', $row1);
        $this->assertTrue((bool) $row1['active']);
        $this->assertSame(2, (int) $row1['cnt']);
        $this->assertSame(30, (int) $row1['sumAge']);
        $this->assertSame(10, (int) $row1['minAge']);
        $this->assertSame(20, (int) $row1['maxAge']);

        // Second row: active = false
        $row2 = $res[1];
        $this->assertFalse((bool) $row2['active']);
        $this->assertSame(2, (int) $row2['cnt']);
        $this->assertSame(40, (int) $row2['sumAge']);
        $this->assertSame(15, (int) $row2['minAge']);
        $this->assertSame(25, (int) $row2['maxAge']);
    }
}
