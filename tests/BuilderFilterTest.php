<?php
declare(strict_types=1);

namespace Tests;

final class BuilderFilterTest extends BaseMongoTestCase
{
    public function testWhereCombinationsAndProjection(): void
    {
        $db = $this->db();
        $tb = $db->table('users');
        // Clean
        $tb->delete([]);

        // Seed diverse data
        $docs = [
            ['email' => 'x1@ex.com','name'=>'Ann','age'=>18,'active'=>true,'roles'=>['user'],'meta'=>['country'=>'RU']],
            ['email' => 'x2@ex.com','name'=>'Bob','age'=>25,'active'=>true,'roles'=>['admin'],'meta'=>['country'=>'US']],
            ['email' => 'x3@ex.com','name'=>'Cat','age'=>30,'active'=>false,'roles'=>['user','editor'],'meta'=>['country'=>'RU']],
            ['email' => 'x4@ex.com','name'=>'Dan','age'=>40,'active'=>true,'roles'=>[],'meta'=>['country'=>'DE']],
            ['email' => 'x6@ex.com','name'=>'Dan','age'=>37,'active'=>true,'roles'=>[],'meta'=>['country'=>'DE']],
            ['email' => 'x5@ex.com','name'=>'Eve','age'=>5,'active'=>false,'roles'=>['guest'],'meta'=>null],
        ];
        $tb->insertBatch($docs);

        // where + orWhere
        $rows = $db->table('users')
            ->where('active', true)
            ->orWhere('age', 30)
            ->orderBy('email', 'ASC')
            ->get()->getResultArray();
        $emails = array_column($rows, 'email');
        $this->assertSame(['x1@ex.com','x2@ex.com','x3@ex.com','x4@ex.com', 'x6@ex.com'], $emails);

        // whereIn / notIn
        $rows = $db->table('users')->whereIn('email', ['x2@ex.com','x5@ex.com','zzz'])->get()->getResultArray();
        $this->assertCount(2, $rows);
        $rows = $db->table('users')->whereNotIn('email', ['x1@ex.com','x2@ex.com','x3@ex.com','x4@ex.com','x5@ex.com', 'x6@ex.com'])->get()->getResultArray();
        $this->assertCount(0, $rows);

        // like (case-insensitive)
        $rows = $db->table('users')->like('name', 'a', 'both', null, true)->orderBy('name','ASC')->get()->getResultArray();
        $this->assertSame(['Ann','Cat','Dan', 'Dan'], array_column($rows,'name'));

        // between / notBetween
        $between = $db->table('users')->between('age', 20, 35)->get()->getResultArray();
        $this->assertEquals(['x2@ex.com','x3@ex.com'], array_column($between,'email'));
        $notBetween = $db->table('users')->notBetween('age', 20, 35)->get()->getResultArray();
        $this->assertContains('x1@ex.com', array_column($notBetween,'email'));
        $this->assertContains('x4@ex.com', array_column($notBetween,'email'));

        // null checks
        $isNull = $db->table('users')->where('meta', null)->get()->getResultArray();
        $this->assertSame(['x5@ex.com'], array_column($isNull,'email'));

        // projection via select
        $rows = $db->table('users')->select('email,age')->orderBy('email','ASC')->get()->getResultArray();
        $this->assertArrayHasKey('email', $rows[0]);
        $this->assertArrayHasKey('age', $rows[0]);
        $this->assertArrayNotHasKey('name', $rows[0]);

        // distinct
        $names = $db->table('users')->select('name')->distinct(true)->get()->getResultArray();
        $vals = array_values(array_filter(array_unique(array_map(static function($r){ return $r['name'] ?? null; }, $names))));
        sort($vals);
        $this->assertSame(['Ann', 'Bob', 'Cat', 'Dan', 'Eve'], $vals);

        // limit/offset via methods and get parameters
        $this->assertCount(2, $db->table('users')->orderBy('email','ASC')->limit(2)->get()->getResultArray());
        $this->assertCount(2, $db->table('users')->orderBy('email','ASC')->get(2, 2)->getResultArray());
    }
}
