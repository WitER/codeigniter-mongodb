<?php
declare(strict_types=1);

namespace Tests;

final class ResultTest extends BaseMongoTestCase
{
    public function testResultVariants(): void
    {
        $db = $this->db();
        $tb = $db->table('users');
        $tb->delete([]);

        $docs = [
            ['email' => 'r1@ex.com','name'=>'R1','age'=>10,'active'=>true],
            ['email' => 'r2@ex.com','name'=>'R2','age'=>20,'active'=>false],
            ['email' => 'r3@ex.com','name'=>'R3','age'=>30,'active'=>true],
        ];
        $tb->insertBatch($docs);

        $result = $db->table('users')->orderBy('email','ASC')->get();

        // getNumRows
        $this->assertSame(3, $result->getNumRows());

        // arrays
        $arr = $result->getResultArray();
        $this->assertSame(['r1@ex.com','r2@ex.com','r3@ex.com'], array_column($arr, 'email'));

        // objects
        $result2 = $db->table('users')->orderBy('email','ASC')->get();
        $objs = $result2->getResultObject();
        $this->assertSame('r1@ex.com', $objs[0]->email);

        // getRow / getRowArray
        $result3 = $db->table('users')->orderBy('email','ASC')->get();
        $this->assertSame('r2@ex.com', $result3->getRow(1)->email);
        $this->assertSame('r3@ex.com', $result3->getRowArray(2)['email']);

        // first/last row helpers
        $result4 = $db->table('users')->orderBy('email','ASC')->get();
        $this->assertSame('r1@ex.com', $result4->getFirstRow()->email);
        $this->assertSame('r3@ex.com', $result4->getLastRow()->email);
    }
}
