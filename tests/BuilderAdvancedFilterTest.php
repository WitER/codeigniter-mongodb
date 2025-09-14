<?php
declare(strict_types=1);

namespace Tests;

final class BuilderAdvancedFilterTest extends BaseMongoTestCase
{
    public function testGroupsNegationsAndLikeVariants(): void
    {
        $db = $this->db();
        $tb = $db->table('users');

        // Clean and seed
        $tb->delete([]);
        $docs = [
            ['email' => 'x1@ex.com','name'=>'Ann','age'=>18,'active'=>true,'roles'=>['user'],'meta'=>['country'=>'RU']],
            ['email' => 'x2@ex.com','name'=>'Bob','age'=>25,'active'=>true,'roles'=>['admin'],'meta'=>['country'=>'US']],
            ['email' => 'x3@ex.com','name'=>'Cat','age'=>30,'active'=>false,'roles'=>['user','editor'],'meta'=>['country'=>'RU']],
            ['email' => 'x4@ex.com','name'=>'Dan','age'=>40,'active'=>true,'roles'=>[],'meta'=>['country'=>'DE']],
            ['email' => 'x6@ex.com','name'=>'Dan','age'=>37,'active'=>true,'roles'=>[],'meta'=>['country'=>'DE']],
            ['email' => 'x5@ex.com','name'=>'Eve','age'=>5, 'active'=>false,'roles'=>['guest'],'meta'=>null],
        ];
        $tb->insertBatch($docs);

        // whereNot
        $rows = $db->table('users')->whereNot('name', 'Ann')->get()->getResultArray();
        $this->assertCount(5, $rows);

        // orWhereNot combined with where
        $rows = $db->table('users')->where('name', 'Ann')->orWhereNot('age', 40)->get()->getResultArray();
        $emails = array_column($rows, 'email'); sort($emails);
        $this->assertSame(['x1@ex.com','x2@ex.com','x3@ex.com','x5@ex.com','x6@ex.com'], $emails);

        // Grouping: AND with inner OR
        $rows = $db->table('users')
            ->where('active', true)
            ->groupStart()
                ->where('age >', 30)
                ->orWhere('name', 'Bob')
            ->groupEnd()
            ->orderBy('email','ASC')
            ->get()->getResultArray();
        $this->assertSame(['x2@ex.com','x4@ex.com','x6@ex.com'], array_column($rows, 'email'));

        // Grouping: OR the closed group with previous filter
        $rows = $db->table('users')
            ->where('name', 'Ann')
            ->orGroupStart()
                ->where('age >=', 40)
                ->where('active', true)
            ->orGroupEnd()
            ->orderBy('email','ASC')
            ->get()->getResultArray();
        $this->assertSame(['x1@ex.com','x4@ex.com'], array_column($rows, 'email'));

        // notGroupStart act like NOR of the inner terms; here it equals name != 'Eve'
        $norRows = $db->table('users')
            ->notGroupStart()
                ->where('name', 'Eve')
            ->groupEnd()
            ->orderBy('email','ASC')
            ->get()->getResultArray();
        $neqRows = $db->table('users')->whereNot('name','Eve')->orderBy('email','ASC')->get()->getResultArray();
        $this->assertSame(array_column($neqRows,'email'), array_column($norRows,'email'));

        // orWhereIn
        $rows = $db->table('users')
            ->where('active', false)
            ->orWhereIn('name', ['Dan'])
            ->orderBy('email','ASC')
            ->get()->getResultArray();
        $this->assertSame(['x3@ex.com','x4@ex.com','x5@ex.com','x6@ex.com'], array_column($rows,'email'));

        // orWhereNotIn
        $rows = $db->table('users')
            ->where('name', 'Ann')
            ->orWhereNotIn('email', ['x2@ex.com','x3@ex.com'])
            ->orderBy('email','ASC')
            ->get()->getResultArray();
        $this->assertSame(['x1@ex.com','x4@ex.com','x5@ex.com','x6@ex.com'], array_column($rows,'email'));

        // like across multiple fields (OR inside)
        $rows = $db->table('users')->like(['name','email'], 'x3@ex.com', 'both', null, true)->get()->getResultArray();
        $this->assertSame(['x3@ex.com'], array_column($rows,'email'));

        // notLike with side and insensitive option (exclude names ending with "n")
        $rows = $db->table('users')->notLike('name', 'n', 'before', null, true)->orderBy('email','ASC')->get()->getResultArray();
        $this->assertSame(['x2@ex.com','x3@ex.com','x5@ex.com'], array_column($rows,'email'));

        // orLike
        $rows = $db->table('users')
            ->where('age <', 10)
            ->orLike('name', 'Cat', 'both', null, false)
            ->orderBy('email','ASC')
            ->get()->getResultArray();
        $this->assertSame(['x3@ex.com','x5@ex.com'], array_column($rows,'email'));

        // orNotLike (email not containing 'x6' OR name === 'Ann' => all except x6)
        $rows = $db->table('users')
            ->where('name', 'Ann')
            ->orNotLike('email', 'x6', 'both', null, false)
            ->orderBy('email','ASC')
            ->get()->getResultArray();
        $this->assertSame(['x1@ex.com','x2@ex.com','x3@ex.com','x4@ex.com','x5@ex.com'], array_column($rows,'email'));
    }
}
