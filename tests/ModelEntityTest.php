<?php
declare(strict_types=1);

namespace Tests;

use Tests\BaseMongoTestCase;
use Tests\Support\Entities\User;
use Tests\Support\Models\UserModel;

final class ModelEntityTest extends BaseMongoTestCase
{
    public function testModelCrudAndEntityHydration(): void
    {
        $model = new UserModel();
        // Clean
        $model->where([])->delete();

        // Create
        $id = $model->insert([
            'email'  => 'model@example.com',
            'name'   => 'Model User',
            'age'    => 22,
            'active' => true,
            'roles'  => ['user','tester'],
            'meta'   => ['lang' => 'ru'],
        ], true);
        $this->assertNotEmpty((string) $id);

        // Read -> returns Entity
        $u = $model->where('email', 'model@example.com')->first();
        $this->assertEquals($id, $u->_id);
        $this->assertInstanceOf(User::class, $u);
        $this->assertSame('Model User', $u->name);
        $this->assertTrue($u->active);
        $this->assertIsArray($u->roles);

        // Update via save
        $u->name = 'Model User 2';
        $model->save($u);
        $u2 = $model->find($u->_id);
        $this->assertSame('Model User 2', $u2->name);

        // Delete
        $this->assertTrue($model->delete($u2->_id));
        $this->assertNull($model->find($u2->_id));
    }
}
