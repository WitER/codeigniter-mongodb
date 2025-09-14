<?php
declare(strict_types=1);

namespace Tests;

use Tests\Support\Models\UserModel;
use Tests\Support\Entities\User;

final class EntityCastingTest extends BaseMongoTestCase
{
    /**
     * @throws \ReflectionException
     */
    public function testEntityCastingAndDates(): void
    {
        $model = new UserModel();
        $model->where([])->delete();

        $id = $model->insert([
            'email'  => 'cast@example.com',
            'name'   => 'Caster',
            'age'    => '27', // string input
            'active' => 1,    // int input
            'roles'  => ['a','b'],
            'meta'   => ['x' => 1],
        ], true);
        $this->assertNotEmpty((string) $id);

        /** @var User $u */
        $u = $model->find($id);
        $this->assertSame(27, $u->age);
        $this->assertTrue($u->active);
        $this->assertIsArray($u->roles);
        $this->assertIsArray($u->meta);
        $this->assertNotNull($u->created_at);
        $this->assertNotNull($u->updated_at);

        sleep(5);

        // Modify and save to update updated_at
        $oldUpdated = (string) $u->updated_at;
        $u->age = 28;
        $model->save($u);
        $u2 = $model->find($id);
        $this->assertSame(28, $u2->age);
        $this->assertNotSame($oldUpdated, (string) $u2->updated_at);
    }
}
