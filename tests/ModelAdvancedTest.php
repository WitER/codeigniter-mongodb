<?php
declare(strict_types=1);

namespace Tests;

use Tests\Support\Models\UserModel;

final class ModelAdvancedTest extends BaseMongoTestCase
{
    public function testAllowedFieldsAndValidationAndTimestamps(): void
    {
        $model = new UserModel();
        // clean
        $model->where([])->delete();

        // Set validation rules
        $model->setValidationRules([
            'email' => 'required|valid_email',
            'name'  => 'required|string',
            'age'   => 'required|integer',
            'active' => 'required|integer',
        ]);

        // Invalid insert
        $this->assertFalse($model->insert(['email' => 'bad', 'name' => 'X', 'age' => 'nope'], true));
        $this->assertNotEmpty($model->errors());

        // Valid insert with extra fields (should be filtered out by allowedFields)
        $id = $model->insert([
            'email' => 'val@example.com',
            'name'  => 'Valid',
            'age'   => 33,
            'active' => 0,
            'unknown_field' => 'should be ignored',
        ], true);
        $this->assertNotEmpty((string) $id);

        $row = $model->find($id);
        $this->assertNotNull($row->created_at);
        $this->assertNotNull($row->updated_at);
        $this->assertNull($row->unknown_field);

        // save() as upsert for update
        sleep(5);
        $row->name = 'Valid 2';
        $model->save($row);
        $row2 = $model->find($id);
        $this->assertSame('Valid 2', $row2->name);
        $this->assertNotEquals((string)$row->updated_at, (string)$row2->updated_at);

        // Batch insert/update via builder-level methods from model
        $batch = [];
        for ($i=0; $i<3; $i++) {
            $batch[] = ['email' => "m$i@example.com", 'name' => "M$i", 'age' => 20+$i, 'active' => $i % 2];
        }
        $this->assertEquals(3, $model->builder()->insertBatch($batch));

        // findAll with limit/offset
        $all = $model->orderBy('email','ASC')->findAll(2, 1);
        $this->assertCount(2, $all);

        // whereIn by emails
        $subset = $model->whereIn('email', ['m0@example.com','m2@example.com'])->findAll();
        $this->assertCount(2, $subset);

        // countAllResults from model builder
        $cnt = $model->builder()->countAllResults();
        $this->assertGreaterThanOrEqual(4, $cnt);
    }
}
