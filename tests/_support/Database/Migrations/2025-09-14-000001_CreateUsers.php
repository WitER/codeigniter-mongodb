<?php
declare(strict_types=1);

namespace Tests\Support\Database\Migrations;

use CodeIgniter\Database\Migration;

class CreateUsers extends Migration
{
    protected $DBGroup = 'tests';

    public function up()
    {
        $this->forge->addField([
            '_id'        => ['type' => 'char', 'constraint' => 24],
            'email'      => ['type' => 'varchar', 'constraint' => 120, 'null' => false],
            'name'       => ['type' => 'varchar', 'constraint' => 60, 'null' => false],
            'age'        => ['type' => 'int', 'min' => 0, 'max' => 150, 'null' => false, 'default' => 18],
            'active'     => ['type' => 'bool', 'null' => false, 'default' => true],
            'roles'      => ['type' => 'array', 'null' => true],
            'meta'       => ['type' => 'object', 'null' => true],
            'created_at' => ['type' => 'date', 'null' => true],
            'updated_at' => ['type' => 'date', 'null' => true],
        ]);
        // unique email index
        $this->forge->addUniqueKey('email');
        // plain index for age
        $this->forge->addKey('age');
        // create collection with validator + indexes
        $this->forge->createTable('users', true);
    }

    public function down()
    {
        //$this->forge->dropTable('users', true);
    }
}
