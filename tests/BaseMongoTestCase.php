<?php
declare(strict_types=1);

namespace Tests;

use \CodeIgniter\Test\CIUnitTestCase;
use CodeIgniter\Test\DatabaseTestTrait;
use Config\Database;

class BaseMongoTestCase extends CIUnitTestCase
{
    use DatabaseTestTrait;

    // Use the 'tests' DB group defined in phpunit.xml.dist
    protected $DBGroup = 'tests';

    // Run our test migrations from Tests\\Support namespace
    protected $migrate = true;
    protected $migrateOnce = true;
    protected $namespace = 'Tests\\Support';

    protected function db()
    {
        return Database::connect($this->DBGroup);
    }
}
