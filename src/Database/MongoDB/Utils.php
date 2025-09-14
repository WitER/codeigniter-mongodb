<?php
declare(strict_types=1);

namespace CodeIgniter\Database\MongoDB;

use CodeIgniter\Database\BaseUtils;
use CodeIgniter\Database\Exceptions\DatabaseException;

/**
 * Utils for SQLite3
 */
class Utils extends BaseUtils
{

    /**
     * Platform-dependent version of the backup function.
     *
     * @return never
     */
    public function _backup(?array $prefs = null)
    {
        throw new DatabaseException('Unsupported feature of the database platform you are using.');
    }
}
