<?php
declare(strict_types=1);

namespace Tests\Support\Models;

use CodeIgniter\Model;
use Tests\Support\Entities\User;

class UserModel extends Model
{
    protected $DBGroup      = 'tests';
    protected $table        = 'users';
    protected $primaryKey   = '_id';
    protected $useAutoIncrement = true;
    protected $returnType   = User::class;
    protected $useSoftDeletes = false;

    protected $allowedFields = ['_id','email','name','age','active','roles','meta','created_at','updated_at'];

    protected $useTimestamps = true;
    protected $createdField  = 'created_at';
    protected $updatedField  = 'updated_at';
}
