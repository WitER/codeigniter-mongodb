<?php
declare(strict_types=1);

namespace Tests\Support\Entities;

use CodeIgniter\Entity\Entity;

class User extends Entity
{
    protected $dates = ['created_at', 'updated_at'];
    protected $casts = [
        'age'    => 'integer',
        'active' => 'boolean',
        'roles'  => 'array',
        'meta'   => 'array',
    ];
}
