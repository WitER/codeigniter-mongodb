<?php
declare(strict_types=1);

namespace CodeIgniter\Database\MongoDB;

use CodeIgniter\Database\BasePreparedQuery;
use CodeIgniter\Database\Exceptions\DatabaseException;

/**
 * PreparedQuery is not applicable to MongoDB in this driver.
 * Provided to satisfy BaseConnection::prepare() lookup.
 */
class PreparedQuery extends BasePreparedQuery
{
    public function _prepare(string $sql, array $options = []): static
    {

        // Если включён режим отладки БД — бросаем исключение, чтобы сразу увидеть проблему
        if ($this->db && $this->db->DBDebug) {
            throw new DatabaseException('MongoDB driver does not support prepared SQL queries.');
        }

        // В противном случае «мягкая» заглушка: просто возвращаем self без подготовки
        return $this;

    }

    public function _execute(array $data): bool
    {
        // Если включён режим отладки БД — бросаем исключение
        if ($this->db && $this->db->DBDebug) {
            throw new DatabaseException('MongoDB driver does not support prepared SQL queries.');
        }

        // В продакшене возвращаем неуспех вместо исключения
        return false;

    }

    public function _getResult()
    {
        return false;
    }

    protected function _close(): bool
    {
        return true;
    }
}
