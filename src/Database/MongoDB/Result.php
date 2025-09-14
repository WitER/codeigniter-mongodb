<?php

declare(strict_types=1);

namespace CodeIgniter\Database\MongoDB;

use CodeIgniter\Database\BaseResult;
use CodeIgniter\Entity\Entity;
use MongoDB\Driver\Cursor as DriverCursor;
use MongoDB\Model\CodecCursor as CodecCursor;
use stdClass;

/**
 * Result wrapper for MongoDB find() operations
 */
class Result extends BaseResult
{
    /** @var array<int, array> */
    private array $buffer = [];

    /** @var int */
    private int $pointer = 0;

    public function __construct(&$connID, &$resultID)
    {
        parent::__construct($connID, $resultID);

        // Buffer all results into arrays for BaseResult consumption, preserving BSON types
        $iter = [];
        if ($resultID instanceof CodecCursor || $resultID instanceof DriverCursor || is_iterable($resultID)) {
            foreach ($resultID as $doc) {
                $row = $this->toArrayPreserveTypes($doc);

                // Совместимость с CI: если есть _id, но нет id — добавим алиас id
                if (is_array($row) && array_key_exists('_id', $row) && ! array_key_exists('id', $row)) {
                    $row['id'] = $this->stringifyIdForAlias($row['_id']);
                }
                // Приводим _id к строке, если это ObjectId или ExtendedJSON-массив с 'oid'
                if (is_array($row) && array_key_exists('_id', $row)) {
                    if ($row['_id'] instanceof \MongoDB\BSON\ObjectId) {
                        $row['_id'] = (string) $row['_id'];
                    } elseif (is_array($row['_id']) && array_key_exists('oid', $row['_id'])) {
                        $row['_id'] = (string) $row['_id']['oid'];
                    }
                }

                $iter[] = $row;
            }
        }
        $this->buffer = $iter;
    }


    /**
     * Преобразовать значение _id в строку для алиаса id (если это ObjectId), иначе вернуть как есть.
     *
     * @param mixed $val
     * @return mixed
     */
    private function stringifyIdForAlias($val)
    {
        if (is_array($val) && array_key_exists('oid', $val)) {
            $val = (string) $val['oid'];
        }
        if ($val instanceof \MongoDB\BSON\ObjectId) {
            return (string) $val;
        }
        // Если _id уже строка/число — оставляем, для совместимости с CI
        if (is_string($val) || is_int($val) || is_float($val)) {
            return $val;
        }
        // Иное (сложный тип _id) — ничего не конвертируем
        return $val;
    }


    /**
     * Recursively converts documents to associative arrays while preserving BSON types.
     *
     * @param mixed $value
     * @return mixed
     */
    private function toArrayPreserveTypes($value)
    {
        // Специальные BSON-типы
        if ($value instanceof \MongoDB\BSON\UTCDateTime) {
            // Вернём CodeIgniter\I18n\Time (UTC), у него есть __toString()
            $dt = $value->toDateTime();
            $ts = $dt->getTimestamp();
            return \CodeIgniter\I18n\Time::createFromTimestamp($ts, 'UTC');
        }
        if ($value instanceof \MongoDB\BSON\ObjectId) {
            // Строковое представление
            return (string) $value;
        }
        if ($value instanceof \MongoDB\Model\BSONDocument || $value instanceof \MongoDB\Model\BSONArray) {
            // Рекурсивно развернём содержимое документа/массива
            $out = [];
            foreach ($value as $k => $v) {
                $out[$k] = $this->toArrayPreserveTypes($v);
            }
            return $out;
        }

        if (is_array($value)) {
            $out = [];
            foreach ($value as $k => $v) {
                $out[$k] = $this->toArrayPreserveTypes($v);
            }
            return $out;
        }

        if ($value instanceof \Traversable) {
            $out = [];
            foreach ($value as $k => $v) {
                $out[$k] = $this->toArrayPreserveTypes($v);
            }
            return $out;
        }

        if (is_object($value)) {
            // Не раскрываем внутренности BSON-типов, которые не перечислены выше, отдаём как есть
            $vars = get_object_vars($value);
            if ($vars !== []) {
                $out = [];
                foreach ($vars as $k => $v) {
                    $out[$k] = $this->toArrayPreserveTypes($v);
                }
                return $out;
            }
            return $value;
        }

        return $value;
    }


    public function getFieldCount(): int
    {
        if ($this->buffer === []) {
            return 0;
        }
        $names = [];
        foreach ($this->buffer as $row) {
            foreach ($row as $k => $_) {
                $names[$k] = true;
            }
        }
        return count($names);
    }

    public function getFieldNames(): array
    {
        if ($this->buffer === []) {
            return [];
        }
        $names = [];
        foreach ($this->buffer as $row) {
            foreach ($row as $k => $_) {
                $names[$k] = true;
            }
        }
        return array_keys($names);
    }

    public function getFieldData(): array
    {
        $fields = [];
        $names = $this->getFieldNames();
        foreach ($names as $name) {
            $nullable = false;
            foreach ($this->buffer as $row) {
                if (!array_key_exists($name, $row) || $row[$name] === null) {
                    $nullable = true;
                    break;
                }
            }
            $field = new stdClass();
            $field->name = $name;
            $field->type = 'mixed';
            $field->max_length = null;
            $field->nullable = $nullable;
            $fields[] = $field;
        }
        return $fields;
    }

    public function freeResult()
    {
        $this->buffer = [];
        $this->pointer = 0;
    }

    public function dataSeek(int $n = 0)
    {
        if ($n < 0 || $n >= count($this->buffer)) {
            return false;
        }
        $this->pointer = $n;
        return true;
    }

    protected function fetchAssoc()
    {
        if ($this->pointer >= count($this->buffer)) {
            return null;
        }
        return $this->buffer[$this->pointer++];
    }

    protected function fetchObject(string $className = stdClass::class)
    {
        $row = $this->fetchAssoc();
        if ($row === null) {
            return false;
        }

        // Гарантируем алиас id для любых объектных результатов
        if (is_array($row) && array_key_exists('_id', $row) && ! array_key_exists('id', $row)) {
            $row['id'] = $this->stringifyIdForAlias($row['_id']);
            if (is_array($row['_id']) && array_key_exists('oid', $row['_id'])) {
                $row['_id'] = (string) $row['_id']['oid'];
            }
        }


        if (is_subclass_of($className, Entity::class)) {
            return empty($row) ? false : (new $className())->injectRawData($row);
        }

        if ($className === stdClass::class) {
            return (object) $row;
        }
        $obj = new $className();
        foreach ($row as $k => $v) {
            $obj->{$k} = $v;
        }
        // Если у кастомного класса всё ещё нет id, но есть _id — добавим строковый алиас
        if (!property_exists($obj, 'id') && isset($row['_id'])) {
            $obj->id = $this->stringifyIdForAlias($row['_id']);
            if (is_array($row['_id']) && array_key_exists('oid', $row['_id'])) {
                $obj->_id = (string) $row['_id']['oid'];
            }
        }

        return $obj;
    }

    /**
     * Количество строк в результате.
     */
    public function getNumRows(): int
    {
        return count($this->buffer);
    }

    /**
     * Все строки в виде массива массивов.
     */
    public function getResultArray(): array
    {
        // Гарантируем чтение с начала
        $this->dataSeek(0);
        $out = [];
        while (($row = $this->fetchAssoc()) !== null) {
            $out[] = $row;
        }
        // Восстанавливаем указатель в начало для повторяемости чтения
        $this->dataSeek(0);
        return $out;
    }

    /**
     * Все строки в виде массива stdClass.
     */
    public function getResultObject(): array
    {
        // Гарантируем чтение с начала
        $this->dataSeek(0);
        $out = [];
        while (($row = $this->fetchAssoc()) !== null) {
            $out[] = (object) $row;
        }
        $this->dataSeek(0);
        return $out;
    }

    /**
     * Все строки в виде массива объектов заданного класса.
     */
    public function customResultObject(string $className): array
    {
        $this->dataSeek(0);
        $out = [];
        while (true) {
            $obj = $this->fetchObject($className);
            if ($obj === false) {
                break;
            }
            $out[] = $obj;
        }
        $this->dataSeek(0);
        return $out;
    }

    /**
     * Получить одну строку в виде массива по индексу (по умолчанию первая).
     */
    public function getRowArray(int $n = 0): ?array
    {
        $old = $this->pointer;
        if (! $this->dataSeek($n)) {
            return null;
        }
        $row = $this->fetchAssoc();
        // Restore original pointer
        $this->pointer = $old;
        return $row ?? null;
    }

    /**
     * Wrapper object to return a row as either an array, an object, or
     * a custom class.
     *
     * If the row doesn't exist, returns null.
     *
     * @template T of object
     *
     * @param int|string                       $n    The index of the results to return, or column name.
     * @param 'array'|'object'|class-string<T> $type The type of result object. 'array', 'object' or class name.
     *
     * @return ($n is string ? float|int|string|null : ($type is 'object' ? stdClass|null : ($type is 'array' ? array|null : T|null)))
     */
    public function getRow($n = 0, string $type = 'object')
    {
        // When $n is a column name, return scalar from current (or first) row without moving the pointer
        if (is_string($n)) {
            if ($this->buffer === []) {
                return null;
            }
            $idx = ($this->pointer < count($this->buffer)) ? $this->pointer : 0;
            $row = $this->buffer[$idx] ?? null;
            if ($row === null) {
                return null;
            }
            return $row[$n] ?? null;
        }

        $index = (int) $n;
        $old = $this->pointer;
        if (! $this->dataSeek($index)) {
            return null;
        }

        if ($type === 'array') {
            $row = $this->fetchAssoc();
            $this->pointer = $old;
            return $row ?? null;
        }

        if ($type === 'object') {
            $row = $this->fetchAssoc();
            $this->pointer = $old;
            return $row === null ? null : (object) $row;
        }

        // Otherwise assume it's a class name
        $obj = $this->fetchObject($type);
        $this->pointer = $old;
        return $obj === false ? null : $obj;
    }

    /**
     * Эмулирует «небуферизованное» чтение — выдаёт следующую строку и продвигает указатель.
     * $type: 'object' | 'array'
     */
    public function getUnbufferedRow(?string $type = 'object')
    {
        $row = $this->fetchAssoc();
        if ($row === null) {
            return null;
        }
        if ($type === 'array') {
            return $row;
        }
        return (object) $row;
    }

}
