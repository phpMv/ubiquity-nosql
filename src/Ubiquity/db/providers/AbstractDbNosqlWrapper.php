<?php
namespace Ubiquity\db\providers;

abstract class AbstractDbNosqlWrapper extends AbstractDbWrapper_ {

	public static $databaseClass = '\\Ubiquity\\db\\DatabaseNosql';

	abstract public function query(string $collectionName, array $criteres = [], array $options = []);

	abstract public function count(string $collectionName, array $criteres = []);

	abstract public function update(string $collectionName, $filter = [], $newValues = [], $options = []);

	abstract public function getTablesName();

	abstract public function getFieldsInfos(string $collectionName);

	abstract public function getRowNum(string $collectionName, string $field, $value): int;

	abstract public function paginate(string $collectionName, int $page = 1, int $rowsPerPage = 20, array $criteres = []);
}

