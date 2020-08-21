<?php
namespace Ubiquity\db\providers;

abstract class AbstractDbNosqlWrapper extends AbstractDbWrapper_ {

	public static $databaseClass = '\\Ubiquity\\db\\DatabaseNosql';

	abstract public function query(string $collectionName, array $criteres = []);

	abstract public function update(string $collectionName, $filter = [], $newValues = [], $options = []);

	abstract public function getTablesName();

	abstract public function getFieldsInfos($collectionName);
}

