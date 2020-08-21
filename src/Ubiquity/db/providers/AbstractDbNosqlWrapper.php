<?php
namespace Ubiquity\db\providers;

abstract class AbstractDbNosqlWrapper extends AbstractDbWrapper_ {

	abstract public function query(string $collectionName, array $criteres = []);

	abstract public function update(string $collectionName, $filter = [], $newValues = [], $options = []);

	abstract public function getTablesName();

	abstract public function getFieldsInfos($collectionName);
}

