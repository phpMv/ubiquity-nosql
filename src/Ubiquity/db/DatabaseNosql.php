<?php
namespace Ubiquity\db;

class DatabaseNosql extends AbstractDatabase {

	public function query(string $collectionName, array $criteres = []) {
		return $this->wrapperObject->query($collectionName, $criteres);
	}

	public function update(string $collectionName, $filter = [], $newValues = [], $options = []) {
		return $this->wrapperObject->update($collectionName, $filter, $newValues, $options);
	}
}

