<?php
namespace Ubiquity\db;

class DatabaseNosql extends AbstractDatabase {

	public function _connect() {
		$this->wrapperObject->connect($this->dbType, $this->dbName, $this->serverName, $this->port, $this->user, $this->password, $this->options);
	}

	public function query(string $collectionName, array $criteres = []) {
		return $this->wrapperObject->query($collectionName, $criteres);
	}

	public function update(string $collectionName, $filter = [], $newValues = [], $options = []) {
		return $this->wrapperObject->update($collectionName, $filter, $newValues, $options);
	}

	public function getTablesName() {
		return $this->wrapperObject->getTablesName();
	}

	public function getFieldsInfos($collectionName) {
		return $this->wrapperObject->getFieldsInfos($collectionName);
	}

	public function getPrimaryKeys($collectionName) {
		return [];
	}

	public function getForeignKeys($collectionName, $pkName, $dbName = null) {
		return [];
	}
}

