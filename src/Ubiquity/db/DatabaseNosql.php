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

	public function count(string $collectionName, array $criteres = []) {
		return $this->wrapperObject->count($collectionName, $criteres);
	}

	public function getTablesName() {
		return $this->wrapperObject->getTablesName();
	}

	public function getFieldsInfos($collectionName) {
		return $this->wrapperObject->getFieldsInfos($collectionName);
	}

	public function getPrimaryKeys($collectionName) {
		return $this->wrapperObject->getPrimaryKeys($collectionName);
	}

	public function getForeignKeys($collectionName, $pkName, $dbName = null) {
		return $this->wrapperObject->getForeignKeys($collectionName, $pkName, $dbName);
	}

	public function startBulk(string $collectionName) {
		return $this->wrapperObject->startBulk($collectionName);
	}

	public function toUpdate(string $id, $filter = [], $newValues = [], $options = []) {
		return $this->wrapperObject->toUpdate($id, $filter, $newValues, $options);
	}

	public function flush(string $id) {
		return $this->wrapperObject->flush($id);
	}
}

