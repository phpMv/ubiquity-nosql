<?php
namespace Ubiquity\db\providers;

use Ubiquity\utils\base\UString;

/**
 * MongoDb wrapper class.
 * Ubiquity\db\providers$MongoDbWrapper
 * This class is part of Ubiquity
 *
 * @author jcheron <myaddressmail@gmail.com>
 * @version 1.0.0
 *
 * @property \MongoDB\Driver\Manager $dbInstance
 *
 */
class MongoDbWrapper extends AbstractDbNosqlWrapper {

	protected static $bulks = [];

	protected $dbName;

	protected function getBulk($collectionName) {
		return self::$bulks[$collectionName] ??= [
			'collection' => $collectionName,
			'bulk' => new \MongoDB\Driver\BulkWrite([
				'ordered' => false
			])
		];
	}

	public function startBulk(string $collectionName, array $options = []) {
		$options = \array_merge([
			'ordered' => false
		], $options);
		$id = \uniqid();
		self::$bulks[$id] = [
			'collection' => $collectionName,
			'bulk' => new \MongoDB\Driver\BulkWrite($options)
		];
		return $id;
	}

	public function toUpdate(string $idOrCollectionName, array $filter = [], array $newValues = [], array $options = []) {
		if (\count($newValues) > 0) {
			$options = array_merge([
				'multi' => false,
				'upsert' => false
			], $options);

			self::getBulk($idOrCollectionName)['bulk']->update($filter, [
				'$set' => $newValues
			], $options);
		}
	}

	public function flush(string $idOrCollectionName, bool $byId = true) {
		$bulk = self::$bulks[$idOrCollectionName];
		if ($byId) {
			$collectionName = $bulk['collection'];
		} else {
			$collectionName = $idOrCollectionName;
		}
		$result = $this->dbInstance->executeBulkWrite($this->dbName . '.' . $collectionName, $bulk['bulk']);
		unset(self::$bulks[$idOrCollectionName]);
		return $result;
	}

	public function getDSN($serverName, $port, $dbName, $dbType = '') {
		return "mongodb://$serverName:$port";
	}

	public function ping() {
		$command = new \MongoDB\Driver\Command([
			'ping' => 1
		]);
		try {
			$cursor = $this->dbInstance->executeCommand($this->dbName, $command);
			$response = $cursor->toArray()[0];
			return $response['ok'] == 1;
		} catch (\MongoDB\Driver\Exception $e) {
			return false;
		}
	}

	public static function getAvailableDrivers() {
		return [
			'mongodb'
		];
	}

	public function connect($dbType, $dbName, $serverName, $port, $user, $password, array $options) {
		$this->dbName = $dbName;
		$auth = [];
		if ($user != '') {
			$auth = [
				'username' => $user,
				'password' => $password
			];
		}
		$this->dbInstance = new \MongoDB\Driver\Manager("mongodb://$serverName:$port", $auth, $options);
	}

	public function query(string $collectionName, array $criteres = [], array $options = []) {
		$query = new \MongoDB\Driver\Query($criteres, $options);
		return $this->dbInstance->executeQuery($this->dbName . "." . $collectionName, $query);
	}

	public function queryOne(string $collectionName, array $criteres = [], array $options = []) {
		$query = new \MongoDB\Driver\Query($criteres, [
			'limit' => 1
		] + $options);
		$cursor = $this->dbInstance->executeQuery($this->dbName . "." . $collectionName, $query);
		return \current($cursor->toArray());
	}

	public function getIndexes(string $collectionName, $unique = true) {
		$command = new \MongoDB\Driver\Command([
			'getIndexes' => $collectionName
		]);

		$cursor = $this->dbInstance->executeCommand($this->dbName, $command);
		return $cursor->toArray();
	}

	public function count(string $collectionName, array $criteres = []) {
		$command = new \MongoDB\Driver\Command([
			'count' => $collectionName,
			'query' => $criteres
		]);

		$cursor = $this->dbInstance->executeCommand($this->dbName, $command);
		return $cursor->toArray()[0]->n;
	}

	public function update(string $collectionName, $filter = [], $newValues = [], $options = []) {
		$options = array_merge([
			'multi' => false,
			'upsert' => false
		], $options);
		$bulk = new \MongoDB\Driver\BulkWrite();
		$bulk->update($filter, [
			'$set' => $newValues
		], $options);
		return $this->dbInstance->executeBulkWrite($this->dbName . '.' . $collectionName, $bulk);
	}

	public function upsert(string $collectionName, $filter = [], $newValues = [], $options = []) {
		return $this->update($collectionName, $filter, $newValues, [
			'upsert' => true
		] + $options);
	}

	public function insert(string $collectionName, $values = []) {
		$bulk = new \MongoDB\Driver\BulkWrite();
		$bulk->insert($values);
		return $this->dbInstance->executeBulkWrite($this->dbName . '.' . $collectionName, $bulk);
	}

	public function delete(string $collectionName, $filter = [], $options = []) {
		$bulk = new \MongoDB\Driver\BulkWrite();
		$bulk->delete($filter, $options);
		return $this->dbInstance->executeBulkWrite($this->dbName . '.' . $collectionName, $bulk);
	}

	public function getTablesName() {
		$listdatabases = new \MongoDB\Driver\Command([
			"listCollections" => 1
		]);
		$collections = $this->dbInstance->executeCommand($this->dbName, $listdatabases);
		$res = [];
		foreach ($collections as $collection) {
			if (\substr($collection->name, 0, 1) !== '_') {
				$res[] = $collection->name;
			}
		}
		return $res;
	}

	public function getFieldsInfos(string $collectionName) {
		$query = $this->query($collectionName)->toArray();
		$res = [];
		if (\count($query) > 0) {
			$row = \current($query);
			foreach ($row as $field => $value) {
				$res[$field] = [
					'Type' => gettype($value),
					'Nullable' => true
				];
			}
		}
		return $res;
	}

	public function getPrimaryKeys($collectionName) {
		$query = $this->query($collectionName)->toArray();
		$res = [];
		if (\count($query) > 0) {
			$row = \current($query);
			foreach ($row as $field => $value) {
				if (\substr($field, 0, 3) === '_id') {
					return [
						'_id'
					];
				}
				if (\substr($field, 0, 2) === 'id') {
					$res[] = $field;
				}
			}
		}
		return $res;
	}

	public function getForeignKeys($collectionName, $pkName, $dbName) {
		$collectionNames = $this->getTablesName();
		foreach ($collectionNames as $collection) {
			$query = $this->query($collection)->toArray();
			$res = [];
			if (\count($query) > 0) {
				$row = \current($query);
				$fk = $collectionName . \ucfirst($pkName);
				$fkReverse = $pkName . \ucfirst($collectionName);
				foreach ($row as $field => $value) {
					if ($field === $fk || $field === $fkReverse) {
						$res[] = [
							'TABLE_NAME' => $collection,
							'COLUMN_NAME' => $field
						];
					}
				}
			}
		}
		return $res;
	}

	public function getRowNum(string $collectionName, string $field, $value): int {
		return $this->count($collectionName, [
			$field => [
				'$lt' => $value
			]
		]);
	}

	public function paginate(string $collectionName, int $page = 1, int $rowsPerPage = 20, array $criteres = []) {
		return $this->query($collectionName, $criteres, [
			'skip' => (($page - 1) * $rowsPerPage),
			'limit' => $rowsPerPage
		]);
	}
}

