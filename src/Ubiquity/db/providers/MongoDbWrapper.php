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

	public function toUpdate(string $id, $filter = [], $newValues = [], $options = []) {
		$options = array_merge([
			'multi' => false,
			'upsert' => false
		], $options);
		self::$bulks[$id]['bulk']->update($filter, [
			'$set' => $newValues
		], $options);
	}

	public function startBulk($collectionName) {
		$id = \uniqid();
		self::$bulks[$id] = [
			'collection' => $collectionName,
			'bulk' => new \MongoDB\Driver\BulkWrite()
		];
		return $id;
	}

	public function flush($id) {
		return $this->dbInstance->executeBulkWrite($this->dbName . '.' . self::$bulks[$id]['collection'], self::$bulks[$id]['bulk']);
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
		$query = new \MongoDB\Driver\Query($criteres);
		return $this->dbInstance->executeQuery($this->dbName . "." . $collectionName, $query, $options);
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

