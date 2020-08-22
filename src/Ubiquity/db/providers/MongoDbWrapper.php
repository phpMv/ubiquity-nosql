<?php
namespace Ubiquity\db\providers;

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

	protected $dbName;

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

	public function query(string $collectionName, array $criteres = []) {
		$query = new \MongoDB\Driver\Query($criteres);
		return $this->dbInstance->executeQuery($this->dbName . "." . $collectionName, $query);
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
		$bulk->update($filter, $newValues, $options);
		return $this->dbInstance->executeBulkWrite($collectionName, $bulk);
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

	public function getRowNum(string $collectionName, string $field, $value): int {
		return $this->count($collectionName, [
			$field => [
				'$lt' => $value
			]
		]);
	}
}

