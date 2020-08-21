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

	public function getAvailableDrivers() {
		return [
			'mongodb'
		];
	}

	public function connect($dbType, $dbName, $serverName, $port, $user, $password, array $options) {
		$this->dbName = $dbName;
		$this->dbInstance = new \MongoDB\Driver\Manager("mongodb://$serverName:$port", [
			'username' => $user,
			'password' => $password
		], $options);
	}

	public function query(string $collectionName, array $criteres = []) {
		$query = new \MongoDB\Driver\Query($criteres);
		return $this->dbInstance->executeQuery($this->dbName . "." . $collectionName, $query);
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
}

