<?php
namespace Ubiquity\orm;

use Ubiquity\controllers\Startup;
use Ubiquity\db\DatabaseNosql;
use Ubiquity\events\DAOEvents;
use Ubiquity\events\EventsManager;
use Ubiquity\exceptions\DAOException;
use Ubiquity\log\Logger;
use Ubiquity\orm\parser\Reflexion;
use Ubiquity\orm\traits\DAOCommonTrait;
use Ubiquity\orm\traits\DAOPooling;

class DAONosql {
	use DAOCommonTrait,DAOPooling;

	protected static $bulkDbs = [];

	/**
	 * Establishes the connection to the database using the $config array
	 *
	 * @param array $config
	 *        	the config array (Startup::getConfig())
	 */
	public static function startDatabase(&$config, $offset = null) {
		$db = $offset ? ($config['database'][$offset] ?? ($config['database'] ?? [])) : ($config['database']['default'] ?? $config['database']);
		if ($db['dbName'] !== '') {
			self::connect($offset ?? 'default', $db['wrapper'] ?? \Ubiquity\db\providers\MongoDbWrapper::class, $db['type'], $db['dbName'], $db['serverName'] ?? '127.0.0.1', $db['port'] ?? 27017, $db['user'] ?? '', $db['password'] ?? '', $db['options'] ?? [], $db['cache'] ?? false);
		}
	}

	/**
	 * Establishes the connection to the database using the past parameters
	 *
	 * @param string $offset
	 * @param string $wrapper
	 * @param string $dbType
	 * @param string $dbName
	 * @param string $serverName
	 * @param string $port
	 * @param string $user
	 * @param string $password
	 * @param array $options
	 * @param boolean $cache
	 */
	public static function connect($offset, $wrapper, $dbType, $dbName, $serverName = '127.0.0.1', $port = '27017', $user = '', $password = '', $options = [], $cache = false) {
		self::$db[$offset] = new DatabaseNosql($wrapper, $dbType, $dbName, $serverName, $port, $user, $password, $options, $cache, self::$pool);
		try {
			self::$db[$offset]->connect();
		} catch (\Exception $e) {
			Logger::error("DAO", $e->getMessage());
			throw new DAOException($e->getMessage(), $e->getCode(), $e->getPrevious());
		}
	}

	/**
	 * Returns the database instance defined at $offset key in config
	 *
	 * @param string $offset
	 * @return \Ubiquity\db\Database
	 */
	public static function getDatabase($offset = 'default') {
		if (! isset(self::$db[$offset])) {
			self::startDatabase(Startup::$config, $offset);
		}
		return self::$db[$offset];
	}

	/**
	 * Returns an instance of $className from the database, from $keyvalues values of the primary key
	 *
	 * @param String $className
	 *        	complete classname of the model to load
	 * @param Array|string $keyValues
	 *        	primary key values or condition
	 * @param array $options
	 *        	The db query options
	 * @param boolean|null $useCache
	 * @return object the instance loaded or null if not found
	 */
	public static function getById($className, $keyValues, $options = [], $useCache = NULL) {
		if (! \is_array($keyValues)) {
			$keyValues = [
				$keyValues
			];
		}
		$params = $keyValues;
		if ((\array_keys($keyValues) === \range(0, \count($keyValues) - 1))) {
			$pks = OrmUtils::getAnnotationInfo($className, "#primaryKeys");
			foreach ($pks as $index => $pk) {
				$params[$pk] = $keyValues[$index];
			}
		}
		return static::_getOne(self::getDatabase(self::$modelsDatabase[$className] ?? 'default'), $className, $params, $options, $useCache);
	}

	/**
	 * Returns an instance of $className from the database, from $keyvalues values of the primary key or with a condition
	 *
	 * @param string $className
	 *        	class name of the model to load
	 * @param array|null $parameters
	 * @param array $options
	 *        	The db query options
	 * @param boolean $useCache
	 *        	use the active cache if true
	 * @return array
	 */
	public static function getOne($className, $parameters = [], $options = [], $useCache = NULL) {
		$db = self::getDb($className);
		return static::_getOne($db, $className, $parameters, $options, $useCache);
	}

	/**
	 * Returns an array of $className objects from the database
	 *
	 * @param string $className
	 *        	class name of the model to load
	 * @param array|null $parameters
	 * @param array $options
	 *        	The db query options
	 * @param boolean $useCache
	 *        	use the active cache if true
	 * @return array
	 */
	public static function getAll($className, $parameters = [], $options = [], $useCache = NULL) {
		$db = self::getDb($className);
		return static::_getAll($db, $className, $parameters, $options, $useCache);
	}

	public static function count($className, $parameters = []) {
		$db = self::getDb($className);
		$tableName = OrmUtils::getTableName($className);
		return $db->count($tableName, $parameters);
	}

	public static function paginate($className, $page = 1, $rowsPerPage = 20, $parameters = [], $useCache = NULL) {
		$db = self::getDb($className);
		$tableName = OrmUtils::getTableName($className);
		return $db->paginate($tableName, $page, $rowsPerPage, $parameters, $useCache);
	}

	protected static function _getOne(DatabaseNosql $db, $className, $params, $options, $useCache) {
		$object = null;

		$metaDatas = OrmUtils::getModelMetadata($className);
		$tableName = $metaDatas['#tableName'];
		$transformers = $metaDatas['#transformers'][self::$transformerOp] ?? [];
		$doc = $db->queryOne($tableName, $params, $options);
		if ($doc) {
			$object = self::_loadObjectFromRow($db, $doc, $className, $metaDatas['#memberNames'] ?? null, $metaDatas['#accessors'], $transformers);
			EventsManager::trigger(DAOEvents::GET_ONE, $object, $className);
		}
		return $object;
	}

	/**
	 *
	 * @param DatabaseNosql $db
	 * @param string $className
	 * @param array $params
	 * @param array $options
	 * @param bool $useCache
	 * @return array
	 */
	protected static function _getAll(DatabaseNosql $db, $className, $params = [], $options = [], $useCache = NULL) {
		$objects = array();

		$metaDatas = OrmUtils::getModelMetadata($className);
		$tableName = $metaDatas['#tableName'];

		$transformers = $metaDatas['#transformers'][self::$transformerOp] ?? [];
		$query = $db->query($tableName, $params, $options);
		$propsKeys = OrmUtils::getPropKeys($className);
		foreach ($query as $row) {
			$object = self::_loadObjectFromRow($db, $row, $className, $metaDatas['#memberNames'] ?? null, $metaDatas['#accessors'], $transformers);
			$objects[OrmUtils::getPropKeyValues($object, $propsKeys)] = $object;
		}
		EventsManager::trigger(DAOEvents::GET_ALL, $objects, $className);
		return $objects;
	}

	/**
	 *
	 * @param DatabaseNosql $db
	 * @param array $row
	 * @param string $className
	 * @param array $memberNames
	 * @param array $accessors
	 * @param array $transformers
	 * @return object
	 */
	public static function _loadObjectFromRow(DatabaseNosql $db, $row, $className, $memberNames, $accessors, $transformers) {
		$o = new $className();
		if (self::$useTransformers) {
			self::applyTransformers($transformers, $row, $memberNames);
		}
		foreach ($row as $k => $v) {
			if ($accesseur = ($accessors[$k] ?? false)) {
				$o->$accesseur($v);
				$o->_rest[$memberNames[$k] ?? $k] = $v;
			}
		}
		return $o;
	}

	/**
	 *
	 * @param array $row
	 * @param string $className
	 * @param array $memberNames
	 * @param array $transformers
	 * @return object
	 */
	public static function _loadSimpleObjectFromRow($row, $className, $memberNames, $transformers) {
		$o = new $className();
		if (self::$useTransformers) {
			self::applyTransformers($transformers, $row, $memberNames);
		}
		foreach ($row as $k => $v) {
			$m = $memberNames[$k] ?? $k;
			if (\property_exists($className, $m)) {
				$o->$m = $v;
				$o->_rest[$m] = $v;
			}
		}
		return $o;
	}

	/**
	 * Updates an existing $instance in the database.
	 * Be careful not to modify the primary key
	 *
	 * @param object $instance
	 *        	instance to modify
	 */
	public static function update($instance) {
		EventsManager::trigger('dao.before.update', $instance);
		$className = \get_class($instance);
		$db = self::getDb($className);
		$tableName = OrmUtils::getTableName($className);
		$ColumnskeyAndValues = Reflexion::getPropertiesAndValues($instance, NULL, true);
		$keyFieldsAndValues = OrmUtils::getKeyFieldsAndValues($instance);
		if (Logger::isActive()) {
			Logger::info("DAOUpdates", \json_encode($keyFieldsAndValues), "update");
			Logger::info("DAOUpdates", \json_encode($ColumnskeyAndValues), "Key and values");
		}
		try {
			$result = $db->update($tableName, $keyFieldsAndValues, $ColumnskeyAndValues);
			$instance->_rest = \array_merge($instance->_rest, $ColumnskeyAndValues);
			EventsManager::trigger(DAOEvents::AFTER_UPDATE, $instance, $result);
			return $result;
		} catch (\Exception $e) {
			Logger::warn("DAOUpdates", $e->getMessage(), "update");
		}
		return false;
	}

	/**
	 * Inserts a new instance $instance into the database
	 *
	 * @param object $instance
	 *        	the instance to insert
	 */
	public static function insert($instance) {
		EventsManager::trigger('dao.before.insert', $instance);
		$className = \get_class($instance);
		$db = self::getDb($className);
		$tableName = OrmUtils::getTableName($className);
		$keyAndValues = Reflexion::getPropertiesAndValues($instance);
		$pk = OrmUtils::getFirstKey($className);
		$pkVal = $keyAndValues[$pk] ?? null;
		if (($pkVal) == null) {
			unset($keyAndValues[$pk]);
		}
		if (Logger::isActive()) {
			Logger::info('DAOUpdates', \json_encode($keyFieldsAndValues), 'insert');
			Logger::info('DAOUpdates', \json_encode($keyAndValues), 'Key and values');
		}

		try {
			$result = $db->insert($tableName, $keyAndValues);
			if ($result) {
				if ($pkVal == null) {
					$propKey = OrmUtils::getFirstPropKey($className);
					$propKey->setValue($instance, $result);
					$pkVal = $result;
				}
				$instance->_rest = $keyAndValues;
				$instance->_rest[$pk] = $pkVal;
			}
			EventsManager::trigger(DAOEvents::AFTER_INSERT, $instance, $result);
			return $result;
		} catch (\Exception $e) {
			Logger::warn('DAOUpdates', $e->getMessage(), 'insert');
			if (Startup::$config['debug']) {
				throw $e;
			}
		}
		return false;
	}

	/**
	 *
	 * @param object $instance
	 * @param boolean $updateMany
	 * @return boolean|int
	 */
	public static function save($instance) {
		if (isset($instance->_rest)) {
			return self::update($instance);
		}
		return self::insert($instance);
	}

	/**
	 * Deletes the object $instance from the database
	 *
	 * @param object $instance
	 *        	instance à supprimer
	 */
	public static function remove($instance): ?int {
		$className = \get_class($instance);
		$tableName = OrmUtils::getTableName($className);
		$keyAndValues = OrmUtils::getKeyFieldsAndValues($instance);
		return self::removeBy_($className, $tableName, $keyAndValues);
	}

	/**
	 * Deletes all instances from $modelName corresponding to $ids
	 *
	 * @param string $modelName
	 * @param array $filter
	 * @return int|boolean
	 */
	public static function delete($modelName, $filter) {
		return self::removeBy_($modelName, OrmUtils::getTableName($modelName), $filter);
	}

	/**
	 *
	 * @param string $className
	 * @param string $tableName
	 * @param array $keyAndValues
	 * @return int the number of rows that were deleted
	 */
	private static function removeBy_($className, $tableName, $keyAndValues): ?int {
		$db = self::getDb($className);
		Logger::info('DAOUpdates', $tableName, 'delete');
		try {
			return $db->delete($tableName, $keyAndValues);
		} catch (\PDOException $e) {
			Logger::warn('DAOUpdates', $e->getMessage(), 'delete');
			return null;
		}
		return 0;
	}

	/**
	 * Starts a bulk for insert, update or delete operation
	 *
	 * @param string $className
	 * @param array $options
	 * @return string the bulk id to use with toUpdate and flush
	 */
	public static function startBulk(string $className, array $options = []) {
		$tableName = OrmUtils::getTableName($className);
		$db = self::getDb($className);
		$bId = $db->startBulk($tableName, $options);
		self::$bulkDbs[$bId] = $db;
		return $bId;
	}

	public static function toUpdate($instance, $bulkId = null) {
		$ColumnskeyAndValues = Reflexion::getPropertiesAndValues($instance, NULL, true);
		if (\count($ColumnskeyAndValues) > 0) {
			$keyFieldsAndValues = OrmUtils::getKeyFieldsAndValues($instance);
			$instance->_rest = \array_merge($instance->_rest, $ColumnskeyAndValues);

			if (isset($bulkId)) {
				$db = self::$bulkDbs[$bulkId];
				return $db->toUpdate($bulkId, $keyFieldsAndValues, $ColumnskeyAndValues);
			}
			$className = \get_class($instance);
			$db = self::getDb($className);
			return $db->toUpdate(OrmUtils::getTableName($className), $keyFieldsAndValues, $ColumnskeyAndValues);
		}
		return false;
	}

	public static function flush($idOrClassName, bool $byId = true) {
		if ($byId) {
			$db = self::$bulkDbs[$idOrClassName];
			return $db->flush($idOrClassName, true);
		}
		$db = self::getDb($idOrClassName);
		$tableName = OrmUtils::getTableName($idOrClassName);
		return $db->flush($tableName, false);
	}
}

