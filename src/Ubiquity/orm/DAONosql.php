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
	 * @param boolean|null $useCache
	 * @return object the instance loaded or null if not found
	 */
	public static function getById($className, $keyValues, $useCache = NULL) {
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
		return static::_getOne(self::getDatabase(self::$modelsDatabase[$className] ?? 'default'), $className, $params, $useCache);
	}

	/**
	 * Returns an instance of $className from the database, from $keyvalues values of the primary key or with a condition
	 *
	 * @param string $className
	 *        	class name of the model to load
	 * @param array|null $parameters
	 * @param boolean $useCache
	 *        	use the active cache if true
	 * @return array
	 */
	public static function getOne($className, $parameters = [], $useCache = NULL) {
		$db = self::getDb($className);
		return static::_getOne($db, $className, $parameters, $useCache);
	}

	/**
	 * Returns an array of $className objects from the database
	 *
	 * @param string $className
	 *        	class name of the model to load
	 * @param array|null $parameters
	 * @param boolean $useCache
	 *        	use the active cache if true
	 * @return array
	 */
	public static function getAll($className, $parameters = [], $useCache = NULL) {
		$db = self::getDb($className);
		return static::_getAll($db, $className, $parameters, $useCache);
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

	protected static function _getOne(DatabaseNosql $db, $className, $params, $useCache) {
		$object = null;

		$metaDatas = OrmUtils::getModelMetadata($className);
		$tableName = $metaDatas['#tableName'];
		$transformers = $metaDatas['#transformers'][self::$transformerOp] ?? [];
		$doc = $db->queryOne($tableName, $params);
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
	 * @return array
	 */
	protected static function _getAll(DatabaseNosql $db, $className, $params = [], $useCache = NULL) {
		$objects = array();

		$metaDatas = OrmUtils::getModelMetadata($className);
		$tableName = $metaDatas['#tableName'];

		$transformers = $metaDatas['#transformers'][self::$transformerOp] ?? [];
		$query = $db->query($tableName, $params);
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

