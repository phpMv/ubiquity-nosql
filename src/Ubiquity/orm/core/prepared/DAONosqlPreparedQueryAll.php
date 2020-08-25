<?php
namespace Ubiquity\orm\core\prepared;

use Ubiquity\events\DAOEvents;
use Ubiquity\events\EventsManager;
use Ubiquity\orm\DAONosql;
use Ubiquity\orm\OrmUtils;

class DAONosqlPreparedQueryAll extends DAONosqlPreparedQuery {

	protected $propsKeys;

	public function __construct($className, $cache = null) {
		parent::__construct($className, $cache);
		$this->propsKeys = OrmUtils::getPropKeys($className);
		$this->prepare();
	}

	public function execute($params = [], $useCache = false) {
		$objects = array();
		$query = $this->db->query($this->collectionName, $params);
		foreach ($query as $row) {
			if ($this->allPublic) {
				$object = DAONosql::_loadSimpleObjectFromRow($row, $this->className, $this->memberNames, $this->transformers);
			} else {
				$object = DAONosql::_loadObjectFromRow($this->db, $row, $this->className, $this->memberNames, $this->accessors, $this->transformers);
			}
			$objects[OrmUtils::getPropKeyValues($object, $this->propsKeys)] = $object;
		}
		EventsManager::trigger(DAOEvents::GET_ALL, $objects, $className);
		return $objects;
	}
}

