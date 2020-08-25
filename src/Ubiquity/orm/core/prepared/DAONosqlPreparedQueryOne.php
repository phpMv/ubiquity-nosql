<?php
namespace Ubiquity\orm\core\prepared;

use Ubiquity\events\DAOEvents;
use Ubiquity\events\EventsManager;
use Ubiquity\orm\DAONosql;

class DAONosqlPreparedQueryOne extends DAONosqlPreparedQuery {

	public function __construct($className, $cache = null) {
		parent::__construct($className, $cache);
		$this->prepare();
	}

	public function execute($keyValues = [], $useCache = false) {
		$object = null;
		$doc = $this->db->queryOne($this->tableName, $params);
		if ($doc) {
			if ($this->allPublic) {
				$object = DAONosql::_loadSimpleObjectFromRow($doc, $className, $this->memberNames, $this->transformers);
			} else {
				$object = DAONosql::_loadObjectFromRow($db, $doc, $className, $this->memberNames, $this->accessors, $this->transformers);
			}
			EventsManager::trigger(DAOEvents::GET_ONE, $object, $className);
		}
		return $object;
	}
}

