<?php
namespace Ubiquity\orm\core\prepared;

use Ubiquity\orm\OrmUtils;

class DAONosqlPreparedQuery {

	protected $db;

	protected $className;

	protected $collectionName;

	protected $transformers;

	protected $accessors;

	protected $allPublic;

	protected $memberNames;

	public function __construct($className, $cache = null) {
		$this->className = $className;
	}

	protected function prepare() {
		$this->db = DAO::getDb($this->className);
		$metaDatas = OrmUtils::getModelMetadata($this->className);
		$this->collectionName = $metaDatas['#tableName'];
		$this->memberNames = $metaDatas['#memberNames'];
		if (! ($this->allPublic = OrmUtils::hasAllMembersPublic($this->className))) {
			$this->accessors = $metaDatas['#accessors'];
		}
	}
}

