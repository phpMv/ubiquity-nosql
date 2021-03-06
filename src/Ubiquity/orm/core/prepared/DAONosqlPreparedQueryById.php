<?php
namespace Ubiquity\orm\core\prepared;

use Ubiquity\orm\OrmUtils;

class DAONosqlPreparedQueryById extends DAONosqlPreparedQueryOne {

	protected $primaryKeys;

	public function __construct($className, $cache = null) {
		parent::__construct($className, $cache);
		$this->primaryKeys = OrmUtils::getAnnotationInfo($className, "#primaryKeys");
	}

	public function execute($keyValues = [], $options = [], $useCache = false) {
		if (! \is_array($keyValues)) {
			$keyValues = [
				$keyValues
			];
		}
		$params = $keyValues;
		if ((\array_keys($keyValues) === \range(0, \count($keyValues) - 1))) {
			foreach ($this->primaryKeys as $index => $pk) {
				$params[$pk] = $keyValues[$index];
			}
		}
		return parent::execute($params, $options, $useCache);
	}
}

