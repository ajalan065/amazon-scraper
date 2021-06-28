<?php

use CustomerService\src\ReportsDBConnector;

require_once(__DIR__ . '/../../../CustomerService/Report/ReturnsReports/ReportsDBConnector.php');

updateStatus();

function updateStatus()
{
    $reportConnect = new ReportsDBConnector();
    $results = $reportConnect->dbStoresRead->fetchQueryToArray("SELECT id FROM amazon_brand_url_input WHERE in_progress!='Completed' limit 10");

    if (!empty($results)) {
        foreach ($results as $result) {
            $records = $reportConnect->dbStoresRead->fetchQueryToArray("select count(brand_id) as cnt from brand_asins where in_progress!=10 and brand_id= " . $result['id'] .  " group by brand_id", true);
            t("Brand id - " . $result['id']);
            t($records);
            if (empty($records)) {
                $reportConnect->dbStoresWrite->query("UPDATE amazon_brand_url_input SET in_progress='Completed' WHERE id=" . $result['id']);
            }
        }
    }

}