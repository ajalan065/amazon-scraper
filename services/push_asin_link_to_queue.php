<?php

use CustomerService\src\ReportsDBConnector;

require_once(__DIR__ . '/../../Scraper/ScraperInput.php');
require_once(__DIR__ . '/../../../CustomerService/Report/ReturnsReports/ReportsDBConnector.php');

push_to_queue();

function push_to_queue() {
    $reportConnect = new ReportsDBConnector();

    $results = $reportConnect->dbStoresRead->fetchQueryToArray("SELECT id, asin_value FROM brand_asins WHERE in_progress=0 limit 10");

    foreach ($results as $result) {
        $flag = true;

        $si = new ScraperInput("https://www.amazon.com/dp/" . $result["asin_value"], 'asin_scraper');

        try {
            $si->pushMessageToSqsQueue();
        }catch (Exception $e) {
            $flag = false;
            error_log("could not insert into input_scraper " . $e->getMessage());
            print_r($e->getMessage());
        }
        if ($flag == false) {
            $reportConnect->dbStoresWrite->query("UPDATE brand_asins SET in_progress=2 WHERE id=" . $result["id"]);
        }
        else {
            $reportConnect->dbStoresWrite->query("UPDATE brand_asins SET in_progress=1 WHERE id=" . $result["id"]);
        }
    }
}
