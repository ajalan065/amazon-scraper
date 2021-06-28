<?php

use CustomerService\src\ReportsDBConnector;

require_once(__DIR__ . '/../../Scraper/ScraperInput.php');
require_once(__DIR__ . '/../../../CustomerService/Report/ReturnsReports/ReportsDBConnector.php');

push_to_queue();

function push_to_queue() {
    $reportConnect = new ReportsDBConnector();

    $results = $reportConnect->dbStoresRead->fetchQueryToArray("SELECT id, brand_link, approx_pages FROM amazon_brand_url_input WHERE in_progress='link_recreated' limit 5");

    foreach ($results as $result) {
        $flag = true;
        $reportConnect->dbStoresWrite->query("UPDATE amazon_brand_url_input SET in_progress='push_to_sqs_started' WHERE id=" . $result["id"]);
        for($page=1; $page <= $result['approx_pages']; $page++) {
            $si = new ScraperInput($result["brand_link"] . $page, 'link_scraper');
            try {
                $si->pushMessageToSqsQueue();
            }catch (Exception $e) {
                $flag = false;
                error_log("could not insert into input_scraper " . $e->getMessage());
                print_r($e->getMessage());
            }
        }
        if ($flag == false) {
            $reportConnect->dbStoresWrite->query("UPDATE amazon_brand_url_input SET in_progress='push_to_sqs_failed' WHERE id=" . $result["id"]);
        }
        else {
            $reportConnect->dbStoresWrite->query("UPDATE amazon_brand_url_input SET in_progress='push_to_sqs_success' WHERE id=" . $result["id"]);
        }
    }
}
