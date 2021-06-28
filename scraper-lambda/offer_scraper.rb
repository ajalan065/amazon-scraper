require 'httparty'
require 'nokogiri'
require 'random_user_agent'
require 'uri'
require 'cgi'
require 'aws-sdk-sqs'
require 'mysql2'
load './constants.rb'

class OfferScraper

  S3_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/118637774962/web-scrapper-output-queue"
  RETRY_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/118637774962/web-scrapper-retry-queue"

  def error(parsed_page)
    if (parsed_page.search("title").text.to_s.strip.downcase == 'robot check') || (parsed_page.search("title").text.strip.include?("Something went wrong")) || ((parsed_page.search('center').any?) && (parsed_page.search('center').to_s.downcase.include?('hello!')))
      return true
    else
      return false
    end
  end

  def bulk_scrape_offers
    connection = Mysql2::Client.new(Constants::WRITE)
    sqs = Aws::SQS::Client.new(Constants::AWS_SQS_CONFIG)
    results = connection.query("SELECT id, s_id, url, ref_key, s3_url, converted_flag, retry_count, retry_timestamp FROM input_scraper where converted_flag=0 and ref_key='offer_scraper' and s3_url is not null limit 10")
    if results.any?
      results.each do |res|
        f = connection.query("UPDATE input_scraper SET converted_flag=1 WHERE id = " + res['id'].to_s)
        url= res["url"]
        offer_url = res["s3_url"]
        if res["ref_key"].downcase == 'offer_scraper'
          connection = Mysql2::Client.new(Constants::WRITE)
    
          http_response = HTTParty.get(offer_url, headers: {'User-Agent' => "#{RandomUserAgent.randomize}"}, follow_redirects: false)

          parsed_page = Nokogiri::HTML(http_response)
          asin_value = URI.parse(url).path.split('/').last
          
          if error(parsed_page)
            f = connection.query("UPDATE input_scraper SET converted_flag=3 WHERE id = " + res['id'].to_s)
            connection.query("UPDATE asin_details SET in_progress=2 WHERE asin_value='" + asin_value + "'")
            sqs.send_message({queue_url: RETRY_QUEUE_URL, message_body: ({id: res["id"], ref_key: res["ref_key"], action: "retry", body: ""}).to_json})
          else
            f = connection.query("UPDATE input_scraper SET converted_flag=2 WHERE id = " + res['id'].to_s)
            amazon_present = false

            temp_amz = connection.query("SELECT bb_winner FROM asin_details WHERE asin_value='" + asin_value + "'")

            if (temp_amz.any? && temp_amz.first['bb_winner'].to_s.downcase == 'amazon.com')
                amazon_present = true
            else
                olp_seller_name = parsed_page.css('.olpSellerName')
                olp_seller_name.each do |sn|
                  next unless (img = sn.search('img')).any?
                  amazon_present = true
                end
            end

            is_prime = parsed_page.css('.a-icon-prime').any?
            

            result_query = "UPDATE asin_details SET sold_by_amazon=#{amazon_present}, is_prime=#{is_prime} WHERE asin_value='" + asin_value + "'"
            connection.query(result_query)
            f = connection.query("UPDATE input_scraper SET converted_flag=4 WHERE id = " + res['id'].to_s)
            connection.query("UPDATE asin_details SET in_progress=1 WHERE asin_value='" + asin_value + "'")
            connection.query("UPDATE brand_asins SET in_progress=10 where asin_value='" + asin_value + "'")
          end
        end
      end
    end
  end
end

os = OfferScraper.new
os.bulk_scrape_offers