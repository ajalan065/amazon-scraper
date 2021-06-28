require 'httparty'
require 'random_user_agent'
require 'uri'
require 'cgi'
require 'aws-sdk-sqs'
require 'nokogiri'
require 'hpricot'
require 'mysql2'
load './constants.rb'

class AsinScraper

  S3_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/118637774962/web-scrapper-output-queue"
  RETRY_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/118637774962/web-scrapper-retry-queue"

  def error(parsed_page)
    if (parsed_page.search("title").text.to_s.strip.downcase == 'robot check') || (parsed_page.search("title").text.strip.include?("Something went wrong")) || ((parsed_page.search('center').any?) && (parsed_page.search('center').to_s.downcase.include?('hello!')) || (parsed_page/"b#product-title").any?)
      return true
    else
      return false
    end
  end

  def bulk_scrape_asins
    sqs = Aws::SQS::Client.new(region: 'us-east-1')
    
    connection = Mysql2::Client.new(Constants::WRITE)
    
    results = connection.query("SELECT id, s_id, url, ref_key, s3_url, converted_flag, retry_count, retry_timestamp FROM input_scraper where converted_flag=0 and ref_key='asin_scraper' and s3_url is not null limit 1")
    puts results
    if (results.any?)
      results.each do |res|
        if (res["ref_key"].downcase == 'asin_scraper')
          http_response = HTTParty.get(res['s3_url'], headers: {'User-Agent' => "#{RandomUserAgent.randomize}"}, follow_redirects: false)
          
          f = connection.query("UPDATE input_scraper SET converted_flag=1 WHERE id = " + res['id'].to_s)
          
          parsed_page = Hpricot(http_response)

          if error(parsed_page)
            f = connection.query("UPDATE input_scraper SET converted_flag=3 WHERE id = " + res['id'].to_s)
            sqs.send_message({queue_url: RETRY_QUEUE_URL, message_body: ({id: res["id"], ref_key: res["ref_key"], action: "retry", body: ""}).to_json})
          else

            f = connection.query("UPDATE input_scraper SET converted_flag=2 WHERE id = " + res['id'].to_s)

             asin_value = (parsed_page/'#cerberus-data-metrics')[0]["data-asin"] rescue nil
             asin_ratings = (parsed_page/".a-icon-alt")[0].text.to_s.strip rescue ''
             price = (parsed_page/"#price_inside_buybox").text.to_s.strip.sub('$', '') || (parsed_page/"#newBuyBoxPrice").text.to_s.strip.sub('$', '')

             if (price.nil? || price=='0.0' || price == '' || price == 0.0)
                price = (parsed_page/"span.offer-price")[0].to_s.split(">")[1].split("<")[0].sub('$', '').strip rescue 0.0
             end

             if (price.nil? || price == '0.0' || price == '' || price == 0.0)
                price = (parsed_page/"span.a-color-price")[0].to_s.split(">")[1].split("<")[0].sub('$', '').strip rescue 0.0
             end

            if ((parsed_page/"#merchant-info").text.to_s.strip.downcase.include?('amazon.com'))
              merchant_info = 'amazon.com'
            elsif ((parsed_page/"#sellerProfileTriggerId").any?)
              merchant_info = (parsed_page/"#sellerProfileTriggerId").text.to_s
            end
            
            additional_information = (parsed_page/'#productDetails_detailBullets_sections1')

            if additional_information.any?
                data = additional_information.search('tr').map{|t| t.search('th,td')}.map{|t| t.text.strip}.map{|t| t.strip.gsub("\n", "")}.map{|t| t.strip}.reject{|t| t == ""}
                final_data = []

                data.map{|t| t.split('  ')}.each do |row|
                puts "-------------------"
                  row_str = row.reject{|t| t == ""}.join('????')
                  final_data << row_str if row_str.downcase.include?('best sellers rank')
                end

                if final_data.any?
                puts final_data
                  rank = final_data.first.strip.split(" in ").first.split('????')[1].strip.sub('#', '').gsub(',', '').gsub(', ', '').strip
                  category = final_data.first.strip.split(" in ")[1].split(' (').first
                  puts rank
                else
                  rank = ''
                  category = ''
                end
            else
                temp_text = (parsed_page/'#SalesRank').text.to_s.strip
                temp_text_arr = temp_text.split(":")
                temp_arr = temp_text_arr[1].split(" in ") rescue []
                rank = temp_arr[0].strip.sub('#', '').gsub(',', '').gsub(', ', '').to_s.strip rescue ""
                category = temp_arr[1].split(" (")[0].to_s.strip rescue ""
            end

            if (asin_value.nil? || asin_value == '')
                asin_value =  (parsed_page/"#ASIN").to_s.split('"')[7]
            end

            asin_value = res['url'].split('dp/')[1];
            puts "-----------" + rank + "-----------" + category + "--------------" + asin_ratings + "------------" + asin_value

            asin_id_rec = connection.query("SELECT id from brand_asins where asin_value='" + asin_value + "'")

            if asin_id_rec.any?
                asin_id = asin_id_rec.first["id"]
            end

            first_rec = connection.query("SELECT id from asin_details where asin_value='" + asin_value + "'")

            if first_rec.any?
              connection.query("UPDATE asin_details SET bb_winner='" + merchant_info+ "', bb_price=" + price.to_s + ", category='" + category + "', rank='"+ rank.to_s + "' WHERE id='" + first_rec.first["id"].to_s + "'")
            else
              field_values = ([asin_id, asin_value, merchant_info, price, category, rank, asin_ratings]).join('","')
              puts field_values
              connection.query('INSERT INTO asin_details (asin_id, asin_value, bb_winner, bb_price, category, rank, asin_ratings) VALUES ("' + field_values + '")')
            end
            f = connection.query("UPDATE input_scraper SET converted_flag=4 WHERE id = " + res['id'].to_s)
          end
        end
      end
    end
  end

end

as = AsinScraper.new
as.bulk_scrape_asins
