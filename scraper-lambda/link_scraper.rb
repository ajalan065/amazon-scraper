require 'httparty'
require 'nokogiri'
require 'random_user_agent'
require 'uri'
require 'cgi'
require 'aws-sdk-sqs'
require 'mysql2'
load './constants.rb'

class LinkScraper

  S3_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/118637774962/web-scrapper-output-queue"
  RETRY_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/118637774962/web-scrapper-retry-queue"

  def error(parsed_page)
    if ((parsed_page.css('.a-last').text.include?("Sorry, we just need to make sure you're not a robot")) || parsed_page.title.include?("Something went wrong") || parsed_page.css('img')&.count == 0) || parsed_page.title&.downcase == 'amazon' || (parsed_page.css('center').any? && parsed_page.css('center').downcase.include?('hello!'))
      return true
    else
      return false
    end
  end

  def get_all_asins_list
    sqs = Aws::SQS::Client.new(Constants::AWS_SQS_CONFIG)
    # received_messages = sqs.receive_message({
    #   queue_url: S3_QUEUE_URL, 
    #   max_number_of_messages: 1, # Receive at most one message.
    #   visibility_timeout: 1000
    # })

    connection = Mysql2::Client.new(Constants::WRITE)
    
    results = connection.query("SELECT id, s_id, url, ref_key, s3_url, converted_flag, retry_count, retry_timestamp FROM input_scraper where converted_flag=0 and ref_key='link_scraper' and s3_url is not null limit 10")

    # if (received_messages.any?)
    #   receive_message_result.messages.each do |message|
    #     body = JSON.parse(message.body)
    #     if (body["ref_key"].downcase == 'link_scraper')
    #       http_response = HTTParty.get(body["body"], headers: {'User-Agent' => "#{RandomUserAgent.randomize}"})
          
    #       parse_page = Nokogiri::HTML(http_response)

    #       if error(parse_page)
    #         sqs.send_message({queue_url: RETRY_QUEUE_URL, message_body: ({id: body["id"], ref_key: body["ref_key"], action: "retry", body: ""}).to_json})
    #       else 
    #         sqs.send_message({
    #           queue_url: RETRY_QUEUE_URL, 
    #           message_body: (
    #             {
    #               id: body["id"], 
    #               ref_key: body["ref_key"], 
    #               action: "success",
    #               body: { brand_id: BRAND_ID, asins: parse_page.css('.s-result-item').map { |item| item["data-asin"]}, ratings: parse_page.css('.a-icon-alt').map { |item| item.text }}.to_json }).to_json})
    #       end
    #     end
    #   end
    # end

    if (results.any?)
      results.each do |res|
      puts res
        f = connection.query("UPDATE input_scraper SET converted_flag=1 WHERE id = " + res['id'].to_s)
        if (res['ref_key'].downcase == 'link_scraper')
          http_response = HTTParty.get(res['s3_url'], headers: {'User-Agent' => "#{RandomUserAgent.randomize}"})
          
          parse_page = Nokogiri::HTML(http_response)
          if error(parse_page)
            f = connection.query("UPDATE input_scraper SET converted_flag=3 WHERE id = " + res['id'].to_s)

            sqs.send_message({queue_url: RETRY_QUEUE_URL, message_body: ({id: res["id"], ref_key: res["ref_key"], action: "retry", body: ""}).to_json})
          else
            f = connection.query("UPDATE input_scraper SET converted_flag=2 WHERE id = " + res['id'].to_s)
            x = res['url']
            page_no_url = x[-3..-1].gsub("=", "").to_i
            3.times do |i|
                x.chop! if (x[-1] != '=')
            end

            bl = connection.query("SELECT id, approx_pages FROM amazon_brand_url_input WHERE brand_link like '" + x + "'")

            puts bl.first

#             sqs.send_message({
#               queue_url: RETRY_QUEUE_URL,
#               message_body: (
#                 {
#                   id: res["id"],
#                   ref_key: res["ref_key"],
#                   action: "success",
#                   body: { brand_id: bl.first['id'], asins: parse_page.css('.s-result-item').map { |item| item["data-asin"]}, ratings: parse_page.css('.a-icon-alt').map { |item| item.text }}.to_json }).to_json})

            asins = parse_page.css('.s-result-item').map { |item| item["data-asin"]}
            ratings = parse_page.css('.a-icon-alt').map { |item| item.text }
            ratings.shift(4)

            cntr = asins.size.to_i
            cntr.times do |i|
                values = [bl.first['id'], asins[i], ratings[i]].join("','")
                q = "INSERT INTO brand_asins (brand_id, asin_value, ratings) VALUES ('" + values + "')"
                puts q
                begin
                    connection.query(q)
                 rescue Exception => e
                    puts e
                 end
            end
            f = connection.query("UPDATE input_scraper SET converted_flag=4 WHERE id = " + res['id'].to_s)
            connection.query("UPDATE amazon_brand_url_input SET in_progress='asins_list_obtained' where id=" + bl.first["id"].to_s) if (page_no_url.to_i == bl.first['approx_pages'].to_i)
          end
        end
      end
    end
  end

end

ls = LinkScraper.new

ls.get_all_asins_list

