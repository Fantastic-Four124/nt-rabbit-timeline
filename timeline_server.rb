#!/usr/bin/env ruby
require 'bunny'
require 'thread'
require 'mongoid'
require 'mongoid_search'
require 'sinatra'
require 'byebug'
require 'time_difference'
require 'time'
require 'json'
require 'redis'
require 'rest-client'
require 'set'
require_relative 'models/tweet'

Mongoid.load! "config/mongoid.yml"
$follow_service = "https://fierce-garden-41263.herokuapp.com/"

configure do
  tweet_uri = URI.parse(ENV["TWEET_REDIS_URL"])
  user_uri = URI.parse(ENV['USER_REDIS_URL'])
  follow_uri = URI.parse(ENV['FOLLOW_REDIS_URL'])
  tweet_uri_spare = URI.parse(ENV['TWEET_REDIS_SPARE_URL'])
  tweet_uri_3 = URI.parse(ENV['TWEET_REDIS_3'])
  $tweet_redis_spare = Redis.new(:host => tweet_uri_spare.host, :port => tweet_uri_spare.port, :password => tweet_uri_spare.password)
  $tweet_redis = Redis.new(:host => tweet_uri.host, :port => tweet_uri.port, :password => tweet_uri.password)
  $tweet_redis_3 = Redis.new(:host => tweet_uri_3.host, :port => tweet_uri_3.port, :password => tweet_uri_3.password)
  $user_redis = Redis.new(:host => user_uri.host, :port => user_uri.port, :password => user_uri.password)
  $follow_redis = Redis.new(:host => follow_uri.host, :port => follow_uri.port, :password => follow_uri.password)
  PREFIX = '/api/v1'
end


class TimelineServer
  def initialize(id)
    puts "initializing"
    @connection = Bunny.new(id)
    @connection.start
    @channel = @connection.create_channel
    @num = 0
  end

  def start(queue_name)
    @queue = channel.queue(queue_name)
    @exchange = channel.default_exchange
    subscribe_to_queue
  end

  def stop
    channel.close
    connection.close
  end

  private

  attr_reader :channel, :exchange, :queue, :connection

  def subscribe_to_queue
    queue.subscribe(block: true) do |_delivery_info, properties, payload|
      process(payload)
    end
  end

  def process(original)
    @num = @num + 1
    puts "started processing timeline: #{@num.to_s}"
    hydrate_original = JSON.parse(original)
    if (hydrate_original['isFo'])
      timeline_update_after_follow(hydrate_original['user_id'],hydrate_original['leader_id'])
    else
      timeline_update_after_unfollow(hydrate_original['user_id'])
    end
  end

  def timeline_update_after_unfollow(user_id)
    leader_list = get_leader_list(user_id)
    leaders_tweet_list = generate_potential_tweet_after_unfo(leader_list)
    $tweet_redis.del(user_id.to_s + '_timeline')
    $tweet_redis_spare.del(user_id.to_s + '_timeline')
    $tweet_redis_3.del(user_id.to_s + '_timeline')
    assemble_timeline(leaders_tweet_list,user_id)
  end

  def get_leader_list(user_id)
    leader_list = []
    if $follow_redis.get("#{user_id} leaders").nil?
      leader_list = JSON.parse($follow_redis.get("#{user_id} leaders")).keys
    else
      follow_list_link = $follow_service + "/leaders/#{user_id}"
      leader_list = RestClient.get(follow_list_link)
      leader_list = JSON.parse(leader_list) if leader_list
    end
    leader_list
  end

  # Generate unfollow list
  def generate_potential_tweet_after_unfo(leader_list)
    leaders_tweet_list = []
    puts leader_list
    if leader_list != "[]".to_json
      leader_list.each do |leader_id|
        sub_list = get_new_leader_feed(leader_id)
        leaders_tweet_list << sub_list
      end
    end
    leaders_tweet_list
  end

  def timeline_update_after_follow(user_id,leader_id)
    potential_tweet_list = generate_potential_tweet_list(user_id,leader_id)
    $tweet_redis.del(user_id.to_s + '_timeline')
    $tweet_redis_spare.del(user_id.to_s + '_timeline')
    $tweet_redis_3.del(user_id.to_s + '_timeline')
    assemble_timeline(potential_tweet_list,user_id)
  end

  def generate_potential_tweet_list(user_id,leader_id)
    potential_tweet_list = []
    current_timeline = []
    if $tweet_redis.llen(user_id.to_s + '_timeline') > 0
      previous_timeline = []
      $tweet_redis.lrange(user_id.to_s + "_timeline", 0, -1).each do |tweet|
        previous_timeline << tweet #JSON.parse(tweet)
      end
      potential_tweet_list << current_timeline
    end
    new_leader_feed = get_new_leader_feed(leader_id)
    potential_tweet_list << new_leader_feed
    potential_tweet_list
  end

  def get_new_leader_feed(leader_id)
    new_leader_feed = []
    if $tweet_redis.llen(leader_id.to_s+ "_feed") > 0
        $tweet_redis.lrange(leader_id.to_s+ "_feed", 0, -1).each do |tweet|
          new_leader_feed << tweet #JSON.parse(tweet)
        end
    else
      new_leader_feed  = Tweet.where('user.id' => leader_id).desc(:date_posted).limit(50)
    end
    new_leader_feed
  end

  def assemble_timeline (leaders_tweet_list,user_id)
    count = 0
    empty_list_set = Set.new

    while (count < 50 && empty_list_set.size < leaders_tweet_list.size)
      temp_tweet = nil
      index = -1
      for i in 0..leaders_tweet_list.size - 1 do
        next if check_empty_list(leaders_tweet_list,i,empty_list_set)
        if (temp_tweet.nil? || leaders_tweet_list[i][0][:date_posted] > temp_tweet[:date_posted])
          temp_tweet = leaders_tweet_list[i][0]
          index = i
        end
      end
      push_tweet_to_redis(leaders_tweet_list,user_id,temp_tweet,index) if !temp_tweet.nil?
    end
  end

  def check_empty_list(leaders_tweet_list,i,empty_list_set)
    if leaders_tweet_list[i].empty?
      empty_list_set.add(i)
      return true
    end
    return false
  end

  def push_tweet_to_redis(leaders_tweet_list,user_id,temp_tweet,index)
    $tweet_redis.lpush(user_id.to_s + "_timeline",temp_tweet.to_json)
    $tweet_redis_spare.lpush(user_id.to_s + "_timeline",temp_tweet.to_json)
    $tweet_redis_3.lpush(user_id.to_s + "_timeline",temp_tweet.to_json)
    leaders_tweet_list[index].shift if index >= 0
  end

end


#begin
#  server = WriterServer.new(ENV["RABBITMQ_BIGWIG_RX_URL"])
#  server.start('writer_queue')
#rescue Interrupt => _
#  server.stop
#end
