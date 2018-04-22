# server.rb
require 'sinatra'
#Thread.new {
#  require_relative 'writer_server'
#}



# DB Setup
#Mongoid.load! "config/mongoid.yml"

#set binding

set :bind, '0.0.0.0' # Needed to work with Vagrant
set :port, 8085

get '/' do
  'hi'
end
