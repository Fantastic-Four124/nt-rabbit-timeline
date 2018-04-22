worker_processes Integer(ENV['UNICORN_WORKERS'] || 4)
timeout 120
preload_app true


after_fork do |server, worker|
  require_relative '../timeline_server'
  $rabbit_server = TimelineServer.new(ENV["RABBITMQ_BIGWIG_RX_URL"])
  $rabbit_server.start('timeline_queue')
  rescue Interrupt => _
    $rabbit_server.stop
end

before_exec do |server|
  ENV['BUNDLE_GEMFILE'] = "#{apppath}/current/Gemfile"
end
