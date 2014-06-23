module Sneakers
  class PublisherEx
    attr_accessor :exchange

    def self.publish(msg, routing, opts={})
        @publisher_ex ||= self.new(opts)
        @publisher_ex.publish(msg, routing)
    end

    def initialize(opts={})
        @mutex = Mutex.new
        @opts = Sneakers::Config.merge(opts)
    end

    def publish(msg, routing)
        @mutex.synchronize do
          ensure_connection!() unless connected?
        end
        queue_name = Support::QueueName.new(routing[:to_queue], @opts).to_s
        queue  = @channel.queue(queue_name, :durable => @opts[:durable])
        Sneakers.logger.info("publishing <#{msg}> to [#{queue_name}]")
        queue.publish(msg, :routing_key => queue_name)
    end

  private

    def ensure_connection!()
        @bunny = Bunny.new(@opts[:amqp], :vhost => @opts[:vhost], :heartbeat => @opts[:heartbeat])
        @bunny.start
        @channel = @bunny.create_channel
        @channel.prefetch(@opts[:prefetch])
    end

    def connected?
       @bunny && @bunny.connected?
    end
  end
end

