module Sneakers
    class PublisherEx
        def initialize(opts={})
            @mutex = Mutex.new
            @opts = Sneakers::Config.merge(opts)
        end

        def publish(msg, routing)
            @mutex.synchronize do
                ensure_connection! unless connected?
            end
            queue  = @channel.queue(routing[:to_queue], :durable => @opts[:durable])
            Sneakers.logger.info("publishing <#{msg}> to [#{routing[:to_queue]}]")
            queue.publish(msg, :routing_key => routing[:to_queue])
        end

        private

        def ensure_connection!
            @bunny = Bunny.new(@opts[:amqp], heartbeat: @opts[:heartbeat], vhost: @opts[:vhost])
            @bunny.start
            @channel = @bunny.create_channel
        end

        def connected?
           @bunny && @bunny.connected?
        end
    end
end