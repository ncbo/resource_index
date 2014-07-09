require_relative 'mgrep_client'

module RI::Population::Mgrep
  class ThreadedClient
    def initialize(host, port, threads = 1)
      @host = host
      @port = port
      @pool = []
      @mutex = Mutex.new
      threads.times do
        @pool << RI::Population::Mgrep::Client.new(@host, @port)
      end
    end

    def close
      raise NoMethodError, "ThreadedClient uses a pool, cannot close individual connections. Use #close_all if that's really what you want."
    end

    def close_all
      @pool.each {|c| c.close}
    end

    def annotate(text, longword, wholeword = nil)
      annotation = nil
      pooled_client do |client|
        annotation = client.annotate(text, longword, wholeword)
      end
      annotation
    end

    def pooled_client(&block)
      client = nil
      @mutex.synchronize {client = @pool.pop}
      client ||= RI::Population::Mgrep::Client.new(@host, @port)
      yield client if block_given?
      @mutex.synchronize {@pool.push(client)}
    end
  end
end
