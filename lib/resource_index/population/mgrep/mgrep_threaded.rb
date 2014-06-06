require_relative 'mgrep_client'

module RI::Population::Mgrep
  class ThreadedClient
    def initialize(host, port, threads = 1)
      @host = host
      @port = port
      @pool = []
      (threads * 1.5).to_i.times do
        @pool << RI::Population::Mgrep::Client.new(@host, @port)
      end
      @assigned = {}
    end

    def close
      pooled_client.close()
    end

    def annotate(text, longword, wholeword = nil)
      pooled_client.annotate(text, longword, wholeword)
    end

    def pooled_client
      client = @assigned[Thread.current.object_id]
      client ||= @assigned[Thread.current.object_id] = RI::Population::Mgrep::Client.new(@host, @port)
      client
    end
  end
end
