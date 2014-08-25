require_relative 'class'

module RI::Population
  class LabelConverter
    IDPREFIX = lambda {|prefix| "#{prefix}term:"}
    KEY_STORAGE = lambda {|prefix| "#{prefix}annotator:keys"}
    CHUNK_SIZE = 500_000
    REDIS_PREFIX_KEY = "current_instance"
    REDIS_INSTANCE_VAL = ["c1:", "c2:"]
    OCCURRENCE_DELIM = "|"
    LABEL_DELIM = ","
    DATA_TYPE_DELIM = "@@"

    def initialize(redis_host, redis_port)
      @redis = Redis.new(:host => redis_host,
                         :port => redis_port,
                         :timeout => 30)
    end

    def redis
      @redis
    end

    def get_prefixed_id(instance_prefix, intId)
      return "#{IDPREFIX.call(instance_prefix)}#{intId}"
    end

    def redis_current_instance()
      # TODO: this is a hack code to allow a seamless transition
      # from a single instance of cache to a redundant (double) cache
      # this code is to be removed in a subsequent release
      return "" unless redis.exists(REDIS_PREFIX_KEY)
      # END hack code
      return redis.get(REDIS_PREFIX_KEY) || REDIS_INSTANCE_VAL[0]
    end

    def convert(mgrep_matches)
      cur_inst = redis_current_instance()
      redis_data = {}

      redis.pipelined {
        mgrep_matches.each do |string_id|
          id = get_prefixed_id(cur_inst, string_id)
          redis_data[id] = redis.hgetall(id)
        end
      }

      classes = []
      mgrep_matches.each do |string_id|
        id = get_prefixed_id(cur_inst, string_id)
        while redis_data[id].value.is_a?(Redis::FutureNotReady)
          sleep(1.0 / 150.0)
        end
        class_matches = redis_data[id].value

        class_matches.each do |class_id, vals|
          ontology_id = vals.split(",")[1].split("@@").first
          # ID comes back like this: http://data.bioontology.org/ontologies/NCIT|SYN
          # OR http://data.bioontology.org/ontologies/NCIT|PREF
          acronym = ontology_id.split("/").last.split("|").first
          classes << RI::Population::Class.new(class_id, acronym)
        end
      end

      return classes
    end
  end
end