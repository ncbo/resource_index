require 'logger'
require 'open3'
require_relative 'class'

module RI::Population
  class LabelConverter
    IDPREFIX = lambda {|prefix| "#{prefix}term:"}
    REDIS_PREFIX_KEY = "current_instance"
    REDIS_INSTANCE_VAL = ["c1:", "c2:"]

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
      redis = redis()
      cur_inst = redis.get(REDIS_PREFIX_KEY)

      if (cur_inst.nil?)
        cur_inst = REDIS_INSTANCE_VAL.first
      end

      return cur_inst
    end

    def get_dictionary_entries
      cur_inst = redis_current_instance()
      return redis.hgetall("#{cur_inst}dict")
    end

    def get_acronyms(ontology_ids)
      # Possible acronym formats from Redis: 
      # PREF,http://data.bioontology.org/ontologies/ABA-AMB
      # PREF,http://data.bioontology.org/ontologies/AERO|PREF,http://data.bioontology.org/ontologies/NEOMARK4
      # PREF,http://data.bioontology.org/ontologies/AI-RHEUM@@T020
      # PREF,http://data.bioontology.org/ontologies/AI-RHEUM@@T047,T190
      # PREF,http://data.bioontology.org/ontologies/EFO|SYN,http://data.bioontology.org/ontologies/MP
      # SYN,http://data.bioontology.org/ontologies/AERO
      # SYN,http://data.bioontology.org/ontologies/AI-RHEUM@@T047
      # SYN,http://data.bioontology.org/ontologies/GCC|SYN,http://data.bioontology.org/ontologies/SNOMEDCT@@T033

      acronyms = []
      ontologies = ontology_ids.split('|')
      ontologies.each do |ont|
        acronyms << ont.split('/').last.split('@@').first
      end
      return acronyms
    end

    def convert(mgrep_matches, annotations)
      cur_inst = redis_current_instance()
      redis_data = {}

      redis.pipelined {
        mgrep_matches.each do |string_id|
          id = get_prefixed_id(cur_inst, string_id)
          redis_data[id] = redis.hgetall(id)
        end
      }

      classes = []
      annotations.each do |annotation|
        string_id = annotation.string_id.to_i
        id = get_prefixed_id(cur_inst, string_id)
        while redis_data[id].value.is_a?(Redis::FutureNotReady)
          sleep(1.0 / 150.0)
        end
        class_matches = redis_data[id].value

        class_matches.each do |class_id, vals|
          acronyms = get_acronyms(vals)
          acronyms.each do |acronym|
            classes << RI::Population::Class.new(class_id, acronym, annotation.value, string_id)
          end
        end
      end

      return classes
    end

    def expansion_dir
      dirname = File.join(Dir.pwd, 'expansion_results')
      FileUtils.mkdir_p(dirname) unless Dir.exist?(dirname)
      return dirname
    end

    def expansion_path
      return File.join(expansion_dir(), 'label_expansion.tsv')
    end

    def expansion_path_sorted
      return File.join(expansion_dir(), 'label_expansion_sorted.tsv')
    end

    def convert_all
      logger = Logger.new(STDOUT)
      logger.level = Logger::INFO

      # Create output files
      expansion_file = File.new(expansion_path(), 'w:UTF-8')
      expansion_file_sorted = File.new(expansion_path_sorted(), 'w:UTF-8')

      # Get all terms
      dictionary_entries = get_dictionary_entries()

      # Write classes to disk
      dictionary_entries.each do |key, val|
        label = val.gsub(/[\r\n\t]/, '')
        classes = redis.hgetall(key)
        classes.each do |id, onts|
          acronyms = get_acronyms(onts)
          acronyms.each do |acronym|
            expansion_file.puts(label + "\t" + acronym + "\t" + id)
          end
        end
      end

      # Sort the output file
      stdout, stderr, status = Open3.capture3("sort #{File.path(expansion_file)} > #{File.path(expansion_file_sorted)}")
      if not status.success?
        logger.error "Error generating sorted label expansion file: #{stderr}"
      end

      expansion_file.close
      expansion_file_sorted.close
    end
  end
end