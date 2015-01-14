##
# Monkeypatch to find starting point for all occurrances of `needle` in string
class ::String
  def index_all(needle)
    found = []
    current_index = -1
    while current_index = index(needle, current_index+1)
      found << current_index
    end
    found
  end
end

##
# Monkeypatched mock methods for use in populations so we don't need redis, mgrep, 4store, etc
class RI::Population::LabelConverter
  def convert(mgrep_matches, annotations)
    classes = []
    annotations.each do |a|
      expanded = LABEL_ID_TO_CLASS_MAP[a.string_id.to_i]
      expanded.each do |cls_data|
        acronym, id = cls_data
        classes << RI::Population::Class.new(id, acronym, a.value, a.string_id)
      end
    end
    classes
  end

  def convert_all
    path = File.join(Dir.pwd, 'expansion_results', 'label_expansion.tsv')
    expansion_file = File.new(path, 'w')

    DICTIONARY.each do |k,v|
      label = k
      classes = LABEL_ID_TO_CLASS_MAP[v]
      classes.each do |cls|
        acronym = cls.first
        id = cls.last
        expansion_file.puts(label + "\t" + acronym + "\t" + id)
      end
    end
    expansion_file.close

    sorted_path = File.join(Dir.pwd, 'expansion_results', 'label_expansion_sorted.tsv')
    expansion_file_sorted = File.new(sorted_path, 'w')
    Open3.capture3("sort #{path} > #{sorted_path}")
    expansion_file_sorted.close
  end
end

module MockMGREP
  def annotate(text,longword,wholeword=true)
    matches = []
    up_text = text.upcase
    DICTIONARY.keys.each do |entry|
      hits = up_text.index_all(entry)
      hits.each do |hit|
        matches << [DICTIONARY[entry].to_s, (hit+1).to_s, (hit+entry.length).to_s, entry]
      end
    end
    return RI::Population::Mgrep::AnnotatedText.new(up_text, matches)
  end
end

class RI::Population::Mgrep::Client
  include MockMGREP
end

class MockMGREPClient
  include MockMGREP
end

class RI::Population::Class
  def retrieve_ancestors(acronym, submission_id)
    XXHASH_TO_ANCESTOR_XXHASH[self.xxhash]
  end
end

class RI::Population::Manager
  def latest_submissions
    {}
  end
end