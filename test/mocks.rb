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
  def get_dictionary_entries
    return DICTIONARY
  end

  def get_classes(key)
    return LABEL_ID_TO_CLASS_MAP[key]
  end

  def convert(mgrep_matches, annotations)
    classes = []
    annotations.each do |a|
      expanded = LABEL_ID_TO_CLASS_MAP[a.string_id.to_i]
      expanded.each do |cls_id, ont_id|
        acronym = ont_id.split('/').last
        classes << RI::Population::Class.new(cls_id, acronym, a.value, a.string_id)
      end
    end
    classes
  end
end

module MockMGREP
  def annotate(text,longword,wholeword=true)
    matches = []
    up_text = text.upcase
    DICTIONARY.values.each do |entry|
      hits = up_text.index_all(entry)
      hits.each do |hit|
        matches << [DICTIONARY.key(entry), (hit+1).to_s, (hit+entry.length).to_s, entry]
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