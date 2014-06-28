class RI::Document
  attr_accessor :id, :document_id, :dictionary_id, :resource
  alias :local_element_id :document_id
  alias :"local_element_id=" :"document_id="

  ##
  # Return a lazy enumerator that will lazily get results from the DB
  def self.all(resource, opts = {}, mutex = nil)
    raise ArgumentError, "Please provide a resource" unless resource.is_a?(RI::Resource)
    chunk_size = opts[:chunk_size] || 5000
    record_limit = opts[:record_limit] || Float::INFINITY
    chunk_size = record_limit if chunk_size > record_limit
    mutex ||= Mutex.new
    cls = nil
    mutex.synchronize {
      unless RI::Document.const_defined?(resource.acronym)
        cls = create_doc_subclass(resource)
      end
    }
    cls ||= RI::Document.const_get(resource.acronym)
    return Enumerator.new { |yielder|
      offset = opts[:offset] || 0
      docs = nil
      record_count = 0
      while (docs.nil? || docs.length > 0) && record_count < record_limit
        docs = RI.db["obr_#{resource.acronym.downcase}_element".to_sym].limit(chunk_size).offset(offset).all
        docs.each do |doc|
          doc[:resource] = resource
          yielder << cls.from_hash(doc) if doc
        end
        offset += chunk_size
        record_count += chunk_size
      end
    }.lazy
  end

  def self.count(resource)
    RI.db["obr_#{resource.acronym.downcase}_element".to_sym].count
  end

  def self.threach(resource, opts = {}, mutex = nil, &block)
    thread_count = opts[:thread_count] || 1
    opts[:offset] ||= 0
    threads = []
    thread_limit = ((self.count(resource) - opts[:offset]).to_f / thread_count).ceil
    opts = opts.dup
    opts[:record_limit] = thread_limit
    mutex ||= Mutex.new
    thread_count.times do |i|
      threads << Thread.new do
        new_opts = opts.dup
        new_opts[:offset] = opts[:offset] + (thread_limit * i)
        self.all(resource, new_opts, mutex).each do |doc|
          yield doc if block_given?
        end
      end
    end
    threads.each(&:join)
  end

  VIRT_MAP = {1000 => "MA", 1001 => "GRO-CPGA", 1005 => "BTO", 1006 => "CL", 1007 => "CHEBI", 1008 => "DDANAT", 1009 => "DOID", 1010 => "EMAP", 1011 => "IEV", 1012 => "ECO", 1013 => "EVOC", 1014 => "FIX", 1015 => "FB-BT", 1016 => "FB-DV", 1017 => "FB-CV", 1019 => "FAO", 1020 => "HC", 1021 => "EHDAA", 1022 => "EHDA", 1023 => "FBbi", 1024 => "LHN", 1025 => "MP", 1026 => "MAO", 1027 => "MFO", 1029 => "IMR", 1030 => "TGMA", 1031 => "MPATH", 1032 => "NCIT", 1033 => "NMR", 1035 => "PW", 1036 => "PECO", 1037 => "PTO", 1038 => "PSDS", 1039 => "PROPREO", 1040 => "PPIO", 1041 => "PSIMOD", 1042 => "OBOREL", 1043 => "REX", 1044 => "SEP", 1046 => "SBO", 1047 => "GRO-CPD", 1048 => "WB-BT", 1049 => "WB-LS", 1050 => "ZEA", 1051 => "ZFA", 1052 => "PRO-ONT", 1053 => "FMA", 1054 => "AMINO-ACID", 1055 => "GALEN", 1057 => "RADLEX", 1058 => "SNPO", 1059 => "CPRO", 1060 => "CTONT", 1061 => "SOPHARM", 1062 => "PR", 1063 => "CARO", 1064 => "FB-SP", 1065 => "TADS", 1067 => "WB-PHENOTYPE", 1068 => "SAO", 1069 => "ENVO", 1070 => "GO", 1076 => "OCRE", 1077 => "MIRO", 1078 => "BSPO", 1081 => "TTO", 1082 => "GRO", 1083 => "NPO", 1084 => "NIFSTD", 1085 => "OGMD", 1086 => "OGDI", 1087 => "OGR", 1088 => "MHC", 1089 => "BIRNLEX", 1090 => "AAO", 1091 => "SPD", 1092 => "IDO", 1094 => "PTRANS", 1095 => "XAO", 1099 => "ATMO", 1100 => "OGI", 1101 => "ICD9CM", 1104 => "BRO", 1105 => "MS", 1107 => "PATO", 1108 => "PAE", 1109 => "SO", 1110 => "TAO", 1112 => "UO", 1114 => "BILA", 1115 => "YPO", 1116 => "BHO", 1122 => "SPO", 1123 => "OBI", 1125 => "HP", 1126 => "FHHO", 1128 => "CDAO", 1130 => "ACGT-MO", 1131 => "MO", 1132 => "NCBITAXON", 1134 => "BT", 1135 => "pseudo", 1136 => "EFO", 1141 => "OPB", 1142 => "EP", 1144 => "DC-CL", 1146 => "ECG", 1148 => "BP-METADATA", 1149 => "DERMLEX", 1150 => "RS", 1152 => "MAT", 1158 => "CBO", 1172 => "VO", 1183 => "LIPRO", 1190 => "OPL", 1192 => "CPTAC", 1222 => "APO", 1224 => "SYMP", 1237 => "SITBAC", 1247 => "GEOSPECIES", 1249 => "SBRO", 1257 => "MEGO", 1290 => "ABA-AMB", 1304 => "BCGO", 1311 => "IDOMAL", 1314 => "CLO", 1321 => "NEMO", 1328 => "HOM", 1332 => "BFO", 1335 => "PEO", 1341 => "COSTART", 1343 => "HL7", 1344 => "ICPC", 1347 => "MEDLINEPLUS", 1348 => "OMIM", 1349 => "PDQ", 1350 => "LOINC", 1351 => "MESH", 1352 => "NDFRT", 1353 => "SNOMEDCT", 1354 => "WHO-ART", 1362 => "HAO", 1369 => "PHYFIELD", 1370 => "ATO", 1381 => "NIFDYS", 1393 => "IAO", 1394 => "SSO", 1397 => "GAZ", 1398 => "LDA", 1401 => "ICNP", 1402 => "NIFCELL", 1404 => "UBERON", 1407 => "TEDDY", 1410 => "KISAO", 1411 => "ICF", 1413 => "SWO", 1414 => "OGMS", 1415 => "CTCAE", 1417 => "FLU", 1418 => "TOK", 1419 => "TAXRANK", 1422 => "MEDDRA", 1423 => "RXNORM", 1424 => "NDDF", 1425 => "ICD10PCS", 1426 => "MDDB", 1427 => "RCD", 1428 => "NIC", 1429 => "ICPC2P", 1430 => "AI-RHEUM", 1438 => "MCBCC", 1439 => "GFO", 1440 => "GFO-BIO", 1444 => "CHEMINF", 1461 => "TMO", 1484 => "ICECI", 1487 => "ICD11-BODYSYSTEM", 1488 => "JERM", 1489 => "OAE", 1490 => "PLATSTG", 1491 => "IMGT-ONTOLOGY", 1494 => "TMA", 1497 => "PMA", 1498 => "EDAM", 1500 => "RNAO", 1501 => "NEOMARK3", 1504 => "CPT", 1505 => "OMIT", 1506 => "GO-EXT", 1507 => "CCO", 1509 => "ICPS", 1510 => "CPTH", 1515 => "INO", 1516 => "ICD10", 1517 => "EHDAA2", 1520 => "LSM", 1521 => "NEUMORE", 1522 => "BP", 1523 => "OBOE-SBC", 1526 => "CRISP", 1527 => "VANDF", 1528 => "HUGO", 1529 => "HCPCS", 1530 => "ADW", 1532 => "SIO", 1533 => "BAO", 1537 => "IDOBRU", 1538 => "ROLEO", 1539 => "NIGO", 1540 => "DDI", 1541 => "MCCL", 1544 => "CO", 1545 => "CO-WHEAT", 1550 => "PHARE", 1552 => "REPO", 1553 => "ICD10CM", 1555 => "VSAO", 1560 => "COGPO", 1565 => "OMRSE", 1567 => "PVONTO", 1568 => "AEO", 1569 => "HPIO", 1570 => "TM-CONST", 1571 => "TM-OTHER-FACTORS", 1572 => "TM-SIGNS-AND-SYMPTS", 1573 => "TM-MER", 1574 => "VHOG", 1575 => "EXO", 1576 => "FDA-MEDDEVICE", 1578 => "ELIXHAUSER", 1580 => "AERO", 1581 => "HLTHINDCTRS", 1582 => "CAO", 1583 => "CMO", 1584 => "MMO", 1585 => "XCO", 1586 => "OntoOrpha", 1587 => "PO", 1588 => "ONTODT", 1613 => "BDO", 1614 => "IXNO", 1615 => "CHEMBIO", 1616 => "PHYLONT", 1621 => "NBO", 1626 => "EMO", 1627 => "HOMERUN", 1630 => "UCSFEPIC", 1632 => "WSIO", 1633 => "COGAT", 1638 => "ONTODM-CORE", 1639 => "EPILONT", 1640 => "PEDTERM", 1649 => "OSHPD", 1650 => "UNITSONT", 1651 => "SDO", 1655 => "PHARMGKB", 1656 => "PHENOMEBLAST", 1659 => "VT", 1661 => "UCSFXPLANT", 1665 => "SHR", 1666 => "MFOEM", 1670 => "ICDO3", 1671 => "QIBO", 1672 => "DIKB", 1676 => "RCTONT", 1686 => "NEOMARK4", 1689 => "FYPO", 1694 => "CPT-KM", 1696 => "SYN", 1697 => "UCSFORTHO", 1699 => "VIVO", 3000 => "MIXSCV", 3002 => "MF", 3003 => "CNO", 3004 => "NATPRO", 3006 => "OOEVV", 3007 => "UCSFICU", 3008 => "CARELEX", 3009 => "MEO", 3012 => "NONRCTO", 3013 => "DIAGONT", 3015 => "PMR", 3016 => "ERO", 3017 => "GCC", 3019 => "RH-MESH", 3020 => "CPO", 3021 => "ATC", 3022 => "BIOMODELS", 3025 => "CTX", 3028 => "SOY", 3029 => "SPTO", 3030 => "CANCO", 3031 => "QUDT", 3032 => "EPICMEDS", 3038 => "HOM-TEST", 3042 => "TEO", 3043 => "MEDABBS", 3045 => "ICD9CM-KM", 3046 => "MDCDRG", 3047 => "DEMOGRAPH", 3058 => "DWC", 3062 => "I2B2-PATVISDIM", 3077 => "ONTODM-KDD", 3078 => "PHENX", 3090 => "ONTOMA", 3092 => "CLINIC", 3094 => "DWC-TEST", 3104 => "USSOC", 3108 => "CCONT", 3114 => "RPO", 3119 => "OBIWS", 3120 => "PCO", 3124 => "VSO", 3126 => "NIFSUBCELL", 3127 => "IMMDIS", 3129 => "CONSENT-ONT", 3131 => "PROVO", 3136 => "NHDS", 3137 => "ONSTR", 3139 => "MIRNAO", 3146 => "CMS", 3147 => "CLIN-EVAL", 3150 => "BRIDG", 3151 => "GEXO", 3152 => "REXO", 3153 => "NTDO", 3155 => "ONTOKBCF", 3157 => "GENETRIAL", 3158 => "SWEET", 3159 => "VARIO", 3162 => "RETO", 3167 => "GLOB", 3169 => "GLYCO", 3174 => "IDODEN", 3176 => "XEO", 3178 => "CANONT", 3179 => "GENE-CDS", 3180 => "MEDO", 3181 => "ONTOPNEUMO", 3183 => "IFAR", 3184 => "ZIP3", 3185 => "GPI", 3186 => "I2B2-LOINC", 3189 => "TOP-MENELAS", 3190 => "PATHLEX", 3191 => "MPO", 3192 => "MCCV", 3194 => "PHENOSCAPE-EXT", 3195 => "ICD09", 3197 => "HIMC-CPT", 3198 => "SEMPHYSKB-HUMAN", 3199 => "UCSFICD910CM", 3200 => "ZIP5", 3201 => "BCO", 3203 => "HINO", 3204 => "PORO", 3205 => "ICD0", 3206 => "HCPCS-HIMC", 3207 => "ATOL", 3208 => "IDQA", 3209 => "OPE", 3210 => "TRAK", 3211 => "TEST-PROD", 3212 => "GLYCOPROT", 3214 => "OGSF", 3215 => "MIXS", 3216 => "BAO-GPCR", 3217 => "CABRO", 3218 => "TRON", 3219 => "MSTDE", 3220 => "MSTDE-FRE", 3221 => "DERMO", 3222 => "RSA", 3223 => "GCO", 3224 => "UCSFI9I10CMPCS", 3226 => "SBOL", 3227 => "OVAE", 3228 => "ELIG", 3230 => "EPSO", 3231 => "GLYCANONT", 3232 => "SNMI", 3233 => "CHD", 3234 => "BOF", 3236 => "VTO", 3237 => "VBCV", 3238 => "PDO", 3239 => "CSSO", 3240 => "RNPRIO", 3241 => "UDEF", 3242 => "NHSQI", 3243 => "NHSQI2009", 3244 => "SEDI", 3245 => "SuicidO", 3246 => "ONL-MSA", 3247 => "EDDA", 3249 => "ONL-DP", 3250 => "ONL-MR-DA", 3251 => "ADO", 3252 => "SSE", 3253 => "OntoVIP", 3255 => "CHMO", 3258 => "OBR-Scolio", 3259 => "InterNano", 3261 => "STNFRDRXDEMO", 3262 => "BCTEO", 3263 => "OntoBioUSP", 3264 => "WH", 3265 => "BNO", 3266 => "OBI_BCGO", 3267 => "DCO", 3268 => "ERNO", 3269 => "BICSO", 3270 => "suicideo", 3271 => "BSAO", 3272 => "CARD", 3273 => "HRDO", 3274 => "MSV"}
  def indexable_hash
    fields = self.resource.fields.keys.map {|f| f.downcase.to_sym}
    hash = {}
    fields.each {|f| hash[f] = self.send(f).force_encoding('UTF-8')}
    ont_fields = self.resource.fields.lazy.select {|f| f[1].ontology}.map {|f| f[0].to_sym}
    hash[:manual_annotations] = []
    # Look up manual annotations from the old ids
    ont_fields.each do |f|
      f = f.downcase
      next if hash[f].nil? || hash[f].empty?
      ids = hash[f].split("> ")
      ids.each do |id|
        ont, cls = id.split("/")
        ont = clean_ont_id(ont)
        cls = clean_cls_id(ont, cls)

        onts = RI.db.from(:obs_ontology)
        begin
          local_ont_id = onts[virtual_ontology_id: ont][:local_ontology_id]
        rescue => e
          puts "Manual annotations, problem getting ontology #{ont}: #{e.message}"
          next
        end

        concepts = RI.db.from(:obs_concept)
        begin
          cls_uri = concepts.where(local_concept_id: "#{local_ont_id}/#{cls}").first[:full_id]
        rescue => e
          puts "Manual annotations, problem getting concept #{ont} | #{local_ont_id}/#{cls}: #{e.message}"
          next
        end

        acronym = VIRT_MAP[ont.to_i].upcase
        cls = RI::Population::Class.new(acronym, cls_uri)
        hash[f] = "#{acronym}\C-_#{cls_uri}"
        hash[:manual_annotations] << cls
      end
    end
    hash[:id] = self.document_id
    hash
  end

  def annotatable_text
    fields = self.resource.fields.keys.map {|f| f.downcase.to_sym}
    fields.map {|f| self.send(f)}.join("\n\n")
  end

  private

  def clean_cls_id(ont, cls)
    case ont.to_i
    when 1132
      cls = "obo:#{cls.sub(':', '_')}" unless cls.start_with?("obo:")
    when 1070

    end
    cls
  end

  def clean_ont_id(ont)
    case ont.to_i
    when 46440
      ont = 1070
    end
    ont
  end

  def self.create_doc_subclass(resource)
    fields = resource.fields.keys.map {|f| f.downcase.to_sym}
    cls = Class.new(RI::Document) do
      fields.each do |field|
        define_method field do
          instance_variable_get("@#{field}")
        end
        define_method "#{field}=".to_sym do |arg|
          instance_variable_set("@#{field}", arg)
        end
      end
    end
    cls.define_singleton_method :from_hash do |hsh|
      inst = self.new
      hsh.each {|k,v| inst.send("#{k}=", v)}
      inst
    end
    RI::Document.const_set(resource.acronym, cls)
    cls
  end
end