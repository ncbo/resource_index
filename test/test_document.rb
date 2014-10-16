require 'set'
require_relative 'test_case'

MORE_THAN_ONE_DOC = Set.new([2466199616, 3984536638, 1514996459, 2974317510, 425808451, 1336324502, 3152067627, 2588657372, 4259096489, 3116223171, 2011060733])

class RI::TestDocument < RI::TestCase

  def test_documents
    res = RI::Resource.find("WITCH")
    mgrep = MockMGREPClient.new
    populator = RI::Population::Manager.new(res, mgrep_client: mgrep, bulk_index_size: 500)
    @es = RI.es # triggers delete on teardown
    @index_id = populator.populate()
    sleep(2) # wait for indexing to complete

    found = []
    DIRECT.each do |hash|
      es_docs = res.es_concept_docs(hash)
      docs = res.concept_docs(hash)
      docs.each_with_index do |doc, i|
        assert_equal es_docs[i]["_id"], doc.id
        fields = es_docs[i]["_source"]
        fields.each do |k, v|
          assert doc.respond_to?(k)
          assert_equal v, doc.send(k)
        end
      end

      deleted = MORE_THAN_ONE_DOC.delete?(hash)
      if deleted
        page = res.concept_docs_page(hash, size: 1)
        next_page = page.next_page
        assert page.total_pages > 0
        while next_page
          found.concat(page)
          assert page.length == 1
          page = res.concept_docs_page(hash, size: 1, from: offset_for_page(next_page, 1))
          next_page = page.next_page
        end
      end
    end
    assert_equal 203, found.length
    assert_equal Set.new, MORE_THAN_ONE_DOC
  end

  def test_document_find
    res = RI::Resource.find("WITCH")
    mgrep = MockMGREPClient.new
    offset = RI.db[:obr_witch_element].count - 1 # only need one record, just need alias to work
    doc_id = RI.db[:obr_witch_element][id: (offset + 1)][:local_element_id]
    populator = RI::Population::Manager.new(res, mgrep_client: mgrep, starting_offset: offset)
    @es = RI.es # triggers delete on teardown
    @index_id = populator.populate()
    sleep(2) # wait for indexing to complete

    assert_raises(ArgumentError) do
      ResourceIndex::Document.find({}, doc_id)
    end
    assert_raises(ArgumentError) do
      ResourceIndex::Document.find("AE_doesnt_exist", doc_id)
    end
    doc = ResourceIndex::Document.find("WITCH", doc_id)
    name = 'Susceptibilities of Atlantic salmon to Piscirickettsia salmonis'
    assert_equal name, doc.ae_name, "Bad ae_name"
    description = 'The aquaculture industry has confronted severe economic losses due to infectious diseases in the last years. Piscirickettsiosis or Salmonid Rickettsial Septicaemia (SRS) is the bacterial disease caused by Piscirickettsia salmonis. This Gram-negative, non-motile, cellular pathogen has the ability to infect, survive, replicate, and propagate in salmonid monocytes/macrophages generating a systemic infection characterized by the colonization of several organs including kidney, liver, spleen, intestine, brain, ovary and gills. In this study, we attempted to determine whether global gene expression differences can be detected in different genetic groups of Atlantic salmon as a result of Piscirickettsia salmonis infection. Moreover, we sought to characterize the fish transcriptional response in order to reveal the mechanisms that might confer resistance in Atlantic salmon to an infection with Piscirickettsia salmonis. In doing so, after challenging with Piscirickettsia salmonis, we selected the families with the highest (HS) and the lowest (LS) recorded susceptibility for gene expression analysis using 32K cGRASP microarrays. Our results revealed in LS families expression changes are linked to iron depletion, as well as, low contents of iron in kidney cells and low bacterial load, indicated that the iron-withholding strategy of innate immunity is part of the mechanism of resistance against Piscirickettsia salmonis. This information contributes to elucidate the underlying mechanisms of resistance to Piscirickettsia salmonis infection in Atlantic salmon and to identify new candidate genes for selective breeding programmes. Forty full-sibling families of Atlantic salmon (Salmo salar) were infected by intraperitoneal injection with 0.2 mL Piscirickettsia salmonis (PS889, isolated from Oncorhynchus kisutch, 1×104 PFU/mL). After forty days, the fishes were harvested and the cumulative mortality (dead fish / total fish) for each family was calculated. For the second challenge, the six families with the highest cumulative mortality levels were considered of relatively high susceptibility (HS) and the six families with the lowest cumulative mortality levels were considered of relatively low susceptibility (LS) to the infection. Five control and five infected fish from three HS and three LS families were analyzed. For each HS and LS family, pools of RNA from control and infected fish were prepared separately and were reverse transcribed. Four slides were for each used family hybridized including two dye-swaped slides. Labeled samples were hybridized on a 32K cDNA microarray, developed at the Consortium for Genomics Research on All Salmonids Project (cGRASP), GEO accession number: GPL8904.'
    assert_equal description, doc.ae_description, "Bad ae_description"
  end

  private

  def offset_for_page(page, pagesize)
    page * pagesize - pagesize
  end

end