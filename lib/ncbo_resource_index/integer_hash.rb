require 'ruby-xxhash'

module ResourceIndex::IntegerHash
  HASH_SEED = 112233

  def self.signed_hash(str)
    hash(str, signed: true)
  end

  def self.hash(str, opts = {})
    signed = opts[:signed] == false ? false : true
    hash = XXhash.xxh64(str, HASH_SEED)
    signed ? unsigned_to_signed(hash) : hash
  end

  # Ruby has no notion of signed/unsigned so we convert manually
  def self.unsigned_to_signed(n, bits = 64)
    mid = 2**(bits-1)
    max_unsigned = 2**bits
    n >= mid ? n - max_unsigned : n
  end
end