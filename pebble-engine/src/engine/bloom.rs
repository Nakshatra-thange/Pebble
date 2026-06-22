/// Bloom filter — probabilistic "definitely not here" check.
///
/// Uses double hashing to derive k hash values from two seeds,
/// avoiding the cost of k independent hash functions.
/// Formula: h_i(key) = (h1(key) + i * h2(key)) % num_bits
///
/// On-disk layout (appended to SSTable after data, before index):
/// [ num_bits: 8 bytes ][ num_hash_fns: 1 byte ][ bit_array: ceil(num_bits/8) bytes ]

pub struct BloomFilter {
    bits: Vec<u8>,    // bit array, packed into bytes
    num_bits: u64,
    num_hash_fns: u8, // k — number of hash positions per key
}

impl BloomFilter {
    /// Create a new empty bloom filter.
    ///
    /// `expected_keys` — how many keys will be inserted
    /// `false_positive_rate` — target FPR e.g. 0.01 for 1%
    ///
    /// Formula for optimal num_bits:  m = -n * ln(p) / (ln2)^2
    /// Formula for optimal k:        k = (m/n) * ln2
    pub fn new(expected_keys: usize, false_positive_rate: f64) -> Self {
        let n = expected_keys.max(1) as f64;
        let p = false_positive_rate.clamp(1e-10, 0.999);

        let ln2 = std::f64::consts::LN_2;
        let num_bits = ((-n * p.ln()) / (ln2 * ln2)).ceil() as u64;
        let num_bits = num_bits.max(64); // floor at 64 bits

        let num_hash_fns = ((num_bits as f64 / n) * ln2).round() as u8;
        let num_hash_fns = num_hash_fns.clamp(1, 20);

        let byte_count = ((num_bits + 7) / 8) as usize;

        BloomFilter {
            bits: vec![0u8; byte_count],
            num_bits,
            num_hash_fns,
        }
    }

    /// Restore a bloom filter from its serialized fields (used when opening SSTable)
    pub fn from_parts(num_bits: u64, num_hash_fns: u8, bits: Vec<u8>) -> Self {
        BloomFilter { bits, num_bits, num_hash_fns }
    }

    /// Insert a key into the filter
    pub fn insert(&mut self, key: &[u8]) {
        let (h1, h2) = self.hash_pair(key);
        for i in 0u64..self.num_hash_fns as u64 {
            let bit = self.probe(h1, h2, i);
            self.set_bit(bit);
        }
    }

    /// Check if a key might be present.
    /// Returns false  → key is DEFINITELY NOT in the set (no disk seek needed)
    /// Returns true   → key MIGHT be present (proceed with normal lookup)
    pub fn might_contain(&self, key: &[u8]) -> bool {
        let (h1, h2) = self.hash_pair(key);
        for i in 0u64..self.num_hash_fns as u64 {
            let bit = self.probe(h1, h2, i);
            if !self.get_bit(bit) {
                return false; // definitely absent
            }
        }
        true // possibly present
    }

    // ── Serialization ────────────────────────────────────────────────────────

    /// Serialize to bytes for writing to SSTable file
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(9 + self.bits.len());
        buf.extend_from_slice(&self.num_bits.to_le_bytes());
        buf.push(self.num_hash_fns);
        buf.extend_from_slice(&self.bits);
        buf
    }

    /// Total encoded size in bytes (used to compute bloom_offset in SSTable footer)
    pub fn encoded_size(&self) -> usize {
        8 + 1 + self.bits.len()
    }

    /// Decode from raw bytes (slice starting at the bloom filter section)
    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.len() < 9 {
            return None;
        }
        let num_bits = u64::from_le_bytes(data[0..8].try_into().ok()?);
        let num_hash_fns = data[8];
        let byte_count = ((num_bits + 7) / 8) as usize;
        if data.len() < 9 + byte_count {
            return None;
        }
        let bits = data[9..9 + byte_count].to_vec();
        Some(BloomFilter { bits, num_bits, num_hash_fns })
    }

    pub fn num_bits(&self) -> u64 { self.num_bits }
    pub fn num_hash_fns(&self) -> u8 { self.num_hash_fns }

    /// Theoretical false positive rate given how many keys were inserted
    pub fn expected_fpr(&self, inserted_keys: usize) -> f64 {
        let k = self.num_hash_fns as f64;
        let m = self.num_bits as f64;
        let n = inserted_keys as f64;
        // FPR ≈ (1 - e^(-k*n/m))^k
        (1.0 - std::f64::consts::E.powf(-k * n / m)).powf(k)
    }

    // ── Internal ─────────────────────────────────────────────────────────────

    /// Double hashing: derive two independent 64-bit hashes from the key.
    /// We use FNV-1a for h1 and a shifted variant for h2.
    fn hash_pair(&self, key: &[u8]) -> (u64, u64) {
        // FNV-1a 64-bit
        let mut h1: u64 = 0xcbf29ce484222325;
        for &b in key {
            h1 ^= b as u64;
            h1 = h1.wrapping_mul(0x100000001b3);
        }

        // Second hash: FNV with a different offset prime
        let mut h2: u64 = 0x9e3779b97f4a7c15;
        for &b in key {
            h2 ^= b as u64;
            h2 = h2.wrapping_mul(0x517cc1b727220a95);
        }

        (h1, h2)
    }

    /// Derive the i-th probe position via double hashing
    fn probe(&self, h1: u64, h2: u64, i: u64) -> u64 {
        h1.wrapping_add(i.wrapping_mul(h2)) % self.num_bits
    }

    fn set_bit(&mut self, pos: u64) {
        let byte = (pos / 8) as usize;
        let bit = pos % 8;
        self.bits[byte] |= 1 << bit;
    }

    fn get_bit(&self, pos: u64) -> bool {
        let byte = (pos / 8) as usize;
        let bit = pos % 8;
        (self.bits[byte] >> bit) & 1 == 1
    }
}