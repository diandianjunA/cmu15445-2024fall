#include "primer/hyperloglog.h"

namespace bustub {

template <typename KeyType>
HyperLogLog<KeyType>::HyperLogLog(int16_t n_bits) : cardinality_(0), registers((1 << (n_bits > 0 ? n_bits : 0))), register_num((n_bits > 0 ? n_bits : 0)) {}

template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeBinary(const hash_t &hash) const -> std::bitset<BITSET_CAPACITY> {
  /** @TODO(student) Implement this function! */
  std::bitset<BITSET_CAPACITY> ans(hash);
  return ans;
}

template <typename KeyType>
auto HyperLogLog<KeyType>::PositionOfLeftmostOne(const std::bitset<BITSET_CAPACITY> &bset) const -> uint64_t {
  /** @TODO(student) Implement this function! */
  for (int i = BITSET_CAPACITY - register_num - 1; i >= 0; i--) {
    if (bset.test(i)) {
      return (BITSET_CAPACITY - i - 1) - register_num + 1;
    }
  }
  return BITSET_CAPACITY;
}

template <typename KeyType>
auto HyperLogLog<KeyType>::AddElem(KeyType val) -> void {
  /** @TODO(student) Implement this function! */
  hash_t h = CalculateHash(val);
  std::bitset<BITSET_CAPACITY> bset = ComputeBinary(h);
  int r = 0;
  for (int i = BITSET_CAPACITY - 1; i > BITSET_CAPACITY - register_num - 1; i--) {
    r += (1 << (register_num - (BITSET_CAPACITY - i - 1) - 1)) * (bset.test(i) ? 1 : 0);
  }
  int v = PositionOfLeftmostOne(bset);
  registers[r] = registers[r] > v ? registers[r] : v;
}

template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeCardinality() -> void {
  /** @TODO(student) Implement this function! */
  double avg = 0;
  uint64_t m = (1 << register_num);
  for (uint64_t i = 0; i < m; i++) {
    avg += pow(2, -registers[i]);
  }
  avg = m / avg;
  cardinality_ = std::floor(CONSTANT * m * avg);
}

template class HyperLogLog<int64_t>;
template class HyperLogLog<std::string>;

}  // namespace bustub
