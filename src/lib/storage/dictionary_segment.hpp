#pragma once

#include <iostream>
#include <iterator>
#include <limits>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "base_attribute_vector.hpp"
#include "base_segment.hpp"
#include "fixed_size_attribute_vector.hpp"
#include "type_cast.hpp"
#include "types.hpp"

namespace opossum {

class BaseAttributeVector;
class BaseSegment;

// Even though ValueIDs do not have to use the full width of ValueID (uint32_t), this will also work for smaller ValueID
// types (uint8_t, uint16_t) since after a down-cast INVALID_VALUE_ID will look like their numeric_limit::max()
constexpr ValueID INVALID_VALUE_ID{std::numeric_limits<ValueID::base_type>::max()};

// Dictionary is a specific segment type that stores all its values in a vector
template <typename T>
class DictionarySegment : public BaseSegment {
 public:
  /**
   * Creates a Dictionary segment from a given value segment.
   */
  explicit DictionarySegment(const std::shared_ptr<BaseSegment>& base_segment)
      : _dictionary(std::make_shared<std::vector<T>>()) {
    size_t attribute_vector_size = 0;
    std::set<T> dictionary_helper;
    for (size_t segment_iterator = 0; segment_iterator < base_segment->size(); ++segment_iterator) {
      dictionary_helper.insert(type_cast<T>((*base_segment)[segment_iterator]));
      attribute_vector_size++;
    }
    _dictionary->reserve(dictionary_helper.size());
    for (auto it = dictionary_helper.begin(); it != dictionary_helper.end();) {
      _dictionary->emplace_back(std::move(dictionary_helper.extract(it++).value()));
    }

    _set_attribute_vector(_dictionary->size(), attribute_vector_size);

    for (size_t segment_iterator = 0; segment_iterator < base_segment->size(); ++segment_iterator) {
      T current_attribute_value = type_cast<T>((*base_segment)[segment_iterator]);
      auto dict_memory_address = std::upper_bound(_dictionary->begin(), _dictionary->end(), current_attribute_value);
      // The index before the upper bound includes the correct element
      auto dict_index = static_cast<uint32_t>(std::distance(_dictionary->begin(), dict_memory_address) - 1);
      _attribute_vector->set(segment_iterator, static_cast<ValueID>(dict_index));
    }
  }

  // SEMINAR INFORMATION: Since most of these methods depend on the template parameter, you will have to implement
  // the DictionarySegment in this file. Replace the method signatures with actual implementations.

  // return the value at a certain position. If you want to write efficient operators, back off!
  AllTypeVariant operator[](const ChunkOffset chunk_offset) const override {
    auto helper = _attribute_vector->get(chunk_offset);
    return (*_dictionary)[helper];
  }

  // return the value at a certain position.
  T get(const size_t chunk_offset) const {
    auto helper = _attribute_vector->get(chunk_offset);
    return _dictionary->at(helper);
  }

  // dictionary segments are immutable
  void append(const AllTypeVariant&) override {
    throw std::runtime_error("Dictionary Segments are immutable. They should not be changed");
  }

  // returns an underlying dictionary
  std::shared_ptr<const std::vector<T>> dictionary() const { return _dictionary; }

  // returns an underlying data structure
  std::shared_ptr<BaseAttributeVector> attribute_vector() const { return _attribute_vector; }

  // return the value represented by a given ValueID
  const T& value_by_value_id(ValueID value_id) const { return _dictionary->at(value_id); }

  // returns the first value ID that refers to a value >= the search value
  // returns INVALID_VALUE_ID if all values are smaller than the search value
  ValueID lower_bound(T value) const {
    auto result_memory_address = std::lower_bound(_dictionary->begin(), _dictionary->end(), value);
    auto result = static_cast<ValueID>(std::distance(_dictionary->begin(), result_memory_address));
    if (result == _dictionary->size()) {
      return INVALID_VALUE_ID;
    }
    return result;
  }

  // same as lower_bound(T), but accepts an AllTypeVariant
  ValueID lower_bound(const AllTypeVariant& value) const { return lower_bound(static_cast<T>(value)); }

  // returns the first value ID that refers to a value > the search value
  // returns INVALID_VALUE_ID if all values are smaller than or equal to the search value
  ValueID upper_bound(T value) const {
    auto result_memory_address = std::upper_bound(_dictionary->begin(), _dictionary->end(), value);
    auto result = static_cast<ValueID>(std::distance(_dictionary->begin(), result_memory_address));
    if (result == _dictionary->size()) {
      return INVALID_VALUE_ID;
    }
    return result;
  }

  // same as upper_bound(T), but accepts an AllTypeVariant
  ValueID upper_bound(const AllTypeVariant& value) const { return upper_bound(static_cast<T>(value)); }

  // return the number of unique_values (dictionary entries)
  size_t unique_values_count() const { return _dictionary->size(); }

  // return the number of entries
  size_t size() const override { return _attribute_vector->size(); }

  // returns the calculated memory usage of the segment
  size_t estimate_memory_usage() const {
    auto dic_size = _dictionary->size() * sizeof(T);
    auto att_size = _attribute_vector->size() * _attribute_vector->width();

    return dic_size + att_size;
  }

 protected:
  std::shared_ptr<std::vector<T>> _dictionary;
  std::shared_ptr<BaseAttributeVector> _attribute_vector;

  // set the attribute vector with the current width and length depending on the incoming data
  void _set_attribute_vector(const size_t dictionary_size, const size_t attribute_vector_size) {
    if (dictionary_size <= std::numeric_limits<uint8_t>::max()) {
      _attribute_vector = std::make_shared<FixedSizeAttributeVector<uint8_t>>(attribute_vector_size);
    } else if (dictionary_size <= std::numeric_limits<uint16_t>::max()) {
      _attribute_vector = std::make_shared<FixedSizeAttributeVector<uint16_t>>(attribute_vector_size);
    } else {
      DebugAssert(dictionary_size <= std::numeric_limits<uint32_t>::max(),
                  "A DictionarySegment cannot store more than 4294967295 Values");
      _attribute_vector = std::make_shared<FixedSizeAttributeVector<uint32_t>>(attribute_vector_size);
    }
  }
};

}  // namespace opossum
