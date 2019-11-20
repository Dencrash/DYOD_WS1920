#pragma once

#include <vector>

#include "types.hpp"
#include "base_attribute_vector.hpp"

namespace opossum {
/*
** FixedSizeAttributeVector is a template class that offers different sized implementations of
** the BaseAttributeVector. These can be used to store encoded elements in the dicitonary segment
*/
template <typename T>
class FixedSizeAttributeVector : public BaseAttributeVector {
 public:
  // Create a FixedSizeAttributeVector that is already resized depending on the number of elements
  explicit FixedSizeAttributeVector(const size_t number_of_elements) : _attribute_storage(number_of_elements) {};

  // return the ValueID at a given position
  ValueID get(const size_t i) const override {
    return static_cast<ValueID>(_attribute_storage.at(i));
  };

  // sets the value id at a given position
  void set(const size_t i, const ValueID value_id) override {
    _attribute_storage.at(i) = (T) value_id;
  };

  // returns the number of values
  size_t size() const override {
    return _attribute_storage.size();
  };

  // returns the width of a value ID in bytes
  AttributeVectorWidth width() const override {
    return sizeof(T);
  };

 protected:
  std::vector<T> _attribute_storage;
};

}  // namespace opossum

