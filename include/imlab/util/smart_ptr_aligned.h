// ---------------------------------------------------------------------------------------------------
// IMLAB
// ---------------------------------------------------------------------------------------------------
#ifndef INCLUDE_IMLAB_UTIL_SMART_PTR_ALIGNED
#define INCLUDE_IMLAB_UTIL_SMART_PTR_ALIGNED
// --------------------------------------------------------------------------------------------------
#include <memory>
#include <cstdint>
#include <cassert>
#include <utility>
#include "unique_align.h"
// ---------------------------------------------------------------------------------------------------
namespace imlab {
namespace util {
// ---------------------------------------------------------------------------------------------------

static void aligned_free_wrapper(void * ptr)
{
  printf("Calling aligned_free on ptr: %p\n", ptr);
  aligned_free(ptr);
}

/**
* Here we create aliases for our aligned pointer types.
* We are specifying that our alias has a fixed value: the deleter function type
* We use this for unique_ptr as it requires the type of the deleter in the declaration
*/
template<class T> using unique_ptr_aligned = std::unique_ptr<T, decltype(&aligned_free)>;

/**
* We can create a template function that simplifies our declarations of aligned
* unique pointers. Alignment and size are passed through to aligned malloc, and
* aligned free is always used as the deleter.  We then generate the correct pointer
* type based on the templated call
*/
template<class T>
unique_ptr_aligned<T> aligned_uptr(size_t align, size_t size)
{
  return unique_ptr_aligned<T>(static_cast<T*>(aligned_malloc(align, size)), &aligned_free_wrapper);
}

/**
* We can create a template function that simplifies our declarations of aligned
* shared pointers. Alignment and size are passed through to aligned malloc, and
* aligned free is always used as the deleter.  We then generate the correct pointer
* type based on the templated call
*
* Notice here that the shared pointer doesn't need a special type due to the deleter
* The deleter type is only required for the unique pointer.
*/
template<class T>
std::shared_ptr<T> aligned_sptr(size_t align, size_t size)
{
  return std::shared_ptr<T>(static_cast<T*>(aligned_malloc(align, size)), &aligned_free_wrapper);
}
// ---------------------------------------------------------------------------------------------------
}  // namespace util
// ---------------------------------------------------------------------------------------------------
}  // namespace imlab
// ---------------------------------------------------------------------------------------------------
#endif  // INCLUDE_IMLAB_UTIL_SMART_PTR_ALIGNED
// ---------------------------------------------------------------------------------------------------
