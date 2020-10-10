// ---------------------------------------------------------------------------------------------------
// IMLAB
// ---------------------------------------------------------------------------------------------------
#ifndef INCLUDE_IMLAB_BUFFER_MANAGE_SEGMENT_H_
#define INCLUDE_IMLAB_BUFFER_MANAGE_SEGMENT_H_
// -------------------------------------------------------------------------------------
#include <array>
#include <atomic>
#include "imlab/buffer_manage/buffer_manager.h"
// -------------------------------------------------------------------------------------
namespace imlab {
class Segment {
 public:
  /// Constructor.
  /// @param[in] segment_id       Id of the segment.
  /// @param[in] buffer_manager   The buffer manager that should be used by the segment.
  Segment(uint16_t segment_id, imlab::BufferManager& buffer_manager)
      : segment_id(segment_id), buffer_manager(buffer_manager) {}

  /// The segment id
  uint16_t segment_id;

  /// The buffer manager
  imlab::BufferManager& buffer_manager;
};
}  // namespace imlab
// -------------------------------------------------------------------------------------
#endif  // INCLUDE_IMLAB_BUFFER_MANAGE_SEGMENT_H_
