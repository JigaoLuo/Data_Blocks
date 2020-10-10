// ---------------------------------------------------------------------------------------------------
// IMLAB
// ---------------------------------------------------------------------------------------------------
#ifndef INCLUDE_IMLAB_UTIL_OPERATOR_H
#define INCLUDE_IMLAB_UTIL_OPERATOR_H
// --------------------------------------------------------------------------------------------------
#include <memory>
#include <cstdint>
#include <cassert>
#include <utility>
// ---------------------------------------------------------------------------------------------------
namespace imlab {
namespace util {
// ---------------------------------------------------------------------------------------------------
/// Idea from stl_function.h
/// Motivation: I have no between functional operator from the <functional> library, so I mock one for my scan case: BETWEEN
/// Inorder to use all these functions to get type checked in the compile time, I mock all the function as tertiary_function

/**
 *  Idea from Implementation of unary_function and binary_function
 */
template<typename _Arg1, typename _Arg2, typename _Arg3, typename _Result>
struct ternary_function
{
    /// @c first_argument_type is the type of the first argument
    typedef _Arg1 	first_argument_type;

    /// @c second_argument_type is the type of the second argument
    typedef _Arg2 	second_argument_type;

    /// @c third_argument_type is the type of the second argument
    typedef _Arg3 	third_argument_type;

    /// @c result_type is the return type
    typedef _Result 	result_type;
};

template<typename _Tp>
struct equal_to : public ternary_function<_Tp, _Tp, _Tp, bool>
{
  _GLIBCXX14_CONSTEXPR
  bool
  operator()(const _Tp& __value,const _Tp& __l, const _Tp& __r) const
  { return  __value == __l; }
};

template<typename _Tp>
struct greater : public ternary_function<_Tp, _Tp, _Tp, bool>
{
  _GLIBCXX14_CONSTEXPR
  bool
  operator()(const _Tp& __value,const _Tp& __l, const _Tp& __r) const
  { return  __value > __l; }
};

template<typename _Tp>
struct less : public ternary_function<_Tp, _Tp, _Tp, bool>
{
  _GLIBCXX14_CONSTEXPR
  bool
  operator()(const _Tp& __value,const _Tp& __l, const _Tp& __r) const
  { return  __value < __l; }
};

template<typename _Tp>
struct between : public ternary_function<_Tp, _Tp, _Tp, bool>
{
    _GLIBCXX14_CONSTEXPR
    bool
    operator()(const _Tp& __value,const _Tp& __l, const _Tp& __r) const
    { return __l <= __value && __value <= __r; }
};
// ---------------------------------------------------------------------------------------------------
}  // namespace util
// ---------------------------------------------------------------------------------------------------
}  // namespace imlab
// ---------------------------------------------------------------------------------------------------
#endif  // INCLUDE_IMLAB_UTIL_OPERATOR_H
// ---------------------------------------------------------------------------------------------------
