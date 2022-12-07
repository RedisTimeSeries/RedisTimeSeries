#include "utils/circular_array.h"
#include "minunit.h"

MU_TEST(test_circular_array) {
    carray_t *arr = carray_new(double, 3);
    carray_push_back(arr, 1.25);
    carray_push_back(arr, 2.25);
    double val = carray_pop_front(arr, double);
    mu_assert_double_eq(val, 1.25);
    val = carray_pop_front(arr, double);
    mu_assert_double_eq(val, 2.25);
    carray_push_back(arr, 3.25);
    carray_push_back(arr, 4.25);
    carray_push_back(arr, 5.25);
    val = carray_pop_front(arr, double);
    mu_assert_double_eq(val, 3.25);
    val = carray_pop_front(arr, double);
    mu_assert_double_eq(val, 4.25);
    carray_push_back(arr, 6.25);
    carray_push_back(arr, 7.25);
    val = carray_pop_front(arr, double);
    mu_assert_double_eq(val, 5.25);
}

MU_TEST_SUITE(circular_array_test_suite) {
    MU_RUN_TEST(test_circular_array);
}
