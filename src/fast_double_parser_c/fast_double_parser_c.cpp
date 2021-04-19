#include "fast_double_parser.h"

extern "C" const char *fast_double_parser_c_parse_number(const char *p, double *outDouble) {
    return fast_double_parser::parse_number(p, outDouble);
}
