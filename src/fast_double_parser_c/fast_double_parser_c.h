/* fast_double_parser_c.h -- C Wrapper for the C++ Eisel-Lemire ParseFloat algorithm.
 *
 * This file wraps the original C++ implementation implementation
 * of the Eisel-Lemire ParseFloat algorithm, published in
 * 2020 and discussed extensively at
 * https://nigeltao.github.io/blog/2020/eisel-lemire.html
 *
 * The original C++ implementation is at
 * https://github.com/lemire/fast_double_parser
 *
 * ----------------------------------------------------------------------------
 *
 * Copyright (c) 2020, Daniel Lemire <lemire at gmail dot com>
 * All rights reserved.
 *
 * Copyright [yyyy] [name of copyright owner]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef __FAST_DOUBLE_PARSER_H_C_H__
#define __FAST_DOUBLE_PARSER_H_C_H__

#if defined(__cplusplus)
extern "C"
{
#endif

    /**
     *  Fast function to parse strings containing decimal numbers into double-precision (binary64)
     * floating-point values.
     *
     *  We expect string numbers to follow [RFC 7159](https://tools.ietf.org/html/rfc7159) (JSON
     * standard). In particular, the parser will reject overly large values that would not fit in
     * binary64. It will not accept NaN or infinite values.
     * @param p pointer to the position of the string in which
     *       the number starts. if you must skip whitespace characters, it is your responsibility to
     * do so.
     * @param outDouble pointer to the double to store the parsed number
     * @return NULL in case the function refused to parse the input.
     *         Otherwise, we return a pointer (`const char *`) to the end of the parsed string.
     */
    const char *fast_double_parser_c_parse_number(const char *p, double *outDouble);

#if defined(__cplusplus)
}
#endif

#endif /* __FAST_DOUBLE_PARSER_H_C_H__ */
