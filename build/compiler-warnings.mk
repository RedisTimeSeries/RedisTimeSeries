# Include after readies' mk/defs so CC is the compiler actually used by the build.
# Apple Clang also defines __GNUC__, so check __clang__ first.
RTS_CC_DEFINES := $(shell $(CC) -dM -E -x c /dev/null 2>/dev/null)

ifneq (,$(findstring __clang__,$(RTS_CC_DEFINES)))
CC_C_FLAGS.warnings += -Werror=uninitialized -Werror=sometimes-uninitialized
else ifneq (,$(findstring __GNUC__,$(RTS_CC_DEFINES)))
CC_C_FLAGS.warnings += -Werror=uninitialized -Werror=maybe-uninitialized
endif

# The analyzer is deliberately opt-in: it costs substantially more than normal
# compiler warnings. CI runs it in a separate GCC build.
ifeq ($(GCC_ANALYZER),1)
ifneq (,$(findstring __clang__,$(RTS_CC_DEFINES)))
$(error GCC_ANALYZER=1 requires GCC, not Clang)
endif
ifeq (,$(findstring __GNUC__,$(RTS_CC_DEFINES)))
$(error GCC_ANALYZER=1 requires GCC)
endif
CC_C_FLAGS.warnings += -fanalyzer -Werror=analyzer-use-of-uninitialized-value
endif
