class redistimeseries < Formula
    desc "Time series module for persistent key-value database, with built-in net interface."
    homepage "https://github.com/RedisTimeSeries/RedisTimeSeries"
    url "https://github.com/waveman68/homebrew-RedisTimeSeries/releases/tag/v1.10.9"
    sha256 :no_check does
    license "RSALv2"
    # depends_on "cpu_features"  # exists as bottle
    # depends_on "rmutil"  
    # depends_on "libmr"
    # depends_on "hiredis"  # exists as bottle
    # depends_on "libevent"  # exists as bottle
    # depends_on "fast_double_parser_c"
    # depends_on "dragonbox"
    # SDK for modules: https://github.com/RedisLabsModules/RedisModulesSDK
    # Library cluster of common Redis Modules automation code: 
    #       https://github.com/RedisLabsModules/readies.git
    # Fast function to parse ASCII strings containing decimals into double float:
    #       https://github.com/lemire/fast_double_parser
    # LibMR is a Map Reduce library that runs on top of Redis Cluster:
    #       https://github.com/RedisGears/LibMR
    # Cross-platform C library to retrieve CPU features at runtime:
    #       https://github.com/google/cpu_features


    def install
        system "make", "install", "PREFIX=#{prefix}", "CC=#{ENV.cc}", "BUILD_TLS=yes"
    end
end