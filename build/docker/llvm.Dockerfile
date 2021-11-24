FROM ubuntu:bionic


RUN apt-get update && apt-get -yy install wget curl gnupg software-properties-common
RUN bash -c "curl https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add"
RUN add-apt-repository "deb http://apt.llvm.org/bionic/   llvm-toolchain-bionic$LLVM_VERSION_STRING  main"
RUN apt-get update && apt-get install -y clang clang-format clang-tidy build-essential git

