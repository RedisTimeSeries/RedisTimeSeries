# Developing RedisTimeSeries

Developing RedisTimeSeries involves setting up the development environment (which can be either Linux-based or macOS-based), building RedisTimeSeries, running tests and benchmarks, and debugging both the RedisTimeSeries module and its tests.

## Cloning the git repository
By invoking the following command, RedisTimeSeries module and its submodules are cloned:
```sh
git clone --recursive https://github.com/RedisTimeSeries/RedisTimeSeries.git
```
## Working in an isolated environment
There are several reasons to develop in an isolated environment, like keeping your workstation clean, and developing for a different Linux distribution.
The most general option for an isolated environment is a virtual machine (it's very easy to set one up using [Vagrant](https://www.vagrantup.com)).
Docker is even a more agile solution, as it offers an almost instant solution:
```
ts=$(docker run -d -it -v $PWD:/build debian:bullseye bash)
docker exec -it $ts bash
```
Then, from within the container, `cd /build` and go on as usual.
In this mode, all installations remain in the scope of the Docker container.
Upon exiting the container, you can either re-invoke the container with the above `docker exec` or commit the state of the container to an image and re-invoke it on a later stage:

```
docker commit $ts ts1
docker stop $ts
ts=$(docker run -d -it -v $PWD:/build ts1 bash)
docker exec -it $ts bash
```

## Installing prerequisites
To build and test RedisTimeSeries one needs to install several packages, depending on the underlying OS. Currently, we support the Ubuntu/Debian, CentOS, Fedora, and macOS.

If you have `gnu make` installed, you can execute
```
cd RedisTimeSeries
make setup
```
Alternatively, just invoke the following:
```
cd RedisTimeSeries
git submodule update --init --recursive    
./deps/readies/bin/getpy3
./system-setup.py
```
Note that `system-setup.py` **will install various packages on your system** using the native package manager and pip. This requires root permissions (i.e. `sudo`) on Linux.

If you prefer to avoid that, you can:

* Review `system-setup.py` and install packages manually,
* Use an isolated environment like explained above,
* Utilize a Python virtual environment, as Python installations known to be sensitive when not used in isolation.

## Installing Redis
As a rule of thumb, you're better off running the latest Redis version.

If your OS has a Redis package, you can install it using the OS package manager.

Otherwise, you can invoke `./deps/readies/bin/getredis`.

## Getting help
`make help` provides a quick summary of the development features.

## Building from source
`make` will build RedisTimeSeries.

Build artifacts are placed into `bin/linux-x64-release` (or similar, according to your platform and build options).

Use `make clean` to remove built artifacts. `make clean ALL=1` will remove the entire binary artifacts directory.

## Running Redis with RedisTimeSeries
The following will run `redis` and load RedisTimeSeries module.
```
make run
```
You can open `redis-cli` in another terminal to interact with it.

## Running tests
The module includes a basic set of unit tests and integration tests:
* C unit tests, located in `src/tests`, run by `make unit_tests`.
* Python integration tests (enabled by RLTest), located in `tests/flow`, run by `make flow_tests`.

One can run all tests by invoking `make test`.
A single test can be run using the `TEST` parameter, e.g. `make flow_test TEST=file:name`.

## Debugging
To build for debugging (enabling symbolic information and disabling optimization), run `make DEBUG=1`.
One can the use `make run DEBUG=1` to invoke `gdb`.
In addition to the usual way to set breakpoints in `gdb`, it is possible to use the `BB` macro to set a breakpoint inside RedisTimeSeries code. It will only have an effect when running under `gdb`.

Similarly, Python tests in a single-test mode, one can set a breakpoint by using the `BB()` function inside a test. This will invoke `pudb`.

The two methods can be combined: one can set a breakpoint within a flow test, and when reached, connect `gdb` to a `redis-server` process to debug the module. 

