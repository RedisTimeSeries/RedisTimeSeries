#!/bin/bash
#
# A sandbox with the necessary environment to work with the project.
#
# If the env variables are set or if the user provided those
# successfully, set the variables to the env variables in this script.
#
# Once the variables are set, the script will create a container of
# ubuntu:22.04 which we use in the CI/CD pipeline
# to build the project. The container will be named as
# "REDISTIMESERIES_SANDBOX". Within this container, the script will
# prepare the environment for the project by installing the necessary
# dependencies and fetching the necessary files.
#
# Once the container has everything prepared for the project, the script
# creates an image out of the container and removes the container. The
# image will be named as "redistimeseries-sandbox", which is also
# configurable within this script using the variables.
#
# When entering the sandbox, the script runs the container with the
# image "redistimeseries-sandbox" and mounts the necessary directories
# from the host machine to the container. The script also sets the user
# and group id to the user's id and group id to avoid permission issues.
# As a side-effect of setting the user and group id, the user will not
# be able to run the container as root. And the name of the invitation
# will be "IHaveNoName" or something like that, but this is not really
# an issue.
#
# Prerequisites:
# - coreutils.
# - Docker.
#
# Usage:
# - ./sandbox.sh - Enter the sandbox.

export SANDBOX_SOURCE_DOCKER_IMAGE=ubuntu:22.04
export SANDBOX_DOCKER_IMAGE=redistimeseries-sandbox
export SANDBOX_NAME=REDISTIMESERIES_SANDBOX
export PROJECT_DIR=/home/project
export PROJECT_SRC_DIR=${PROJECT_DIR}/src
export USER_ID=$(id -u)
export USER_GROUP=$(id -g)


ensure_submodules_are_set() {
	git submodule update --init --recursive
}

ensure_not_root() {
	if [ "$USER_ID" = 0 ]; then
		printf "\n\e[0;31mYou must not run this script as root but as a regular user. Exiting...\n\e[0m"
		exit 1
	fi
}

# Prepares the sandbox for the use, by installing the necessary
# dependencies and fetching the necessary files.
prepare_sandbox() {
	set -e

    docker build --no-cache -f Dockerfile.sandbox --rm -t ${SANDBOX_DOCKER_IMAGE} --build-arg project_dir=${PROJECT_DIR} .

    docker run -it \
	--name=$SANDBOX_NAME \
	-e HOME="${PROJECT_DIR}" \
	-w ${PROJECT_SRC_DIR} \
	-v "$PWD":${PROJECT_SRC_DIR} \
	-h $SANDBOX_NAME \
	$SANDBOX_DOCKER_IMAGE /bin/bash -c "chown $USER_ID:$USER_GROUP ${PROJECT_DIR} -R"

	docker commit $SANDBOX_NAME $SANDBOX_DOCKER_IMAGE
	docker rm $SANDBOX_NAME

	set +e
}

# Enters the sandbox.
enter_sandbox() {
	if [ ! "$(docker images $SANDBOX_DOCKER_IMAGE | grep $SANDBOX_DOCKER_IMAGE)" ]; then
		printf "\e[0;35mPreparing the sandbox...\e[0m\n\n"
		prepare_sandbox
		printf "\e[0;35mPrepared the sandbox...\e[0m\n\n"
	else
		printf "\e[0;35mSandbox is already there...\e[0m\n\n"
	fi

	if [ ! "$(docker ps -a -f name=$SANDBOX_NAME | grep $SANDBOX_NAME)" ]; then
		printf "\e[0;35mRunning a new sandbox container...\e[0m\n\n"
		printf "\n\n\e[0;35mWelcome to RedisTimeSeries Sandbox!\e[0m\n\e[1;37mThis sandbox uses the docker image: \e[1;33m${SANDBOX_DOCKER_IMAGE}\e[1;34m\nFeel like home! :>\e[0m\n\n"

		docker run --rm --name=$SANDBOX_NAME \
		-e HOME="${PROJECT_DIR}" \
		-w ${PROJECT_SRC_DIR} \
		-v "$PWD":${PROJECT_SRC_DIR} \
		--user "$USER_ID":"$USER_GROUP" \
		-it \
		$SANDBOX_DOCKER_IMAGE /bin/bash --rcfile ${PROJECT_SRC_DIR}/.venv/bin/activate
	fi
}

# Enters the sandbox and optionally pulls the image.
enter() {
	ensure_not_root
	ensure_submodules_are_set

	enter_sandbox
}

# trap leave EXIT

enter
