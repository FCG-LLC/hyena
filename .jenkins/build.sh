#!/bin/bash

set -ex

function status {
	echo "+----------------------------------------"
	echo "| $1"
	echo "+----------------------------------------"

}

DOCKER_OPTIONS="--build-arg destEnv=$destEnv"

if [[ $nocache == "true" ]]
then
	echo "Doing clean build"
	DOCKER_OPTIONS="${DOCKER_OPTIONS} --no-cache"

	# update builder image base
	status "Updating builder base image"
	docker pull portus.cs.int:5000/prod/rust-snmp-base
else
	echo "Using cache"
fi

# create builder
status "Creating builder image"
pushd source

docker build -t hyena-builder ${DOCKER_OPTIONS} .

popd

# generate debian changelog entry for this build
status "Updating debian changelog"
pushd source

if test "${branch#*tags/}" != "$branch"; then
	VERSION="${branch#tags/}"
else
	SHORT_COMMIT=$(git rev-parse --short $GIT_COMMIT)
	VERSION="`date -u +"%Y%m%d%H%M%S"`-$SHORT_COMMIT-$destEnv"
fi

.jenkins/update-changelog.sh "$VERSION" "$branch" "${GIT_PREVIOUS_SUCCESSFUL_COMMIT:-$GIT_COMMIT}" "$GIT_COMMIT"

PACKAGE_VERSION=$(grep -Po "\(\K([^\)]+)" debian/changelog | head -n 1)

popd


# perform the build
status "Performing the build"
mkdir -p artifacts
chmod a+rwx artifacts
find source \( -type d -exec chmod u+rwx,g+rwx,o+rwx {} \; -o -type f -exec chmod u+rw,g+rw,o+rw {} \; \)

docker run --rm \
	-e ENABLE_GITHUB=0 \
    -v $(realpath source):/home/app/project/hyena \
    -v $(realpath artifacts):/artifacts \
    hyena-builder hyena ${branch} deb

rc=$?

if [[ $rc != 0 ]]
then
	echo "Build failed!"
	exit $rc
fi

# push artifacts to the aptly server
status "Archiving artifacts"
pushd artifacts

artifacts=$(ls *.deb)
rc=$?

if [[ $rc != 0 ]]
then
	echo "No artifacts found, build failed!"
	exit $rc
fi

APTLY_SERVER=http://10.12.1.225:8080
for i in $artifacts; do
	curl -X POST -F file=@$i $APTLY_SERVER/api/files/${i%_amd64.*}
	curl -X POST $APTLY_SERVER/api/repos/main/file/${i%_amd64.*}
done

	ssh -tt -i ~/.ssh/aptly_rsa aptly@10.12.1.225
popd

echo version="$VERSION" > env.properties


# prepare runtime image
# tmp: copy artifacts
status "Preparing runtime image"

cp -rv artifacts/hyena*.deb source/dockerization/

cd $WORKSPACE/source
cd dockerization/

if [[ "${build_type:-Release}" == "Release" ]]
then
	HYENA_PACKAGE="hyena"
else
	HYENA_PACKAGE="hyena-dbg"
fi

docker build \
	--build-arg hyena_version="$PACKAGE_VERSION" \
	--build-arg hyena_package=${HYENA_PACKAGE} \
	${DOCKER_OPTIONS} \
	-t cs/$app .

status "Pushing runtime image"
docker tag cs/$app portus.cs.int:5000/$destEnv/cs-$app
docker push portus.cs.int:5000/$destEnv/cs-$app
