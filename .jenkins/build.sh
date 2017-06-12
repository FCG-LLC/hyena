#!/bin/bash


# update builder image base
docker pull portus.cs.int:5000/prod/rust-snmp-base

# create builder
pushd source

docker build -t hyena-builder .

popd

# generate debian changelog entry for this build
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
mkdir -p artifacts
chmod a+rwx artifacts
find source \( -type d -exec chmod u+rwx,g+rwx,o+rwx {} \; -o -type f -exec chmod u+rw,g+rw,o+rw {} \; \)

docker run --rm \
	-e ENABLE_GITHUB=0 \
    -v $(realpath source):/home/app/project/hyena \
    -v $(realpath artifacts):/artifacts \
    hyena-builder hyena ${branch} deb


# push artifacts to the aptly server
pushd artifacts
APTLY_SERVER=http://10.12.1.225:8080
for i in `ls *.deb`; do
	curl -X POST -F file=@$i $APTLY_SERVER/api/files/${i%_amd64.*}
	curl -X POST $APTLY_SERVER/api/repos/main/file/${i%_amd64.*}
	ssh -tt -i ~/.ssh/aptly_rsa yapee@10.12.1.225
done
popd

echo version="$VERSION" > env.properties


# prepare runtime image
# tmp: copy artifacts

cp -rv artifacts/hyena*.deb source/dockerization/

cd $WORKSPACE/source
cd dockerization/
docker build --build-arg destEnv=$destEnv --build-arg hyena_version="$PACKAGE_VERSION" --no-cache -t cs/$app .
docker tag cs/$app portus.cs.int:5000/$destEnv/cs-$app
docker push portus.cs.int:5000/$destEnv/cs-$app

