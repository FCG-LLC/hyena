FROM portus.cs.int:5000/prod/cs-base
ARG destEnv
ARG hyena_version
ARG hyena_package
ARG git_commit
ARG git_source
ARG jenkins_jobid

LABEL maintainer="Jacek Całusiński <forger@collective-sense.com>" \
	collective.git.commit="${git_commit}" \
	collective.git.source="${git_source}" \
	collective.jenkins.jobid="${jenkins_jobid}" \
	collective.jenkins.build.package.name="${hyena_package}" \
	collective.jenkins.build.package.version="${hyena_version}"

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get install apt-transport-https -y --force-yes

RUN groupadd -g 10024 hyena
RUN adduser --no-create-home --system --shell /bin/false --gecos 'hyena dedicated user' --uid 10024 --gid 10024 --disabled-password hyena

RUN echo "deb http://aptly.cs.int/public xenial $destEnv" >> /etc/apt/sources.list
RUN printf "Package: * \nPin: release a=xenial, o=aptly.cs.int \nPin-Priority: 1600 \n" > /etc/apt/preferences

RUN apt-get update
RUN apt-get install -y nanomsg
RUN apt-get install -y ${hyena_package:-"hyena"}=${hyena_version:-"*"}

VOLUME "/tmp/hyena"

RUN chown hyena:hyena /tmp/hyena

USER hyena

CMD /usr/bin/hyena
